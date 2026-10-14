use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::{Arc, Barrier};
use worldinterface_domain::{ingress::*, ports::QueueWakePort};
use worldinterface_ingress::{
    transport::{
        http::validate_http,
        streaming::{SessionHandshake, StreamSession},
        ProviderCredentials, ValidatedProviderEvent,
    },
    *,
};
use worldinterface_protocol::*;
use worldinterface_storage::{StoreKind, StoreProfile, StoreRoot};
fn r(s: &str) -> OpaqueRef {
    s.to_owned().try_into().unwrap()
}
fn fixture() -> IngressBinding {
    serde_json::from_str(include_str!("../../../../conformance/wi-fabric-2/ingress/binding.json"))
        .unwrap()
}
struct Authority(IngressBinding);
impl BindingAuthority for Authority {
    fn resolve_and_authorize(
        &self,
        scope: &AuthenticatedScope,
        submission: &IngressBindingSubmission,
        _: u64,
    ) -> worldinterface_ingress::Result<BindingGrant> {
        assert_eq!(scope.principal, self.0.principal);
        assert_eq!(submission.source, self.0.submission.source);
        Ok(BindingGrant {
            subscriber: self.0.subscriber.clone(),
            profile: self.0.profile.clone(),
            policy: self.0.policy.clone(),
            resolver_version: self.0.resolver_version.clone(),
            authorization: self.0.authorization.clone(),
            authentication_requirement: self.0.authentication_requirement.clone(),
            expires_at_ms: self.0.expires_at_ms,
        })
    }
}
struct Setup {
    temp: tempfile::TempDir,
    root: StoreRoot,
    b: IngressBinding,
    version: BindingVersion,
}
impl Setup {
    fn new() -> Self {
        Self::with(fixture())
    }
    fn with(b: IngressBinding) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = StoreRoot::create_empty(
            temp.path().join("root"),
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-20T00:00:00.000000000Z".to_string().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        root.publish(b.profile.scope.tenant.clone(), b"authorization").unwrap();
        let owner = AuthenticatedScope {
            tenant: b.profile.scope.tenant.clone(),
            principal: b.principal.clone(),
        };
        let version = Ingress::new(&root)
            .submit_binding(&owner, b.submission.clone(), b.id.clone(), &Authority(b.clone()), 1000)
            .unwrap();
        Self { temp, root, b, version }
    }
    fn owner(&self) -> AuthenticatedScope {
        AuthenticatedScope {
            tenant: self.b.profile.scope.tenant.clone(),
            principal: self.b.principal.clone(),
        }
    }
    fn subscriber(&self) -> AuthenticatedScope {
        AuthenticatedScope {
            tenant: self.b.profile.scope.tenant.clone(),
            principal: self.b.subscriber.clone(),
        }
    }
    fn event(&self, id: &str, offset: u64) -> ProviderEvent {
        ProviderEvent {
            scope: self.b.profile.scope.clone(),
            event_id: Some(r(id)),
            offset: Some(offset),
            produced_at_ms: 1000,
            asserted_sender: r("asserted-not-authenticated"),
            asserted_audience: vec![r("claimed-audience")],
            content: self
                .root
                .publish(provider_artifact_scope(&self.b.profile.scope).unwrap(), b"message")
                .unwrap(),
            attachments: vec![],
            information_use: InformationUseEnvelope {
                purpose_refs: vec![r("observe")],
                allowed_audiences: vec![self.b.subscriber.clone()],
                retention_policy_ref: Some(r("retain-logically")),
                disclosure_policy_ref: Some(r("governed")),
                derivation_policy_ref: None,
                redistribution: RedistributionPolicy::Prohibited,
                expiration: None,
                source_obligation_refs: vec![],
            },
            profile: ObservationProfile::Ordinary,
        }
    }
    fn http(&self, e: &ProviderEvent) -> ValidatedProviderEvent {
        http(e, &self.b.profile)
    }
    fn admit(&self, e: &ProviderEvent, now: u64) -> IngressResult {
        Ingress::new(&self.root)
            .admit_provider_event(self.http(e), vec![self.version.clone()], ids(1), now)
            .unwrap()
    }
    fn state(&self) -> IngressState {
        self.root.ingress_snapshot(&self.b.profile.scope.tenant).unwrap()
    }
}
fn sign(bytes: &[u8]) -> Vec<u8> {
    let mut h = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    h.update(bytes);
    h.finalize().into_bytes().to_vec()
}

#[test]
fn queue_recovery_exhaustion_uses_pinned_policy_and_preserves_lineage() {
    for budget in [1, 2, 100] {
        let mut binding = fixture();
        binding.policy.max_attempts = budget;
        let s = Setup::with(binding);
        let id = delivery(s.admit(&s.event("recovery-budget", 0), 1000));
        let tenant = s.b.profile.scope.tenant.clone();
        let mut state = s.state();
        let request = |generation| {
            let task_request = format!("task-{generation}").into_bytes();
            let signal_request = format!("signal-{generation}").into_bytes();
            QueueObligation {
                version: 1,
                delivery: id.clone(),
                task_digest: ContentHash::of(&task_request),
                signal_digest: ContentHash::of(&signal_request),
                task_request,
                signal_request,
                task_admitted: false,
                signal_sequence: None,
                retired_wake_sequence: None,
                generation,
                prior: vec![],
            }
        };
        let mut initial = request(0);
        initial.task_admitted = true;
        initial.signal_sequence = Some(1);
        state.queue.insert(id.clone(), initial);
        // Current binding configuration cannot extend a captured delivery budget.
        state.bindings.get_mut(&s.b.id).unwrap().policy.max_attempts = 100;
        for generation in 0..=budget {
            let command = IngressCommand::RecoverQueue {
                id: id.clone(),
                expected_generation: generation,
                replacement: Box::new(request(generation + 1)),
            };
            let (next, result) = apply_ingress(&state, &tenant, 1000, &command).unwrap();
            if generation == budget {
                assert_eq!(result, IngressResult::QueueRecoveryExhausted);
                assert_eq!(next.deliveries[&id].state, DeliveryState::DeadLetter);
                assert_eq!(next.queue, state.queue);
                assert!(next.deliveries[&id].attempts.is_empty());
                assert!(apply_ingress(&next, &tenant, 1000, &command).is_err());
            } else {
                assert_eq!(result, IngressResult::Updated);
                assert_eq!(next.queue[&id].prior.len(), generation as usize + 1);
                state = next;
                let q = &state.queue[&id];
                state = apply_ingress(
                    &state,
                    &tenant,
                    1000,
                    &IngressCommand::RecordQueueTask {
                        id: id.clone(),
                        generation: generation + 1,
                        task_digest: q.task_digest.clone(),
                    },
                )
                .unwrap()
                .0;
                let q = &state.queue[&id];
                state = apply_ingress(
                    &state,
                    &tenant,
                    1000,
                    &IngressCommand::RecordQueueSignal {
                        id: id.clone(),
                        generation: generation + 1,
                        signal_digest: q.signal_digest.clone(),
                        sequence: u64::from(generation) + 2,
                    },
                )
                .unwrap()
                .0;
            }
        }
    }
}

#[test]
fn retired_queue_wake_fences_identity_and_preserves_closure() {
    let s = Setup::new();
    let id = delivery(s.admit(&s.event("retired-wake", 0), 1000));
    let tenant = s.b.profile.scope.tenant.clone();
    let mut state = s.state();
    let queue = QueueObligation {
        version: 1,
        delivery: id.clone(),
        task_request: b"task".to_vec(),
        signal_request: b"signal".to_vec(),
        task_digest: ContentHash::of(b"task"),
        signal_digest: ContentHash::of(b"signal"),
        task_admitted: true,
        signal_sequence: Some(7),
        retired_wake_sequence: None,
        generation: 1,
        prior: vec![],
    };
    state.queue.insert(id.clone(), queue.clone());
    let retirement = |generation, signal_digest, sequence| IngressCommand::RecordQueueWakeRetired {
        id: id.clone(),
        generation,
        signal_digest,
        sequence,
    };
    let command = retirement(1, queue.signal_digest.clone(), 7);
    let (unchanged, result) =
        apply_ingress(&state, &tenant, 1000, &retirement(0, queue.signal_digest.clone(), 7))
            .unwrap();
    assert_eq!(result, IngressResult::StaleQueueGeneration);
    assert_eq!(unchanged, state);
    for bad in [
        retirement(1, ContentHash::of(b"wrong"), 7),
        retirement(1, queue.signal_digest.clone(), 0),
        retirement(1, queue.signal_digest.clone(), 8),
    ] {
        assert!(apply_ingress(&state, &tenant, 1000, &bad).is_err());
    }
    let (closed, _) = apply_ingress(&state, &tenant, 1000, &command).unwrap();
    assert_eq!(closed.deliveries[&id].state, DeliveryState::DeadLetter);
    assert_eq!(closed.queue[&id].retired_wake_sequence, Some(7));
    assert_eq!(apply_ingress(&closed, &tenant, 1000, &command).unwrap().0, closed);
    let expires = state.deliveries[&id].expires_at_ms;
    let (expired, _) = apply_ingress(&state, &tenant, expires, &command).unwrap();
    assert_eq!(expired.deliveries[&id].state, DeliveryState::Expired);
    assert!(expired.deliveries[&id].token_invalidated);
    assert!(expired.queue[&id].retired_wake_sequence.is_none());
    let token_digest = ContentHash::of(b"issued-token");
    let (claimed, _) = apply_ingress(
        &state,
        &tenant,
        1000,
        &IngressCommand::Claim {
            id: id.clone(),
            subscriber: s.b.subscriber.clone(),
            token_digest: token_digest.clone(),
        },
    )
    .unwrap();
    let (acknowledged, _) = apply_ingress(
        &claimed,
        &tenant,
        1000,
        &IngressCommand::Acknowledge {
            id: id.clone(),
            subscriber: s.b.subscriber.clone(),
            token_digest,
        },
    )
    .unwrap();
    assert_eq!(apply_ingress(&acknowledged, &tenant, 1000, &command).unwrap().0, acknowledged);
    // Older journal bytes omit the optional cause and round-trip unchanged.
    let bytes = serde_json::to_vec(&queue).unwrap();
    assert!(!std::str::from_utf8(&bytes).unwrap().contains("retired_wake_sequence"));
    assert_eq!(serde_json::from_slice::<QueueObligation>(&bytes).unwrap(), queue);
}
fn credentials(p: &ProviderProfile) -> ProviderCredentials {
    ProviderCredentials::new(p.clone(), vec![7; 32]).unwrap()
}
fn http(e: &ProviderEvent, p: &ProviderProfile) -> ValidatedProviderEvent {
    let body = canonical_data(e).unwrap();
    validate_http(&body, &sign(&body), &credentials(p), 1000).unwrap()
}
fn stream(e: &ProviderEvent, p: &ProviderProfile, reconnect: u64) -> ValidatedProviderEvent {
    try_stream(e, p, reconnect).unwrap()
}
fn try_stream(
    e: &ProviderEvent,
    p: &ProviderProfile,
    reconnect: u64,
) -> worldinterface_ingress::Result<ValidatedProviderEvent> {
    let h = SessionHandshake {
        session: r("session-1"),
        scope: p.scope.clone(),
        reconnect_from: reconnect,
        expires_at_ms: 10000,
    };
    let bytes = canonical_data(&h).unwrap();
    let session = StreamSession::authenticate(&bytes, &sign(&bytes), credentials(p), 1000).unwrap();
    let bytes = canonical_data(e).unwrap();
    let signed = canonical_data(&(h.session, bytes.as_slice())).unwrap();
    session.validate_stream(&bytes, &sign(&signed), 1000)
}
fn ids(n: usize) -> AdmissionIds {
    AdmissionIds {
        observation: ObservationId::generate(),
        receipt: ReceiptId::generate(),
        deliveries: (0..n).map(|_| DeliveryId::generate()).collect(),
    }
}
fn delivery(result: IngressResult) -> DeliveryId {
    match result {
        IngressResult::Admitted { deliveries, .. } => deliveries[0].clone(),
        _ => panic!("not admitted"),
    }
}

#[test]
fn binding_admission_is_scoped_idempotent_and_version_checked() {
    let s = Setup::new();
    let i = Ingress::new(&s.root);
    assert_eq!(
        i.submit_binding(
            &s.owner(),
            s.b.submission.clone(),
            IngressBindingId::generate(),
            &Authority(s.b.clone()),
            1000
        )
        .unwrap(),
        s.version
    );
    let mut bad = s.b.submission.clone();
    bad.subscriber = r("substituted");
    assert!(i
        .submit_binding(
            &s.owner(),
            bad,
            IngressBindingId::generate(),
            &Authority(s.b.clone()),
            1000
        )
        .is_err());
    let event = s.event("event", 0);
    let valid = s.http(&event);
    i.change_binding(&s.owner(), &s.version, BindingStatus::Suspended, 1000).unwrap();
    assert!(i.admit_provider_event(valid, vec![s.version.clone()], ids(1), 1000).is_err());
    assert!(s.state().admissions.is_empty());
    assert!(i.change_binding(&s.owner(), &s.version, BindingStatus::Active, 1000).is_err());
    let v = BindingVersion { id: s.version.id.clone(), revision: 2 };
    i.change_binding(&s.owner(), &v, BindingStatus::Revoked, 1000).unwrap();
    assert!(i
        .change_binding(
            &s.owner(),
            &BindingVersion { id: v.id, revision: 3 },
            BindingStatus::Active,
            1000
        )
        .is_err());
}
#[test]
fn concurrent_http_and_stream_duplicates_capture_one_atomic_fanout() {
    let s = Setup::new();
    let e = s.event("event", 0);
    let mut b = s.b.clone();
    b.id = IngressBindingId::generate();
    b.submission.client_request_id = "second".to_string().try_into().unwrap();
    let second = Ingress::new(&s.root)
        .submit_binding(&s.owner(), b.submission.clone(), b.id.clone(), &Authority(b), 1000)
        .unwrap();
    let mut versions = vec![s.version.clone(), second];
    versions.sort_by(|a, b| a.id.cmp(&b.id));
    let barrier = Arc::new(Barrier::new(8));
    let results = std::thread::scope(|scope| {
        let mut jobs = vec![];
        for n in 0..8 {
            let barrier = barrier.clone();
            let e = &e;
            let s = &s;
            let versions = versions.clone();
            jobs.push(scope.spawn(move || {
                let event = if n % 2 == 0 { s.http(e) } else { stream(e, &s.b.profile, 0) };
                barrier.wait();
                Ingress::new(&s.root).admit_provider_event(event, versions, ids(2), 1000).unwrap()
            }));
        }
        jobs.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    let state = s.state();
    assert_eq!(state.admissions.len(), 1);
    assert_eq!(state.deliveries.len(), 2);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, IngressResult::Admitted { duplicate: false, .. }))
            .count(),
        1
    );
    let first = state.admissions.values().next().unwrap();
    for result in results {
        match result {
            IngressResult::Admitted { observation, deliveries, .. } => {
                assert_eq!(observation, first.observation.id);
                assert_eq!(deliveries, first.deliveries)
            }
            _ => panic!(),
        }
    }
    // A later subscription is not enrolled by retransmission.
    let mut b = s.b.clone();
    b.id = IngressBindingId::generate();
    b.submission.client_request_id = "third".to_string().try_into().unwrap();
    Ingress::new(&s.root)
        .submit_binding(&s.owner(), b.submission.clone(), b.id.clone(), &Authority(b), 1000)
        .unwrap();
    assert!(matches!(s.admit(&e, 1000), IngressResult::Admitted { duplicate: true, .. }));
    assert_eq!(s.state().deliveries.len(), 2);
    let mut conflict = e.clone();
    conflict.asserted_sender = r("changed");
    assert_eq!(s.admit(&conflict, 1000), IngressResult::Conflict);
    assert_eq!(s.state().conflicts.len(), 1);
    assert_eq!(s.state().admissions, state.admissions);
}
#[test]
fn transport_rejects_forgery_substitution_bounds_and_malformed_frames() {
    let s = Setup::new();
    let e = s.event("event", 0);
    let bytes = canonical_data(&e).unwrap();
    let creds = credentials(&s.b.profile);
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(validate_http(&changed, &sign(&bytes), &creds, 1000).is_err());
    for n in [0, 31, 33, 10000] {
        assert!(validate_http(&bytes, &vec![0; n], &creds, 1000).is_err());
    }
    for bytes in [b"{".to_vec(), vec![b' '; 16385], b"{\"scope\":1,\"scope\":2}".to_vec()] {
        assert!(validate_http(&bytes, &sign(&bytes), &creds, 1000).is_err());
    }
    for field in 0..6 {
        let mut event = e.clone();
        match field {
            0 => event.scope.tenant = r("wrong"),
            1 => event.scope.account = r("wrong"),
            2 => event.scope.source.resource = r("wrong"),
            3 => event.scope.namespace = r("wrong"),
            4 => event.produced_at_ms = 1001,
            _ => event.content.scope = r("wrong"),
        };
        let bytes = canonical_data(&event).unwrap();
        assert!(validate_http(&bytes, &sign(&bytes), &creds, 1000).is_err());
    }
    assert!(validate_http(&bytes, &sign(&bytes), &creds, 101001).is_err());
    let handshake = SessionHandshake {
        session: r("session"),
        scope: s.b.profile.scope.clone(),
        reconnect_from: 0,
        expires_at_ms: 2000,
    };
    let h = canonical_data(&handshake).unwrap();
    assert!(StreamSession::authenticate(&h, &[0; 32], credentials(&s.b.profile), 1000).is_err());
    let session =
        StreamSession::authenticate(&h, &sign(&h), credentials(&s.b.profile), 1000).unwrap();
    assert!(session.validate_stream(&bytes, &sign(&bytes), 1000).is_err());
    let signed = canonical_data(&(r("session"), bytes.as_slice())).unwrap();
    assert!(session.validate_stream(&bytes, &sign(&signed), 2000).is_err());
    let mut missing = e;
    missing.offset = None;
    let body = canonical_data(&missing).unwrap();
    let signed = canonical_data(&(r("session"), body.as_slice())).unwrap();
    assert!(session.validate_stream(&body, &sign(&signed), 1000).is_err());
    assert!(s.state().admissions.is_empty());
}
struct MockWake {
    seen: Vec<OpaqueRef>,
}
impl QueueWakePort for MockWake {
    fn wake(&mut self, key: &OpaqueRef, locator: &OpaqueRef) -> std::result::Result<(), String> {
        assert!(locator.as_str().starts_with("delivery:"));
        self.seen.push(key.clone());
        Ok(())
    }
}
#[test]
fn restart_commit_before_wake_and_subscriber_commit_before_ack_are_safe() {
    let s = Setup::new();
    let e = s.event("event", 0);
    let id = delivery(s.admit(&e, 1000));
    let subscriber = s.subscriber();
    let tenant = s.b.profile.scope.tenant.clone();
    let path = s.temp.path().join("root");
    drop(s.root);
    let root = StoreRoot::open_existing(&path).unwrap();
    let i = Ingress::new(&root);
    assert_eq!(i.due_deliveries(&subscriber, 1000, 10).unwrap(), vec![id.clone()]);
    let first =
        i.claim_due_delivery(&subscriber, &id, DeliveryToken::generate(), 1000).unwrap().unwrap();
    let mut wake = MockWake { seen: vec![] };
    wake.wake(&first.wake_key, &first.locator).unwrap();
    i.record_wake(&tenant, id.clone(), 1, true, 1000).unwrap();
    assert_eq!(i.inspect_delivery(&subscriber, &id, 1000).unwrap().state, DeliveryState::Pending);
    let database = s.temp.path().join("subscriber.sqlite");
    let app = rusqlite::Connection::open(&database).unwrap();
    app.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE ingested(id TEXT PRIMARY KEY, content TEXT NOT NULL)").unwrap();
    app.execute(
        "INSERT INTO ingested VALUES(?,?)",
        rusqlite::params![id.as_str(), first.view.content.content.as_str()],
    )
    .unwrap();
    drop(app);
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    let i = Ingress::new(&root);
    let second =
        i.claim_due_delivery(&subscriber, &id, DeliveryToken::generate(), 1015).unwrap().unwrap();
    assert_eq!(first.wake_key, second.wake_key);
    assert_eq!(first.view.observation, second.view.observation);
    let app = rusqlite::Connection::open(&database).unwrap();
    app.execute(
        "INSERT OR IGNORE INTO ingested VALUES(?,?)",
        rusqlite::params![id.as_str(), second.view.content.content.as_str()],
    )
    .unwrap();
    assert_eq!(
        app.query_row("SELECT count(*) FROM ingested", [], |r| r.get::<_, u32>(0)).unwrap(),
        1
    );
    let ack = i.acknowledge(&subscriber, &id, &first.token, 1016).unwrap();
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(Ingress::new(&root).acknowledge(&subscriber, &id, &first.token, 1017).unwrap(), ack);
    root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    let state = root.ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.deliveries[&id].acknowledgment, Some(ack));
    drop(root);
    let backup = s.temp.path().join("backup");
    let restored = s.temp.path().join("restored");
    StoreRoot::backup_offline(&path, &backup).unwrap();
    StoreRoot::restore_offline(&backup, &restored).unwrap();
    let root = StoreRoot::open_existing(&restored).unwrap();
    assert_eq!(root.ingress_snapshot(&tenant).unwrap(), state);
    let db = std::fs::read(restored.join("fabric.sqlite")).unwrap();
    assert!(!db
        .windows(first.token.expose_to_subscriber().len())
        .any(|w| w == first.token.expose_to_subscriber().as_bytes()));
}
#[test]
fn delayed_ack_after_exhaustion_token_misuse_revocation_and_expiration() {
    let s = Setup::new();
    let id = delivery(s.admit(&s.event("one", 0), 1000));
    let i = Ingress::new(&s.root);
    let sub = s.subscriber();
    let first = i.claim_due_delivery(&sub, &id, DeliveryToken::generate(), 1000).unwrap().unwrap();
    assert!(i.claim_due_delivery(&sub, &id, DeliveryToken::generate(), 1001).is_err());
    assert!(i.acknowledge(&sub, &id, &DeliveryToken::generate(), 1001).is_err());
    let wrong = AuthenticatedScope { tenant: r("other"), principal: sub.principal.clone() };
    assert!(i.inspect_delivery(&wrong, &id, 1001).is_err());
    assert!(i.acknowledge(&wrong, &id, &first.token, 1001).is_err());
    let wrong =
        AuthenticatedScope { tenant: sub.tenant.clone(), principal: PrincipalId::generate() };
    assert!(i.acknowledge(&wrong, &id, &first.token, 1001).is_err());
    i.claim_due_delivery(&sub, &id, DeliveryToken::generate(), 1015).unwrap();
    assert!(i.claim_due_delivery(&sub, &id, DeliveryToken::generate(), 1030).unwrap().is_none());
    assert_eq!(s.state().deliveries[&id].state, DeliveryState::DeadLetter);
    i.acknowledge(&sub, &id, &first.token, 1031).unwrap();
    assert_eq!(s.state().deliveries[&id].state, DeliveryState::Acknowledged);
    let id2 = delivery(s.admit(&s.event("two", 1), 1031));
    let issued =
        i.claim_due_delivery(&sub, &id2, DeliveryToken::generate(), 1031).unwrap().unwrap();
    assert!(i.acknowledge(&sub, &id2, &first.token, 1031).is_err());
    i.invalidate_token(&s.owner(), &id2, 1031).unwrap();
    assert!(i.acknowledge(&sub, &id2, &issued.token, 1031).is_err());
    let id3 = delivery(s.admit(&s.event("three", 2), 1031));
    let issued =
        i.claim_due_delivery(&sub, &id3, DeliveryToken::generate(), 1031).unwrap().unwrap();
    i.change_binding(&s.owner(), &s.version, BindingStatus::Revoked, 1031).unwrap();
    assert!(i.inspect_delivery(&sub, &id3, 1031).is_err());
    assert!(i.claim_due_delivery(&sub, &id3, DeliveryToken::generate(), 1046).is_err());
    // Revocation blocks disclosure, but already durably ingested data may still be acknowledged.
    i.acknowledge(&sub, &id3, &issued.token, 1046).unwrap();
    assert!(i.expire_delivery(&sub.tenant, &id2, 1046).is_err());
    i.expire_delivery(&sub.tenant, &id2, 2031).unwrap();
    assert_eq!(s.state().deliveries[&id2].state, DeliveryState::Expired);
    assert!(i.acknowledge(&sub, &id2, &issued.token, 2031).is_err());
    assert_eq!(s.state().admissions.len(), 3);
}
#[test]
fn expired_outbox_sweep_survives_restart_and_rejects_recovery() {
    let s = Setup::new();
    let first = delivery(s.admit(&s.event("first", 0), 1000));
    let second = delivery(s.admit(&s.event("second", 1), 1000));
    let mut expiration_order = [first.clone(), second.clone()];
    expiration_order.sort();
    let tenant = s.b.profile.scope.tenant.clone();
    let subscriber = s.subscriber();
    let ingress = Ingress::new(&s.root);
    let issued = ingress
        .claim_due_delivery(&subscriber, &first, DeliveryToken::generate(), 1000)
        .unwrap()
        .unwrap();
    ingress.invalidate_token(&s.owner(), &second, 1000).unwrap();
    assert!(ingress.expire_due_deliveries(&tenant, 1999, 1).unwrap().is_empty());
    assert!(ingress.expire_due_deliveries(&tenant, 2000, 0).is_err());

    let mut state = s.state();
    let request = b"task".to_vec();
    let signal = b"signal".to_vec();
    let queue = QueueObligation {
        version: 1,
        delivery: first.clone(),
        task_digest: ContentHash::of(&request),
        signal_digest: ContentHash::of(&signal),
        task_request: request,
        signal_request: signal,
        task_admitted: true,
        signal_sequence: Some(1),
        retired_wake_sequence: None,
        generation: 0,
        prior: vec![],
    };
    state.queue.insert(first.clone(), queue.clone());
    let mut replacement = queue.clone();
    replacement.generation = 1;
    replacement.task_admitted = false;
    replacement.signal_sequence = None;
    assert!(apply_ingress(
        &state,
        &tenant,
        1001,
        &IngressCommand::RecoverQueue {
            id: first.clone(),
            expected_generation: 0,
            replacement: Box::new(replacement.clone()),
        },
    )
    .is_ok());
    let mut invalidated = s.state();
    let mut invalidated_queue = queue.clone();
    invalidated_queue.delivery = second.clone();
    invalidated.queue.insert(second.clone(), invalidated_queue);
    let mut invalidated_replacement = replacement.clone();
    invalidated_replacement.delivery = second.clone();
    assert!(apply_ingress(
        &invalidated,
        &tenant,
        1001,
        &IngressCommand::RecoverQueue {
            id: second.clone(),
            expected_generation: 0,
            replacement: Box::new(invalidated_replacement),
        },
    )
    .is_err());
    assert!(apply_ingress(
        &state,
        &tenant,
        2000,
        &IngressCommand::RecoverQueue {
            id: first.clone(),
            expected_generation: 0,
            replacement: Box::new(replacement),
        },
    )
    .is_err());

    let path = s.temp.path().join("root");
    drop(s.root);
    let root = StoreRoot::open_existing(&path).unwrap();
    let ingress = Ingress::new(&root);
    assert_eq!(
        ingress.expire_due_deliveries(&tenant, 2000, 1).unwrap(),
        vec![expiration_order[0].clone()]
    );
    assert_eq!(
        root.ingress_snapshot(&tenant).unwrap().deliveries[&expiration_order[0]].state,
        DeliveryState::Expired
    );
    drop(root);
    let root = StoreRoot::open_existing(&path).unwrap();
    let ingress = Ingress::new(&root);
    assert_eq!(
        ingress.expire_due_deliveries(&tenant, 2000, 1).unwrap(),
        vec![expiration_order[1].clone()]
    );
    assert!(ingress.expire_due_deliveries(&tenant, 2000, 1).unwrap().is_empty());
    let state = root.ingress_snapshot(&tenant).unwrap();
    assert!(state
        .deliveries
        .values()
        .all(|d| d.state == DeliveryState::Expired && d.token_invalidated));
    assert!(ingress.acknowledge(&subscriber, &first, &issued.token, 2000).is_err());
    assert!(ingress.due_deliveries(&subscriber, 2000, 10).unwrap().is_empty());
}
#[test]
fn gaps_reconnects_and_out_of_order_preserve_safe_committed_prefix() {
    let s = Setup::new();
    let i = Ingress::new(&s.root);
    for (offset, next, gap, out_of_order) in
        [(2, 0, Some((0, 2)), false), (0, 1, None, true), (1, 3, None, true)]
    {
        let e = s.event(&format!("event-{offset}"), offset);
        let result = i
            .admit_provider_event(
                stream(&e, &s.b.profile, offset),
                vec![s.version.clone()],
                ids(1),
                1000,
            )
            .unwrap();
        match &result {
            IngressResult::Admitted { contiguous_next, .. } => assert_eq!(*contiguous_next, next),
            _ => panic!(),
        };
        let id = delivery(result);
        let view = i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap();
        assert_eq!(view.ordering.gap, gap);
        assert_eq!(view.ordering.out_of_order, out_of_order);
        assert_eq!(view.provenance.reconnect_from, Some(offset));
    }
    assert_eq!(contiguous(&s.state(), &s.b.profile), 3);
    let e = s.event("failed", 3);
    assert!(i
        .admit_provider_event(
            stream(&e, &s.b.profile, 3),
            vec![BindingVersion { id: s.version.id.clone(), revision: 9 }],
            ids(1),
            1000
        )
        .is_err());
    assert_eq!(contiguous(&s.state(), &s.b.profile), 3);
}
#[test]
fn quarantine_and_typed_lifecycle_evidence_do_not_create_authority() {
    let s = Setup::new();
    let mut e = s.event("event", 0);
    let artifact = s
        .root
        .publish(provider_artifact_scope(&s.b.profile.scope).unwrap(), b"attachment")
        .unwrap();
    e.attachments.push(GovernedAttachment {
        artifact: artifact.clone(),
        quarantine: Some(r("quarantine-1")),
    });
    e.profile = ObservationProfile::RoleStewardship {
        capacity: ActingCapacityRef {
            principal_ref: s.b.principal.clone(),
            assignment_ref: r("asserted-role"),
            organization_ref: None,
            attestation_ref: Some(s.b.authorization.clone()),
        },
        evidence: s.b.authorization.clone(),
    };
    let id = delivery(s.admit(&e, 1000));
    let i = Ingress::new(&s.root);
    let view = i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap();
    assert!(view.attachments.is_empty());
    assert_eq!(view.profile, e.profile);
    assert!(i
        .release_attachment(
            &s.b.profile.scope.tenant,
            artifact.clone(),
            r("wrong"),
            s.b.authorization.clone(),
            1000
        )
        .is_err());
    i.release_attachment(
        &s.b.profile.scope.tenant,
        artifact.clone(),
        r("quarantine-1"),
        s.b.authorization.clone(),
        1000,
    )
    .unwrap();
    assert_eq!(i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap().attachments, vec![artifact]);
    assert_eq!(view.receipt.admitting_binding.id, s.b.id);
    assert_eq!(view.receipt.authorization, s.b.authorization);
    let state = s.state();
    let admission =
        state.admissions.values().find(|a| a.observation.id == view.observation).unwrap();
    assert_eq!(admission.observation.asserted_sender, e.asserted_sender);
    let ReceiptBody::Ingress(receipt) = &admission.receipt.body else {
        panic!("expected ingress provenance receipt");
    };
    assert_eq!(receipt.validation.as_str(), "transport-validated-no-semantic-authority");
    assert_eq!(
        receipt.observation_digest,
        Envelope::new(Record::BoundaryObservation(admission.observation.clone())).digest().unwrap()
    );
    // Transport validation and asserted role evidence confer no application
    // semantics. Each forbidden claim is absent and rejected by the wire schema.
    let body = serde_json::to_value(receipt).unwrap();
    let canonical_receipt = serde_json::to_value(&admission.receipt).unwrap();
    for claim in ["content_truth", "truth", "urgency", "conversation_membership", "authority"] {
        assert!(body.get(claim).is_none(), "receipt body must not claim {claim}");
        assert!(canonical_receipt.get(claim).is_none(), "receipt must not claim {claim}");
        let mut forged = body.clone();
        forged[claim] = serde_json::Value::Bool(true);
        assert!(serde_json::from_value::<IngressReceipt>(forged).is_err(), "reject {claim}");
    }
    let mut e = s.event("commission", 1);
    e.profile = ObservationProfile::Commissioning { evidence: s.b.authorization.clone() };
    let id = delivery(s.admit(&e, 1000));
    assert_eq!(i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap().profile, e.profile);
    assert!(s.root.repository(StoreKind::Platform).is_err());
}
#[test]
fn capacity_tombstones_profiles_and_atomic_fanout_failure() {
    let mut b = fixture();
    b.profile.capacity = 2;
    let s = Setup::with(b);
    let e = s.event("one", 0);
    s.admit(&e, 1000);
    s.admit(&s.event("two", 1), 1000);
    assert!(Ingress::new(&s.root)
        .admit_provider_event(s.http(&s.event("three", 2)), vec![s.version.clone()], ids(1), 1000)
        .is_err());
    assert!(matches!(s.admit(&e, 1000), IngressResult::Admitted { duplicate: true, .. }));
    for strategy in [
        IdentityStrategy::StreamPosition,
        IdentityStrategy::ContentWindow { window_ms: 1000 },
        IdentityStrategy::NonDeduplicable,
    ] {
        let mut b = fixture();
        b.profile.identity = strategy.clone();
        let s = Setup::with(b);
        let e = s.event("id", 0);
        s.admit(&e, 1000);
        s.admit(&e, 1000);
        assert_eq!(s.state().admissions.len(), 1);
        if strategy == IdentityStrategy::NonDeduplicable {
            s.admit(&s.event("another", 1), 1000);
            assert_eq!(s.state().admissions.len(), 2);
        }
    }
    let s = Setup::new();
    let mut b = s.b.clone();
    b.id = IngressBindingId::generate();
    b.submission.client_request_id = "two".to_owned().try_into().unwrap();
    let v = Ingress::new(&s.root)
        .submit_binding(&s.owner(), b.submission.clone(), b.id.clone(), &Authority(b), 1000)
        .unwrap();
    let mut versions = vec![s.version.clone(), v];
    versions.sort_by(|a, b| a.id.cmp(&b.id));
    let repeated = DeliveryId::generate();
    let mut generated = ids(2);
    generated.deliveries = vec![repeated.clone(), repeated];
    assert!(Ingress::new(&s.root)
        .admit_provider_event(s.http(&s.event("event", 0)), versions, generated, 1000)
        .is_err());
    let state = s.state();
    assert!(state.admissions.is_empty());
    assert!(state.deliveries.is_empty());
    let mut p = s.b.policy.clone();
    p.physical_erasure = true;
    assert!(validate_policy(&p).is_err());
}

#[test]
fn audience_matching_and_subscriber_receipt_isolate_principals() {
    let s = Setup::new();
    let mut second = s.b.clone();
    second.id = IngressBindingId::generate();
    second.principal = PrincipalId::generate();
    second.subscriber = PrincipalId::generate();
    second.submission.client_request_id = "second-request".to_owned().try_into().unwrap();
    second.submission.subscriber = r("second-endpoint");
    second.authorization.artifact =
        s.root.publish(s.b.profile.scope.tenant.clone(), b"second-authorization").unwrap();
    let second_scope = AuthenticatedScope {
        tenant: s.b.profile.scope.tenant.clone(),
        principal: second.principal.clone(),
    };
    let second_version = Ingress::new(&s.root)
        .submit_binding(
            &second_scope,
            second.submission.clone(),
            second.id.clone(),
            &Authority(second.clone()),
            1000,
        )
        .unwrap();
    let first_event = s.event("first-only", 0);
    let first_id = delivery(s.admit(&first_event, 1000));
    assert!(Ingress::new(&s.root)
        .inspect_delivery(
            &AuthenticatedScope {
                tenant: second_scope.tenant.clone(),
                principal: second.subscriber.clone(),
            },
            &first_id,
            1000,
        )
        .is_err());
    let mut both = s.event("both", 1);
    both.information_use.allowed_audiences.push(second.subscriber.clone());
    let mut versions = vec![s.version.clone(), second_version.clone()];
    versions.sort_by(|a, b| a.id.cmp(&b.id));
    let result =
        Ingress::new(&s.root).admit_provider_event(s.http(&both), versions, ids(2), 1000).unwrap();
    let IngressResult::Admitted { deliveries, .. } = result else { panic!("not admitted") };
    let state = s.state();
    let second_id =
        deliveries.iter().find(|id| state.deliveries[*id].subscriber == second.subscriber).unwrap();
    let view = Ingress::new(&s.root)
        .inspect_delivery(
            &AuthenticatedScope { tenant: second_scope.tenant, principal: second.subscriber },
            second_id,
            1000,
        )
        .unwrap();
    assert_eq!(view.receipt.admitting_binding, second_version);
    assert_eq!(view.receipt.authorization, second.authorization);
    assert_ne!(view.receipt.authorization, s.b.authorization);
}

#[test]
fn exact_byte_replay_does_not_consume_non_deduplicable_capacity() {
    let mut binding = fixture();
    binding.profile.identity = IdentityStrategy::NonDeduplicable;
    binding.profile.capacity = 2;
    let s = Setup::with(binding);
    let mut event = s.event("not-a-key", 0);
    event.offset = None;
    let ingress = Ingress::new(&s.root);
    let first = delivery(
        ingress
            .admit_provider_event(s.http(&event), vec![s.version.clone()], ids(1), 1000)
            .unwrap(),
    );
    for _ in 0..5 {
        let result = ingress
            .admit_provider_event(s.http(&event), vec![s.version.clone()], ids(1), 1000)
            .unwrap();
        assert!(
            matches!(result, IngressResult::Admitted { duplicate: true, ref deliveries, .. } if deliveries == &vec![first.clone()])
        );
    }
    let mut stream_event = event.clone();
    stream_event.offset = Some(1);
    let second = delivery(
        ingress
            .admit_provider_event(
                stream(&stream_event, &s.b.profile, 0),
                vec![s.version.clone()],
                ids(1),
                1000,
            )
            .unwrap(),
    );
    assert_ne!(first, second);
    for _ in 0..5 {
        let result = ingress
            .admit_provider_event(
                stream(&stream_event, &s.b.profile, 0),
                vec![s.version.clone()],
                ids(1),
                1000,
            )
            .unwrap();
        assert!(
            matches!(result, IngressResult::Admitted { duplicate: true, ref deliveries, .. } if deliveries == &vec![second.clone()])
        );
    }
    assert_eq!(s.state().admissions.len(), 2);
}
#[test]
fn old_manifest_profile_rejects_without_modification() {
    let s = Setup::new();
    let path = s.temp.path().join("root");
    drop(s.root);
    let manifest = path.join("manifest.json");
    let old =
        std::fs::read_to_string(&manifest).unwrap().replace("local-receipt-2", "local-domain-1");
    std::fs::write(&manifest, &old).unwrap();
    let db = std::fs::read(path.join("fabric.sqlite")).unwrap();
    assert!(StoreRoot::open_existing(&path).is_err());
    assert_eq!(std::fs::read_to_string(&manifest).unwrap(), old);
    assert_eq!(std::fs::read(path.join("fabric.sqlite")).unwrap(), db);
}

#[test]
fn admitted_ingress_receipt_uses_canonical_receipt_profile() {
    let s = Setup::new();
    let event = s.event("event:canonical-receipt", 1);
    s.admit(&event, 1000);
    let receipt = s.state().admissions.values().next().unwrap().receipt.clone();
    assert_eq!(receipt.profile.as_str(), worldinterface_protocol::receipt::BOUNDARY_PROFILE);
    assert_eq!(receipt.versions.policy, s.b.policy.version);
    let envelope = Envelope::new(Record::BoundaryReceipt(receipt));
    let canonical = envelope.canonical_bytes().unwrap();
    assert_eq!(Envelope::parse_receipt(&canonical).unwrap(), envelope);
}

#[test]
fn every_ingress_transaction_write_failure_rolls_back_complete_lifecycle() {
    for table in ["ingress_history", "ingress_state"] {
        let s = Setup::new();
        let e = s.event("one", 0);
        let i = Ingress::new(&s.root);
        let connection =
            rusqlite::Connection::open(s.temp.path().join("root/fabric.sqlite")).unwrap();
        let inject = || {
            connection.execute_batch(&format!("CREATE TRIGGER injected_failure BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'injected'); END;")).unwrap()
        };
        let clear = || connection.execute_batch("DROP TRIGGER injected_failure").unwrap();
        let before = s.state();
        inject();
        assert!(i.admit_provider_event(s.http(&e), vec![s.version.clone()], ids(1), 1000).is_err());
        clear();
        assert_eq!(s.state(), before);
        let id = delivery(s.admit(&e, 1000));
        let before = s.state();
        inject();
        assert!(i
            .claim_due_delivery(&s.subscriber(), &id, DeliveryToken::generate(), 1000)
            .is_err());
        clear();
        assert_eq!(s.state(), before);
        let issued = i
            .claim_due_delivery(&s.subscriber(), &id, DeliveryToken::generate(), 1000)
            .unwrap()
            .unwrap();
        let before = s.state();
        inject();
        assert!(i.acknowledge(&s.subscriber(), &id, &issued.token, 1000).is_err());
        clear();
        assert_eq!(s.state(), before);
        let state = s.state();
        s.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(s.state(), state);
        drop(connection);
        let path = s.temp.path().join("root");
        drop(s.root);
        assert!(StoreRoot::open_existing(path).is_ok());
    }
}
#[test]
fn replay_detects_corrupt_projection_and_repairs_only_from_authoritative_history() {
    let s = Setup::new();
    s.admit(&s.event("one", 0), 1000);
    let expected = s.state();
    let path = s.temp.path().join("root");
    let c = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
    c.execute(
        "UPDATE ingress_state SET state=?",
        [canonical_collection(&IngressState::default()).unwrap()],
    )
    .unwrap();
    assert!(StoreRoot::open_existing(&path).is_err());
    s.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(s.state(), expected);
    assert!(c.execute("DELETE FROM ingress_history", []).is_err());
    assert!(c.execute("UPDATE ingress_history SET digest='invalid'", []).is_err());
    c.execute_batch("DROP TRIGGER ingress_no_update; UPDATE ingress_history SET digest='invalid'")
        .unwrap();
    drop(c);
    drop(s.root);
    assert!(StoreRoot::open_existing(&path).is_err());
}
#[test]
fn cumulative_ack_never_advances_on_conflicting_offset_or_failed_admission() {
    let s = Setup::new();
    s.admit(&s.event("one", 0), 1000);
    let mut conflict = s.event("different-id", 0);
    conflict.asserted_sender = r("different");
    assert_eq!(s.admit(&conflict, 1000), IngressResult::Conflict);
    assert_eq!(s.state().admissions.len(), 1);
    assert_eq!(contiguous(&s.state(), &s.b.profile), 1);
    let mut b = fixture();
    b.profile.identity = IdentityStrategy::ContentWindow { window_ms: 1000 };
    let s = Setup::with(b);
    let e = s.event("one", 0);
    s.admit(&e, 1000);
    let mut retry = e;
    retry.produced_at_ms = 1001;
    retry.event_id = Some(r("different"));
    retry.offset = Some(1);
    let body = canonical_data(&retry).unwrap();
    let validated = validate_http(&body, &sign(&body), &credentials(&s.b.profile), 1001).unwrap();
    assert!(matches!(
        Ingress::new(&s.root)
            .admit_provider_event(validated, vec![s.version.clone()], ids(1), 1001)
            .unwrap(),
        IngressResult::Admitted { duplicate: true, .. }
    ));
}
#[test]
fn acknowledgment_race_has_one_durable_result_and_clock_policy_is_explicit() {
    let s = Setup::new();
    let id = delivery(s.admit(&s.event("one", 0), 1000));
    let i = Ingress::new(&s.root);
    let issued = i
        .claim_due_delivery(&s.subscriber(), &id, DeliveryToken::generate(), 1000)
        .unwrap()
        .unwrap();
    let sub = s.subscriber();
    let acks = std::thread::scope(|scope| {
        let mut jobs = vec![];
        for _ in 0..4 {
            jobs.push(scope.spawn(|| {
                Ingress::new(&s.root).acknowledge(&sub, &id, &issued.token, 1001).unwrap()
            }));
        }
        jobs.into_iter().map(|j| j.join().unwrap()).collect::<Vec<_>>()
    });
    assert!(acks.iter().all(|a| a == &acks[0]));
    assert_eq!(s.state().deliveries[&id].state, DeliveryState::Acknowledged);
    assert!(i.inspect_delivery(&sub, &id, 1000).is_err());
    assert!(DeliveryToken::from_subscriber(&"é".repeat(36)).is_err());
}

#[test]
fn attachment_policy_and_information_expiration_are_validated_before_admission() {
    let s = Setup::new();
    let mut e = s.event("one", 0);
    let a = s
        .root
        .publish(provider_artifact_scope(&s.b.profile.scope).unwrap(), b"attachment")
        .unwrap();
    e.attachments = vec![GovernedAttachment { artifact: a, quarantine: None }];
    let body = canonical_data(&e).unwrap();
    assert!(validate_http(&body, &sign(&body), &credentials(&s.b.profile), 1000).is_err());
    e.attachments.clear();
    e.information_use.expiration =
        Some("1970-01-01T00:00:01.010000000Z".to_string().try_into().unwrap());
    let id = delivery(s.admit(&e, 1000));
    let i = Ingress::new(&s.root);
    let issued = i
        .claim_due_delivery(&s.subscriber(), &id, DeliveryToken::generate(), 1000)
        .unwrap()
        .unwrap();
    assert!(i.inspect_delivery(&s.subscriber(), &id, 1010).is_err());
    assert!(i.acknowledge(&s.subscriber(), &id, &issued.token, 1010).is_err());
    let mut e = s.event("missing-artifact", 1);
    e.content.content = ContentHash::of(b"unpublished");
    assert!(i.admit_provider_event(s.http(&e), vec![s.version.clone()], ids(1), 1000).is_err());
    assert_eq!(s.state().admissions.len(), 1);
}

#[test]
fn exact_byte_http_and_session_signatures_match_independent_vectors() {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/ingress/transport-vector.json"
    ))
    .unwrap();
    let unhex = |s: &str| {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>()
    };
    let b = fixture();
    let body = v["body"].as_str().unwrap().as_bytes();
    let signature = unhex(v["http_signature"].as_str().unwrap());
    assert!(validate_http(body, &signature, &credentials(&b.profile), 1000).is_ok());
    let mut body_changed = body.to_vec();
    body_changed.push(b' ');
    assert!(validate_http(&body_changed, &signature, &credentials(&b.profile), 1000).is_err());
    let session = StreamSession::authenticate(
        v["session"].as_str().unwrap().as_bytes(),
        &unhex(v["session_signature"].as_str().unwrap()),
        credentials(&b.profile),
        1000,
    )
    .unwrap();
    assert!(session
        .validate_stream(body, &unhex(v["frame_signature"].as_str().unwrap()), 1000)
        .is_ok());
}

#[test]
fn provider_cannot_disclose_another_sources_artifact_within_same_tenant() {
    let s = Setup::new();
    let mut event = s.event("one", 0);
    let mut other = s.b.profile.scope.clone();
    other.account = r("another-account");
    event.content = s
        .root
        .publish(provider_artifact_scope(&other).unwrap(), b"other provider's secret")
        .unwrap();
    let bytes = canonical_data(&event).unwrap();
    assert!(validate_http(&bytes, &sign(&bytes), &credentials(&s.b.profile), 1000).is_err());
    event.content.scope = provider_artifact_scope(&s.b.profile.scope).unwrap();
    let valid = s.http(&event);
    assert!(Ingress::new(&s.root)
        .admit_provider_event(valid, vec![s.version.clone()], ids(1), 1000)
        .is_err());
    assert!(s.state().admissions.is_empty());
}

// F-001: exercise both public transport paths and the persisted disclosure boundary.
#[test]
fn nested_attestations_enforce_scope_availability_and_release_on_both_transports() {
    for streaming in [false, true] {
        let s = Setup::new();
        let i = Ingress::new(&s.root);
        let validate = |event: &ProviderEvent| {
            if streaming {
                try_stream(event, &s.b.profile, 0)
            } else {
                let body = canonical_data(event).unwrap();
                validate_http(&body, &sign(&body), &credentials(&s.b.profile), 1000)
            }
        };
        let mut event = s.event("attestation", 0);
        let provider_scope = provider_artifact_scope(&s.b.profile.scope).unwrap();
        let artifact = s.root.publish(provider_scope.clone(), b"attestation").unwrap();
        let evidence = |artifact: ArtifactRef| EvidenceRef { artifact, schema: r("role-v1") };
        let role = |attestation| ObservationProfile::RoleStewardship {
            capacity: ActingCapacityRef {
                principal_ref: s.b.principal.clone(),
                assignment_ref: r("role"),
                organization_ref: None,
                attestation_ref: Some(evidence(attestation)),
            },
            evidence: s.b.authorization.clone(),
        };
        let mut foreign_provider = s.b.profile.scope.clone();
        foreign_provider.account = r("other-account");
        for scope in
            [r("other-tenant"), provider_artifact_scope(&foreign_provider).unwrap(), provider_scope]
        {
            let reference = s.root.publish(scope, b"undeclared").unwrap();
            event.profile = role(reference);
            assert!(validate(&event).is_err());
            assert!(s.state().admissions.is_empty());
        }
        // A valid tenant scope is insufficient if the referenced bytes are absent.
        let mut missing = s.b.authorization.artifact.clone();
        missing.content = ContentHash::of(b"not-published");
        event.profile = role(missing);
        assert!(i
            .admit_provider_event(validate(&event).unwrap(), vec![s.version.clone()], ids(1), 1000)
            .is_err());
        assert!(s.state().admissions.is_empty());
        event.profile = role(artifact.clone());
        event.attachments.push(GovernedAttachment {
            artifact: artifact.clone(),
            quarantine: Some(r("quarantine-1")),
        });
        let id = delivery(
            i.admit_provider_event(
                validate(&event).unwrap(),
                vec![s.version.clone()],
                ids(1),
                1000,
            )
            .unwrap(),
        );
        let before = s.state();
        let view = i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap();
        assert!(view.attachments.is_empty());
        let ObservationProfile::RoleStewardship { capacity, .. } = view.profile else { panic!() };
        assert!(capacity.attestation_ref.is_none());
        let issued = i
            .claim_due_delivery(&s.subscriber(), &id, DeliveryToken::generate(), 1000)
            .unwrap()
            .unwrap();
        let ObservationProfile::RoleStewardship { capacity, .. } = issued.view.profile else {
            panic!()
        };
        assert!(capacity.attestation_ref.is_none());
        assert_eq!(s.state().admissions, before.admissions); // redaction never rewrites evidence
        assert!(i
            .release_attachment(
                &s.b.profile.scope.tenant,
                artifact.clone(),
                r("wrong-policy"),
                s.b.authorization.clone(),
                1000
            )
            .is_err());
        assert!(i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap().attachments.is_empty());
        i.release_attachment(
            &s.b.profile.scope.tenant,
            artifact.clone(),
            r("quarantine-1"),
            s.b.authorization.clone(),
            1000,
        )
        .unwrap();
        let view = i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap();
        assert_eq!(view.profile, event.profile);
        assert_eq!(view.attachments, vec![artifact]);
        s.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(i.inspect_delivery(&s.subscriber(), &id, 1000).unwrap(), view);
    }
}

#[test]
fn quarantined_content_alias_is_denied_until_release() {
    let s = Setup::new();
    let mut e = s.event("alias", 0);
    e.attachments.push(GovernedAttachment {
        artifact: e.content.clone(),
        quarantine: Some(r("quarantine-1")),
    });
    let id = delivery(s.admit(&e, 1000));
    let i = Ingress::new(&s.root);
    assert!(i.inspect_delivery(&s.subscriber(), &id, 1000).is_err());
    // Omitting the attachment from another event does not clear its quarantine.
    e.event_id = Some(r("alias-again"));
    e.offset = Some(1);
    e.attachments.clear();
    let second = delivery(s.admit(&e, 1000));
    assert!(i.inspect_delivery(&s.subscriber(), &second, 1000).is_err());
    i.release_attachment(
        &s.b.profile.scope.tenant,
        e.content.clone(),
        r("quarantine-1"),
        s.b.authorization.clone(),
        1000,
    )
    .unwrap();
    assert_eq!(i.inspect_delivery(&s.subscriber(), &second, 1000).unwrap().content, e.content);
}

// F-002: a duplicate content identity cannot override occupied-position evidence.
#[test]
fn content_window_duplicate_preserves_conflicting_committed_offset() {
    let mut b = fixture();
    b.profile.identity = IdentityStrategy::ContentWindow { window_ms: 1000 };
    let s = Setup::with(b);
    let a = s.event("a", 0);
    let original = delivery(s.admit(&a, 1000));
    let mut b = s.event("b", 1);
    b.asserted_sender = r("different-content");
    s.admit(&b, 1000);
    let before = s.state();
    let mut collision = a.clone();
    collision.offset = Some(1);
    assert_eq!(s.admit(&collision, 1000), IngressResult::Conflict);
    let after = s.state();
    assert_eq!(after.admissions, before.admissions);
    assert_eq!(after.deliveries, before.deliveries);
    assert_eq!(after.conflicts.len(), 1);
    assert_eq!(contiguous(&after, &s.b.profile), 2);
    assert_eq!(s.admit(&collision, 1000), IngressResult::Conflict);
    assert_eq!(s.state(), after);
    for offset in [0, 2] {
        let mut retry = a.clone();
        retry.offset = Some(offset);
        assert!(matches!(
            s.admit(&retry, 1000),
            IngressResult::Admitted { duplicate: true, contiguous_next: 2, .. }
        ));
        assert_eq!(delivery(s.admit(&retry, 1000)), original);
        assert_eq!(s.state(), after);
    }
    s.root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    let reopened = StoreRoot::open_existing(s.temp.path().join("root")).unwrap();
    assert_eq!(reopened.ingress_snapshot(&s.b.profile.scope.tenant).unwrap(), after);
}

// F-003: generated commands exercise apply_ingress directly. Models use small
// integer keys and lifecycle facts, not the reducer's identity or transition logic.
mod generated {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn runner() -> proptest::test_runner::TestRunner {
        proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        })
    }
    fn replay(
        initial: &IngressState,
        tenant: &OpaqueRef,
        commands: &[(u64, IngressCommand)],
        expected: &IngressState,
    ) {
        let bytes = canonical_collection(&commands).unwrap();
        let decoded: Vec<(u64, IngressCommand)> = serde_json::from_slice(&bytes).unwrap();
        let mut actual = initial.clone();
        for (now, command) in decoded {
            actual = apply_ingress(&actual, tenant, now, &command).unwrap().0;
        }
        assert_eq!(&actual, expected);
    }
    #[test]
    fn generated_ingress_binding_versions_match_model_and_replay() {
        let b = fixture();
        let tenant = b.profile.scope.tenant.clone();
        runner()
            .run(&prop::collection::vec((0u8..4, any::<bool>()), 1..100), |choices| {
                let initial = IngressState::default();
                let bind = IngressCommand::Bind { binding: Box::new(b.clone()) };
                let mut state = apply_ingress(&initial, &tenant, 1000, &bind).unwrap().0;
                let mut commands = vec![(1000, bind)];
                let (mut revision, mut status) = (1, BindingStatus::Active);
                for (choice, stale) in choices {
                    let before = state.clone();
                    let next =
                        [BindingStatus::Active, BindingStatus::Suspended, BindingStatus::Revoked]
                            [choice as usize % 3]
                            .clone();
                    let command = if choice == 3 {
                        IngressCommand::Bind { binding: Box::new(b.clone()) }
                    } else {
                        IngressCommand::ChangeBinding {
                            id: b.id.clone(),
                            expected: if stale { revision - 1 } else { revision },
                            status: next.clone(),
                        }
                    };
                    let allowed = choice == 3
                        || (!stale && status != BindingStatus::Revoked && status != next);
                    let result = apply_ingress(&state, &tenant, 1000, &command);
                    prop_assert_eq!(result.is_ok(), allowed);
                    if let Ok((new, result)) = result {
                        if choice == 3 {
                            prop_assert_eq!(
                                result,
                                IngressResult::Binding(BindingVersion {
                                    id: b.id.clone(),
                                    revision
                                })
                            );
                            prop_assert_eq!(&new, &before);
                        } else {
                            revision += 1;
                            status = next;
                        }
                        state = new;
                        commands.push((1000, command));
                    }
                    prop_assert_eq!(state.bindings[&b.id].revision, revision);
                    prop_assert_eq!(&state.bindings[&b.id].status, &status);
                    prop_assert_eq!(state.bindings.len(), 1);
                }
                replay(&initial, &tenant, &commands, &state);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn generated_ingress_dedup_offsets_match_independent_model_and_replay() {
        let mut b = fixture();
        b.profile.identity = IdentityStrategy::ContentWindow { window_ms: 1000 };
        let setup = Setup::with(b);
        setup.admit(&setup.event("template", 0), 1000);
        let template = setup.state().admissions.into_values().next().unwrap();
        let tenant = setup.b.profile.scope.tenant.clone();
        let initial = apply_ingress(
            &IngressState::default(),
            &tenant,
            1000,
            &IngressCommand::Bind { binding: Box::new(setup.b.clone()) },
        )
        .unwrap()
        .0;
        runner()
            .run(&prop::collection::vec((0u8..8, 0u64..8), 1..100), |choices| {
                let mut state = initial.clone();
                let mut commands = vec![];
                // Content -> first admitted observation; position -> committed content.
                let mut contents: BTreeMap<u8, (ObservationId, Vec<DeliveryId>)> = BTreeMap::new();
                let mut positions = BTreeMap::new();
                let mut conflicts = BTreeSet::new();
                for (step, (content, offset)) in choices.into_iter().enumerate() {
                    let mut a = template.clone();
                    a.event.asserted_sender = r(&format!("content-{content}"));
                    a.event.offset = Some(offset);
                    a.observation.id =
                        format!("00000000-0000-4000-8000-{:012x}", step + 100).try_into().unwrap();
                    a.receipt.id =
                        format!("00000000-0000-4000-8000-{:012x}", step + 200).try_into().unwrap();
                    a.deliveries = vec![format!("00000000-0000-4000-8000-{:012x}", step + 300)
                        .try_into()
                        .unwrap()];
                    a.identity =
                        provider_identity(&a.profile, &a.event, &a.observation.id).unwrap();
                    a.producer_digest = producer_digest(&a.profile, &a.event).unwrap();
                    a.provenance.exact_bytes_digest =
                        ContentHash::of(&canonical_data(&a.event).unwrap());
                    a.observation.deduplication_key = r(a.identity.as_str());
                    a.observation.asserted_sender = a.event.asserted_sender.clone();
                    if let ReceiptBody::Ingress(receipt) = &mut a.receipt.body {
                        receipt.observation_id = a.observation.id.clone();
                        receipt.observation_digest =
                            Envelope::new(Record::BoundaryObservation(a.observation.clone()))
                                .digest()
                                .unwrap();
                    }
                    let before = state.clone();
                    let command = IngressCommand::Admit { admission: Box::new(a.clone()) };
                    let (new, result) = apply_ingress(&state, &tenant, 1000, &command).unwrap();
                    if let Some(old) = positions.get(&offset).filter(|old| **old != content) {
                        conflicts.insert((*old, content));
                        prop_assert_eq!(result, IngressResult::Conflict);
                        prop_assert_eq!(&new.admissions, &before.admissions);
                        prop_assert_eq!(&new.deliveries, &before.deliveries);
                    } else if let Some((observation, deliveries)) = contents.get(&content) {
                        let mut next = 0;
                        while positions.contains_key(&next) {
                            next += 1;
                        }
                        prop_assert_eq!(
                            result,
                            IngressResult::Admitted {
                                observation: observation.clone(),
                                deliveries: deliveries.clone(),
                                duplicate: true,
                                contiguous_next: next
                            }
                        );
                        prop_assert_eq!(&new, &before);
                    } else {
                        contents.insert(content, (a.observation.id.clone(), a.deliveries.clone()));
                        positions.insert(offset, content);
                        prop_assert!(
                            matches!(result, IngressResult::Admitted { duplicate: false, .. }),
                            "new content must be admitted"
                        );
                    }
                    state = new;
                    commands.push((1000, command));
                    prop_assert_eq!(state.admissions.len(), contents.len());
                    prop_assert_eq!(state.deliveries.len(), contents.len());
                    prop_assert_eq!(state.conflicts.len(), conflicts.len());
                    let mut next = 0;
                    while positions.contains_key(&next) {
                        next += 1;
                    }
                    prop_assert_eq!(contiguous(&state, &setup.b.profile), next);
                }
                replay(&initial, &tenant, &commands, &state);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn generated_ingress_delivery_closure_tokens_expiration_and_replay() {
        let mut b = fixture();
        b.policy.retention_ms = 60;
        let setup = Setup::with(b);
        let id = delivery(setup.admit(&setup.event("template", 0), 1000));
        let initial = setup.state();
        let tenant = setup.b.profile.scope.tenant.clone();
        runner()
            .run(&prop::collection::vec((0u8..12, 0u64..10, any::<bool>()), 1..100), |choices| {
                let mut state = initial.clone();
                let mut commands = vec![];
                let mut now = 1000;
                let mut tokens = Vec::new();
                let mut statuses = Vec::new();
                let mut next_due = 1000;
                let mut closed = DeliveryState::Pending;
                let mut invalidated = false;
                let mut acknowledged_at = None;
                for (step, (choice, delta, wrong)) in choices.into_iter().enumerate() {
                    now += delta;
                    let token = ContentHash::of(format!("token-{step}").as_bytes());
                    let subscriber =
                        if wrong { setup.b.principal.clone() } else { setup.b.subscriber.clone() };
                    let (command, allowed) = match choice {
                        0..=4 => (
                            IngressCommand::Claim {
                                id: id.clone(),
                                subscriber,
                                token_digest: token.clone(),
                            },
                            !wrong
                                && !invalidated
                                && now < 1060
                                && closed == DeliveryState::Pending
                                && now >= next_due,
                        ),
                        5..=8 => (
                            IngressCommand::Acknowledge {
                                id: id.clone(),
                                subscriber,
                                token_digest: tokens.first().unwrap_or(&token).clone(),
                            },
                            !wrong
                                && !invalidated
                                && now < 1060
                                && !tokens.is_empty()
                                && closed != DeliveryState::Expired,
                        ),
                        9 => (IngressCommand::InvalidateToken { id: id.clone() }, true),
                        10 => (IngressCommand::Expire { id: id.clone() }, now >= 1060),
                        _ => (
                            IngressCommand::Finish {
                                id: id.clone(),
                                attempt: 1,
                                succeeded: !wrong,
                            },
                            !tokens.is_empty()
                                && (statuses[0] == AttemptStatus::Issued
                                    || statuses[0]
                                        == if wrong {
                                            AttemptStatus::WakeFailed
                                        } else {
                                            AttemptStatus::WakeSucceeded
                                        }),
                        ),
                    };
                    let before = state.clone();
                    let result = apply_ingress(&state, &tenant, now, &command);
                    prop_assert_eq!(result.is_ok(), allowed);
                    if let Ok((new, result)) = result {
                        match choice {
                            0..=4 => {
                                if tokens.len() == 2 {
                                    closed = DeliveryState::DeadLetter;
                                } else {
                                    tokens.push(token);
                                    statuses.push(AttemptStatus::Issued);
                                    next_due = now + 15;
                                }
                            }
                            5..=8 => {
                                let at = *acknowledged_at.get_or_insert(now);
                                closed = DeliveryState::Acknowledged;
                                prop_assert_eq!(
                                    result,
                                    IngressResult::Acknowledged(DeliveryAcknowledgment {
                                        delivery: id.clone(),
                                        subscriber: setup.b.subscriber.clone(),
                                        acknowledged_at_ms: at,
                                        attempt: 1
                                    })
                                );
                            }
                            9 => invalidated = true,
                            10 => {
                                invalidated = true;
                                if closed != DeliveryState::Acknowledged {
                                    closed = DeliveryState::Expired;
                                }
                            }
                            _ => {
                                statuses[0] = if wrong {
                                    AttemptStatus::WakeFailed
                                } else {
                                    AttemptStatus::WakeSucceeded
                                }
                            }
                        }
                        state = new;
                        commands.push((now, command));
                    } else {
                        prop_assert_eq!(&state, &before);
                    }
                    let d = &state.deliveries[&id];
                    prop_assert_eq!(&d.state, &closed);
                    prop_assert_eq!(d.token_invalidated, invalidated);
                    prop_assert_eq!(d.attempts.len(), tokens.len());
                    prop_assert_eq!(
                        d.acknowledgment.as_ref().map(|ack| ack.acknowledged_at_ms),
                        acknowledged_at
                    );
                    prop_assert_eq!(
                        d.attempts.iter().map(|a| a.status.clone()).collect::<Vec<_>>(),
                        statuses.clone()
                    );
                    prop_assert_eq!(&state.admissions, &initial.admissions);
                }
                replay(&initial, &tenant, &commands, &state);
                Ok(())
            })
            .unwrap();
    }
}
