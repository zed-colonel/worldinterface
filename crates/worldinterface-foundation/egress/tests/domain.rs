use proptest::test_runner::TestRunner;
use std::collections::BTreeSet;
use worldinterface_domain::{
    agency::*, authorization::*, effect::*, ingress::digest, ports::*, resource::*, Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_storage::{Batch, StoreKind, StoreProfile, StoreRoot};
fn opaque(s: &str) -> OpaqueRef {
    s.to_owned().try_into().unwrap()
}
fn now() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../conformance/wi-fabric-2/egress/fixture.json"
    ))
    .unwrap();
    (
        serde_json::from_value(v["submission"].clone()).unwrap(),
        serde_json::from_value(v["snapshot"].clone()).unwrap(),
    )
}
fn trusted(s: &AuthorizationSnapshot) -> VerifiedSnapshot {
    VerifiedSnapshot::from_trusted_verifier(s.clone()).unwrap()
}
fn caller(s: &AuthorizationSnapshot) -> AuthenticatedCaller {
    AuthenticatedCaller::from_trusted_authentication(
        s.bindings[0].tenant.clone(),
        s.bindings[0].principal.clone(),
    )
}
fn root(t: &tempfile::TempDir) -> StoreRoot {
    let r = StoreRoot::create_empty(
        t.path().join("root"),
        StoreProfile { instance: InstanceId::generate(), created_at: now(), platform: false },
    )
    .unwrap();
    for name in [
        "input",
        "executable",
        "dependency",
        "configuration",
        "profile-definition",
        "adapter",
        "resolution",
        "grant",
        "fixture-review",
        "worker",
    ] {
        r.publish(opaque("tenant:fixture"), name.as_bytes()).unwrap();
    }
    r
}

fn submit(root: &StoreRoot, s: &EffectSubmission, p: &AuthorizationSnapshot) -> EffectIntentId {
    Egress::new(root)
        .apply(
            &caller(p),
            now(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(s.clone()),
                snapshot: trusted(p),
                link: None,
            },
        )
        .unwrap()
}
fn resolve(root: &StoreRoot, id: &EffectIntentId, p: &AuthorizationSnapshot) {
    Egress::new(root)
        .apply(&caller(p), now(), EffectAction::Resolve { id: id.clone(), snapshot: trusted(p) })
        .unwrap();
}
fn evaluate(root: &StoreRoot, id: &EffectIntentId, p: &AuthorizationSnapshot) -> EffectView {
    Egress::new(root)
        .apply(&caller(p), now(), EffectAction::Authorize { id: id.clone(), snapshot: trusted(p) })
        .unwrap();
    Egress::new(root).inspect(&caller(p), id).unwrap()
}
fn ready(root: &StoreRoot, s: &EffectSubmission, p: &AuthorizationSnapshot) -> EffectIntentId {
    let id = submit(root, s, p);
    resolve(root, &id, p);
    assert_eq!(evaluate(root, &id, p).phase, Phase::Authorized);
    id
}
fn requalify(p: &mut AuthorizationSnapshot) {
    p.fixture.as_mut().unwrap().realization = digest(p.worker.as_ref().unwrap()).unwrap();
}
fn allow_submission(p: &mut AuthorizationSnapshot, s: &EffectSubmission) {
    let d = digest(s).unwrap();
    p.bindings[0].principal_authority.insert(d.clone());
    p.bindings[0].capacity_authority.insert(d.clone());
    p.policy.platform.insert(d.clone());
    p.policy.resource.insert(d.clone());
    p.policy.information_use.insert(d.clone());
    p.grants[0].authority.insert(d);
}
#[derive(Default)]
struct Queue {
    calls: Vec<EffectAdmissionRequest>,
    lost: bool,
}
impl TestEffectAdmissionPort for Queue {
    fn ensure_effect(
        &mut self,
        r: &EffectAdmissionRequest,
    ) -> std::result::Result<Option<EffectAcknowledgment>, String> {
        self.calls.push(r.clone());
        if self.lost {
            self.lost = false;
            return Ok(None);
        }
        Ok(Some(EffectAcknowledgment {
            task: r.task.clone(),
            admission_key: r.admission_key.clone(),
            request_digest: digest(r)?,
            durable_sequence: 1,
        }))
    }
}
fn deny(p: AuthorizationSnapshot, reason: Reason) {
    let (s, _) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    resolve(&r, &id, &p);
    let v = evaluate(&r, &id, &p);
    assert_eq!(v.phase, Phase::Denied, "{v:?}");
    assert!(v.authorization.unwrap().reasons.contains(&reason));
    let mut queue = Queue::default();
    assert!(Egress::new(&r)
        .deliver_test(&caller(&p), now(), &id, trusted(&p), &mut queue)
        .is_err());
    assert!(queue.calls.is_empty());
}
#[test]
fn wp009_exact_fixture_durable_intent_and_admission() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
    assert_eq!(v.phase, Phase::Submitted);
    assert!(v.dispatch.is_none());
    let c = rusqlite::Connection::open(t.path().join("root/fabric.sqlite")).unwrap();
    let n: u32 = c
        .query_row("SELECT count(*) FROM effect_outbox WHERE dispatch=?", [b"[]".as_slice()], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(n, 1);
    resolve(&r, &id, &p);
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::Authorized);
    let mut q = Queue::default();
    assert!(Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap());
    assert_eq!(q.calls.len(), 1);
    let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
    assert_eq!(v.phase, Phase::DispatchPending);
    assert_eq!(v.commit, CommitObservation::NotAttempted);
    assert!(v.outcome.is_none());
}
#[test]
fn wp010_unreviewed_executable_cannot_be_waived() {
    let (_, mut p) = fixture();
    p.worker.as_mut().unwrap().reviewed_executable = false;
    requalify(&mut p);
    deny(p, Reason::Executable);
}
#[test]
fn wp011_protected_credentials_and_cross_run_denied() {
    let (_, mut p) = fixture();
    p.worker.as_mut().unwrap().input_class = InputClass::Protected;
    requalify(&mut p);
    deny(p, Reason::ProtectedInput);
    let (_, mut p) = fixture();
    p.worker.as_mut().unwrap().credential = Credential::Production;
    requalify(&mut p);
    deny(p, Reason::Credential);
    let (s, mut p) = fixture();
    p.worker.as_mut().unwrap().credential = Credential::Test {
        run: opaque("another-run"),
        subject: p.bindings[0].principal.clone(),
        expires_at: p.policy.expires_at.clone(),
        authority: BTreeSet::from([digest(&s).unwrap()]),
    };
    requalify(&mut p);
    deny(p, Reason::Credential);
}
#[test]
fn wp011_test_credential_is_exact_and_short_lived() {
    let (s, mut p) = fixture();
    p.worker.as_mut().unwrap().credential = Credential::Test {
        run: opaque("run:fixture"),
        subject: p.bindings[0].principal.clone(),
        expires_at: "2026-09-21T00:01:00.000000000Z".to_owned().try_into().unwrap(),
        authority: BTreeSet::from([digest(&s).unwrap()]),
    };
    requalify(&mut p);
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    let mut q = Queue::default();
    assert!(Egress::new(&r)
        .deliver_test(
            &caller(&p),
            "2026-09-21T00:02:00.000000000Z".to_owned().try_into().unwrap(),
            &id,
            trusted(&p),
            &mut q
        )
        .is_err());
    assert!(q.calls.is_empty());
}
#[test]
fn wp012_each_unmet_control_names_requirement_and_never_dispatches() {
    for name in ["cpu", "memory", "mount", "network"] {
        for kind in 0..6 {
            let (_, mut p) = fixture();
            let w = p.worker.as_mut().unwrap();
            let k = opaque(name);
            match kind {
                0 => {
                    w.controls.remove(&k);
                }
                1 => w.controls.get_mut(&k).unwrap().enforcement = Enforcement::Unknown,
                2 => w.controls.get_mut(&k).unwrap().enforcement = Enforcement::Unsupported,
                3 => w.controls.get_mut(&k).unwrap().advertised = opaque("weaker"),
                4 => w.controls.get_mut(&k).unwrap().applied = opaque("weaker"),
                _ => w.controls.get_mut(&k).unwrap().observed = opaque("unknown"),
            };
            requalify(&mut p);
            deny(p, Reason::MandatoryControl(k));
        }
    }
}
#[test]
fn wp013_unsupported_operation_is_unavailable() {
    for op in ["suspend", "checkpoint", "resume", "command"] {
        let (_, mut p) = fixture();
        p.worker.as_mut().unwrap().operation = opaque(op);
        requalify(&mut p);
        deny(p, Reason::Unavailable);
    }
}
#[test]
fn wp014_every_snapshot_binding_drift_requires_reevaluation() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    let value = serde_json::to_value(&p).unwrap();
    fn leaves(v: &serde_json::Value, path: String, out: &mut Vec<String>) {
        match v {
            serde_json::Value::Object(m) => {
                for (k, v) in m {
                    leaves(v, format!("{path}/{k}"), out)
                }
            }
            serde_json::Value::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    leaves(v, format!("{path}/{i}"), out)
                }
            }
            serde_json::Value::String(_)
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_) => out.push(path),
            _ => {}
        }
    }
    let mut paths = vec![];
    leaves(&value, String::new(), &mut paths);
    let mut checked = 0;
    for path in paths {
        let mut v = value.clone();
        let leaf = v.pointer_mut(&path).unwrap();
        *leaf = match leaf {
            serde_json::Value::String(s) => {
                let changed = if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
                    if let Some(rest) = s.strip_prefix('0') {
                        format!("1{rest}")
                    } else {
                        format!("0{}", &s[1..])
                    }
                } else if s.len() == 36 {
                    format!("4{}", &s[1..])
                } else if s.ends_with('Z') {
                    "2026-09-23T00:00:00.000000000Z".into()
                } else {
                    format!("{s}-drift")
                };
                changed.into()
            }
            serde_json::Value::Bool(b) => (!*b).into(),
            serde_json::Value::Number(n) => (n.as_u64().unwrap() + 1).into(),
            _ => unreachable!(),
        };
        if let Ok(changed) = serde_json::from_value::<AuthorizationSnapshot>(v) {
            if changed == p {
                continue;
            }
            let mut q = Queue::default();
            assert!(
                Egress::new(&r)
                    .deliver_test(&caller(&p), now(), &id, trusted(&changed), &mut q)
                    .is_err(),
                "drift accepted: {path}"
            );
            assert!(q.calls.is_empty());
            checked += 1;
        }
    }
    assert!(checked > 70, "{checked}");
}
#[test]
fn ordinary_provider_cannot_reuse_fixture_proof() {
    let (_, mut p) = fixture();
    p.fixture = None;
    deny(p, Reason::FixtureOnly);
}
#[test]
fn ordinary_unsupported_operations_are_unavailable_without_substitution() {
    for operation in ["suspend", "checkpoint", "resume", "command"] {
        let (s, mut p) = fixture();
        p.worker.as_mut().unwrap().operation = opaque(operation);
        if let Target::Worker { operation: target, .. } = &mut p.facts[0].document.target {
            *target = opaque(operation);
        }
        p.facts[0].attested_document = digest(&p.facts[0].document).unwrap();
        p.policy.targets = BTreeSet::from([p.facts[0].attested_document.clone()]);
        let w = p.worker.as_ref().unwrap();
        p.qualification = Some(OrdinaryQualification {
            realization: digest(w).unwrap(),
            tenant: p.bindings[0].tenant.clone(),
            subject: p.bindings[0].principal.clone(),
            provider: w.provider.clone(),
            profile: w.profile.clone(),
            operations: BTreeSet::from([opaque("start")]),
            aq_version: w.aq_version.clone(),
            wi_version: w.wi_version.clone(),
            expires_at: p.policy.expires_at.clone(),
            revoked: false,
            quarantined: false,
            evidence: p.fixture.take().unwrap().evidence,
        });
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let id = submit(&r, &s, &p);
        resolve(&r, &id, &p);
        let view = evaluate(&r, &id, &p);
        assert_eq!(view.phase, Phase::Denied);
        assert_eq!(view.authorization.unwrap().reasons, vec![Reason::Unavailable]);
        let mut queue = Queue::default();
        assert!(Egress::new(&r)
            .deliver_test(&caller(&p), now(), &id, trusted(&p), &mut queue)
            .is_err());
        assert!(queue.calls.is_empty());
        let state = r.effect_snapshot(caller(&p).tenant()).unwrap();
        assert!(state.uses.is_empty());
        assert_eq!(state.effects.len(), 1);
        assert!(state.effects[&id].dispatches.is_empty());
        // An unavailable operation must not mask an invalid ordinary qualification.
        p.qualification.as_mut().unwrap().revoked = true;
        let effect = &state.effects[&id];
        let invalid = authorize(
            &effect.intent,
            &effect.agency,
            effect.resolution.as_ref().unwrap(),
            &trusted(&p),
            &now(),
        )
        .unwrap();
        assert_eq!(invalid.reasons, vec![Reason::ProfileDrift]);
    }
}
#[test]
fn ordinary_qualification_is_exact_and_revocable() {
    let (s, mut p) = fixture();
    p.fixture = None;
    let w = p.worker.as_mut().unwrap();
    w.credential = Credential::None;
    let q = OrdinaryQualification {
        realization: digest(w).unwrap(),
        tenant: opaque("tenant:fixture"),
        subject: p.bindings[0].principal.clone(),
        provider: w.provider.clone(),
        profile: w.profile.clone(),
        operations: BTreeSet::from([w.operation.clone()]),
        aq_version: w.aq_version.clone(),
        wi_version: w.wi_version.clone(),
        expires_at: "2026-09-22T00:00:00.000000000Z".to_owned().try_into().unwrap(),
        revoked: false,
        quarantined: false,
        evidence: p.facts[0].evidence.clone(),
    };
    p.qualification = Some(q.clone());
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    resolve(&r, &id, &p);
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::Authorized);
    for (credential, input_class, expected) in [
        (Credential::Production, InputClass::NonSensitive, Reason::Credential),
        (Credential::None, InputClass::Protected, Reason::ProtectedInput),
    ] {
        let mut excluded = p.clone();
        let worker = excluded.worker.as_mut().unwrap();
        worker.credential = credential;
        worker.input_class = input_class;
        excluded.qualification.as_mut().unwrap().realization = digest(worker).unwrap();
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let id = submit(&r, &s, &excluded);
        resolve(&r, &id, &excluded);
        let view = evaluate(&r, &id, &excluded);
        assert_eq!(view.phase, Phase::Denied);
        assert!(view.authorization.unwrap().reasons.contains(&expected));
    }
    p.qualification.as_mut().unwrap().quarantined = true;
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    resolve(&r, &id, &p);
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::Denied);
    assert!(Egress::new(&r)
        .inspect(&caller(&p), &id)
        .unwrap()
        .authorization
        .unwrap()
        .reasons
        .contains(&Reason::ProfileDrift));
}
#[test]
fn agency_forgery_ambiguity_expiry_revocation_and_attenuation() {
    let (s, p) = fixture();
    let c = caller(&p);
    for mode in 0..7 {
        let mut s = s.clone();
        let mut b = p.bindings.clone();
        match mode {
            0 => s.submitted_by = Some(ActorId::generate()),
            1 => s.requested_accountable_principal = Some(PrincipalId::generate()),
            2 => {
                let mut cap = b[0].capacity.clone();
                cap.assignment_ref = opaque("forged");
                s.acting_capacity = Some(cap)
            }
            3 => b.push(b[0].clone()),
            4 => b[0].revoked = true,
            5 => b[0].expires_at = now(),
            _ => {
                b[0].capacity_authority.insert(ContentHash::of(b"widen"));
            }
        }
        assert!(resolve_agency(&c, &s, &b, &now()).is_err());
    }
}
#[test]
fn delegation_endpoints_cycles_and_widening_fail_closed() {
    let (s, p) = fixture();
    let mut b = p.bindings[0].clone();
    let root = PrincipalId::generate();
    b.accountable = root.clone();
    b.capacity.principal_ref = root.clone();
    let d = Delegation {
        id: opaque("delegation"),
        version: 1,
        issuer: root,
        subject: b.principal.clone(),
        tenant: b.tenant.clone(),
        authority: b.capacity_authority.clone(),
        expires_at: b.expires_at.clone(),
        revoked: false,
        evidence: p.grants[0].evidence.clone(),
    };
    b.delegations = vec![d.clone()];
    assert!(resolve_agency(&caller(&p), &s, &[b.clone()], &now()).is_ok());
    for mode in 0..6 {
        let mut b = b.clone();
        match mode {
            0 => b.delegations[0].subject = PrincipalId::generate(),
            1 => b.delegations[0].subject = b.accountable.clone(),
            2 => {
                b.delegations[0].authority.insert(ContentHash::of(b"wider"));
            }
            3 => b.delegations[0].revoked = true,
            4 => b.delegations[0].expires_at = now(),
            _ => b.delegations.push(d.clone()),
        };
        assert!(resolve_agency(&caller(&p), &s, &[b], &now()).is_err());
    }
}
#[test]
fn resolution_failures_are_durable_without_running() {
    for mode in 0..6 {
        let (s, mut p) = fixture();
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let id = submit(&r, &s, &p);
        match mode {
            0 => p.facts.push(p.facts[0].clone()),
            1 => p.registry.entries.clear(),
            2 => p.facts[0].document.tenant = opaque("foreign"),
            3 => p.facts[0].attested_document = ContentHash::of(b"wrong"),
            4 => p.facts[0].valid_until = now(),
            _ => p.facts[0].registry = ContentHash::of(b"old"),
        };
        resolve(&r, &id, &p);
        let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
        assert_eq!(v.phase, Phase::Failed);
        assert_eq!(v.commit, CommitObservation::ProvenImpossible);
        assert!(v.resolution_failure.is_some());
        assert!(v.dispatch.is_none());
    }
}
#[test]
fn canonical_resource_grammar_and_namespace_specific_bounds() {
    let (_, p) = fixture();
    let mut d = p.facts[0].document.clone();
    let r = d.reference(opaque("v1")).unwrap();
    assert_eq!(r.resource.as_str(), format!("wi-resource:sha256:{}", digest(&d).unwrap().as_str()));
    d.target = Target::Filesystem {
        root: opaque("root"),
        components: vec!["..".into()],
        symlink_chain: ContentHash::of(b"link"),
    };
    assert!(d.reference(opaque("v1")).is_err());
    d.target = Target::Network {
        scheme: opaque("https"),
        host: opaque("example.com"),
        port: 443,
        addresses: BTreeSet::new(),
        redirects: vec![],
        path: "/".into(),
    };
    assert!(d.reference(opaque("v1")).is_err());
}
#[test]
fn identical_request_recovery_conflicts_and_scoped_inspection() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    assert_eq!(id, submit(&r, &s, &p));
    let old = r.effect_snapshot(caller(&p).tenant()).unwrap();
    let mut changed = s.clone();
    changed.budget.max_attempts += 1;
    assert!(Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(changed),
                snapshot: trusted(&p),
                link: None
            }
        )
        .is_err());
    assert_eq!(old, r.effect_snapshot(caller(&p).tenant()).unwrap());
    for c in [
        AuthenticatedCaller::from_trusted_authentication(
            opaque("another-tenant"),
            caller(&p).principal().clone(),
        ),
        AuthenticatedCaller::from_trusted_authentication(
            caller(&p).tenant().clone(),
            PrincipalId::generate(),
        ),
    ] {
        assert!(Egress::new(&r).inspect(&c, &id).is_err());
    }
}
#[test]
fn lost_ack_reopens_identical_request_without_extra_use() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    let mut q = Queue { lost: true, ..Default::default() };
    assert!(!Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap());
    let uses = r.effect_snapshot(caller(&p).tenant()).unwrap().uses;
    drop(r);
    let r = StoreRoot::open_existing(t.path().join("root")).unwrap();
    assert!(Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap());
    assert_eq!(q.calls[0], q.calls[1]);
    assert_eq!(uses, r.effect_snapshot(caller(&p).tenant()).unwrap().uses);
    assert!(Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap());
    assert_eq!(q.calls.len(), 2);
}
#[test]
fn forged_acknowledgments_cannot_complete_outbox() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    let mut q = Queue { lost: true, ..Default::default() };
    Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap();
    let req = &q.calls[0];
    for mode in 0..4 {
        let mut ack = EffectAcknowledgment {
            task: req.task.clone(),
            admission_key: req.admission_key.clone(),
            request_digest: digest(req).unwrap(),
            durable_sequence: 1,
        };
        match mode {
            0 => ack.task = opaque("wrong"),
            1 => ack.admission_key = opaque("wrong"),
            2 => ack.request_digest = ContentHash::of(b"wrong"),
            _ => ack.durable_sequence = 0,
        };
        assert!(Egress::new(&r)
            .apply(
                &caller(&p),
                now(),
                EffectAction::Acknowledge { id: id.clone(), acknowledgment: ack }
            )
            .is_err());
    }
    assert!(Egress::new(&r)
        .inspect(&caller(&p), &id)
        .unwrap()
        .dispatch
        .unwrap()
        .acknowledgment
        .is_none());
}
#[test]
fn concurrent_submissions_and_logical_use_races() {
    let (mut s, mut p) = fixture();
    p.grants[0].max_uses = 1;
    let mut second = s.clone();
    second.client_request_id = "second".to_owned().try_into().unwrap();
    allow_submission(&mut p, &second);
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let ids: Vec<_> = std::thread::scope(|scope| {
        let mut threads = vec![];
        for _ in 0..8 {
            let r = &r;
            let s = &s;
            let p = &p;
            threads.push(scope.spawn(move || submit(r, s, p)));
        }
        threads.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(ids.iter().all(|i| i == &ids[0]));
    let id1 = ids[0].clone();
    s = second;
    let id2 = submit(&r, &s, &p);
    for id in [&id1, &id2] {
        resolve(&r, id, &p);
        assert_eq!(evaluate(&r, id, &p).phase, Phase::Authorized);
    }
    let results: Vec<_> = std::thread::scope(|scope| {
        [id1, id2]
            .into_iter()
            .map(|id| {
                let r = &r;
                let p = &p;
                scope.spawn(move || {
                    Egress::new(r)
                        .apply(
                            &caller(p),
                            now(),
                            EffectAction::Prepare { id, snapshot: trusted(p) },
                        )
                        .is_ok()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert_eq!(results.into_iter().filter(|b| *b).count(), 1);
}
fn start(
    r: &StoreRoot,
    s: &EffectSubmission,
    p: &AuthorizationSnapshot,
) -> (EffectIntentId, OutcomeEvidence) {
    let id = ready(r, s, p);
    let mut q = Queue::default();
    Egress::new(r).deliver_test(&caller(p), now(), &id, trusted(p), &mut q).unwrap();
    let attempt = opaque("attempt:1");
    Egress::new(r)
        .apply(
            &caller(p),
            now(),
            EffectAction::Start { id: id.clone(), snapshot: trusted(p), attempt: attempt.clone() },
        )
        .unwrap();
    let o = OutcomeEvidence {
        classification: None,
        reconciliation_admission: None,
        intent: id.clone(),
        attempt,
        connector_semantics: p.registry.entries[0].semantics.clone(),
        commit: CommitObservation::Unknown,
        outcome: ExecutionOutcome::Uncertain,
        evidence: vec![p.grants[0].evidence.clone()],
        required_outputs: p.required_outputs.clone(),
        durable_outputs: vec![],
        boundary_record: None,
    };
    (id, o)
}
fn outcome(
    r: &StoreRoot,
    p: &AuthorizationSnapshot,
    id: &EffectIntentId,
    o: OutcomeEvidence,
) -> worldinterface_storage::Result<EffectIntentId> {
    if !o.durable_outputs.is_empty() {
        r.publish(opaque("tenant:fixture"), b"output").unwrap();
    }
    let state = r.effect_snapshot(caller(p).tenant())?;
    let execution = ExecutionRecord {
        classification: None,
        connector_execution: None,
        intent_id: id.clone(),
        attempt: o.attempt.clone(),
        outcome: o.outcome.clone(),
        evidence: o.evidence.clone(),
        recorded_at: now(),
    };
    let command = validate_command(
        &state,
        &caller(p),
        now(),
        EffectAction::Outcome {
            id: id.clone(),
            evidence: VerifiedOutcome::from_trusted_verifier(o),
        },
    )?;
    r.effect_transact_with_receipt(
        &command,
        ReceiptId::generate(),
        &execution,
        receipt_temporal(),
    )?;
    Ok(id.clone())
}
fn reconciliation(
    p: &AuthorizationSnapshot,
    o: &OutcomeEvidence,
    result: ReconciliationResult,
) -> ReconciliationEvidence {
    ReconciliationEvidence {
        intent: o.intent.clone(),
        attempt: o.attempt.clone(),
        connector_semantics: o.connector_semantics.clone(),
        result,
        evidence: vec![p.grants[0].evidence.clone()],
    }
}
#[test]
fn commit_evidence_rejects_false_success_failure_and_missing_publication() {
    for mode in 0..8 {
        let (s, p) = fixture();
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let (id, mut o) = start(&r, &s, &p);
        match mode {
            0 => o.evidence.clear(),
            1 => o.intent = EffectIntentId::generate(),
            2 => o.attempt = opaque("another-attempt"),
            3 => o.connector_semantics = opaque("another-semantics"),
            4 => o.outcome = ExecutionOutcome::FailedBeforeCommit,
            5 => {
                o.commit = CommitObservation::ConfirmedCommitted;
                o.outcome = ExecutionOutcome::Succeeded;
                o.boundary_record = Some(p.grants[0].evidence.clone())
            }
            6 => {
                o.commit = CommitObservation::ConfirmedCommitted;
                o.outcome = ExecutionOutcome::Succeeded;
                o.durable_outputs = o.required_outputs.clone()
            }
            _ => {
                o.commit = CommitObservation::ConfirmedCommitted;
                o.outcome = ExecutionOutcome::Succeeded;
                o.required_outputs.clear();
                o.durable_outputs.clear();
                o.boundary_record = Some(p.grants[0].evidence.clone())
            }
        }
        assert!(outcome(&r, &p, &id, o).is_err());
        assert_eq!(Egress::new(&r).inspect(&caller(&p), &id).unwrap().phase, Phase::Running);
    }
}
#[test]
fn successful_boundary_requires_durable_output_evidence() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, mut o) = start(&r, &s, &p);
    o.commit = CommitObservation::ConfirmedCommitted;
    o.outcome = ExecutionOutcome::Succeeded;
    o.durable_outputs = o.required_outputs.clone();
    o.boundary_record = Some(p.grants[0].evidence.clone());
    outcome(&r, &p, &id, o.clone()).unwrap();
    let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
    assert_eq!(v.phase, Phase::Succeeded);
    assert_eq!(v.outcome, Some(o));
}
#[test]
fn receipt_outcome_and_outbox_commit_after_scoped_publication() {
    let (mut s, mut p) = fixture();
    s.causal_refs.push(opaque("campaign:permitted"));
    allow_submission(&mut p, &s);
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, mut o) = start(&r, &s, &p);
    o.commit = CommitObservation::ConfirmedCommitted;
    o.outcome = ExecutionOutcome::Succeeded;
    o.durable_outputs = o.required_outputs.clone();
    o.boundary_record = Some(p.grants[0].evidence.clone());
    let make_command = || {
        let state = r.effect_snapshot(caller(&p).tenant()).unwrap();
        validate_command(
            &state,
            &caller(&p),
            now(),
            EffectAction::Outcome {
                id: id.clone(),
                evidence: VerifiedOutcome::from_trusted_verifier(o.clone()),
            },
        )
        .unwrap()
    };
    let execution = ExecutionRecord {
        classification: None,
        connector_execution: None,
        intent_id: id.clone(),
        attempt: o.attempt.clone(),
        outcome: o.outcome.clone(),
        evidence: o.evidence.clone(),
        recorded_at: now(),
    };
    let time = TimeEvidence {
        at: now(),
        source: opaque("fixture-clock"),
        assurance: opaque("fixture"),
        uncertainty_nanos: 0,
    };
    let temporal = TemporalProvenance {
        occurred_at: None,
        observed_at: None,
        received_at: time.clone(),
        admitted_at: time,
        dispatched_at: None,
        committed_at: None,
        reconciled_at: None,
        sequence: None,
    };
    let receipt_id = ReceiptId::generate();
    let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        caller(&p).tenant().clone(),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Full,
    );
    let prior = r.effect_snapshot(caller(&p).tenant()).unwrap();
    assert!(r
        .effect_transact_with_receipt(
            &make_command(),
            receipt_id.clone(),
            &execution,
            temporal.clone()
        )
        .is_err());
    assert_eq!(prior, r.effect_snapshot(caller(&p).tenant()).unwrap());
    assert!(r.inspect_receipt(&access, &receipt_id).unwrap().is_none());
    r.stage_publication(opaque("output:publication"), opaque("tenant:fixture"), b"output").unwrap();
    let output = r.publish_staged(&opaque("output:publication"), b"output").unwrap();
    assert_eq!(output, o.required_outputs[0]);
    let digest = r
        .effect_transact_with_receipt(&make_command(), receipt_id.clone(), &execution, temporal)
        .unwrap();
    assert_eq!(r.inspect_receipt(&access, &receipt_id).unwrap().unwrap().digest.unwrap(), digest);
    let summary = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        caller(&p).tenant().clone(),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Summary,
    );
    let view = r.inspect_receipt(&summary, &receipt_id).unwrap().unwrap();
    assert_eq!(view.digest, Some(digest.clone()));
    assert!(view.canonical.is_none() && view.evidence_hashes.is_empty());
    assert_eq!(view.intent_id, Some(id.clone()));
    assert_eq!(view.causal_refs, vec![opaque("campaign:permitted")]);
    let redacted = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        caller(&p).tenant().clone(),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Redacted,
    );
    let redacted_view = r.inspect_receipt(&redacted, &receipt_id).unwrap().unwrap();
    assert_eq!(redacted_view.digest, Some(digest));
    assert!(redacted_view.canonical.is_none());
    assert_eq!(redacted_view.causal_refs, vec![opaque("campaign:permitted")]);
    assert!(!redacted_view.evidence_hashes.is_empty());
    assert!(redacted_view
        .evidence_hashes
        .iter()
        .all(|hash| hash == &o.evidence[0].artifact.content));
    let foreign = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        opaque("foreign-tenant"),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Full,
    );
    assert!(r.inspect_receipt(&foreign, &receipt_id).unwrap().is_none());
    let foreign_redacted = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        opaque("foreign-tenant"),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Redacted,
    );
    assert!(r.inspect_receipt(&foreign_redacted, &receipt_id).unwrap().is_none());
    assert_eq!(Egress::new(&r).inspect(&caller(&p), &id).unwrap().phase, Phase::Succeeded);
    drop(r);
    let reopened = StoreRoot::open_existing(t.path().join("root")).unwrap();
    assert!(reopened.inspect_receipt(&access, &receipt_id).unwrap().is_some());
    let mut forged =
        reopened.inspect_receipt(&access, &receipt_id).unwrap().unwrap().canonical.unwrap();
    let Record::BoundaryReceipt(ref mut receipt) = forged.record else { unreachable!() };
    receipt.id = ReceiptId::generate();
    let Record::BoundaryReceipt(ref mut receipt) = forged.record else { unreachable!() };
    let ReceiptBody::Egress(ref mut body) = receipt.body else { unreachable!() };
    body.agency_digest = ContentHash::of(b"forged-agency");
    let forged_id = receipt.id.clone();
    let bytes = forged.canonical_bytes().unwrap();
    let forged_digest = forged.digest().unwrap();
    let revision = reopened.effect_snapshot(caller(&p).tenant()).unwrap().revision;
    drop(reopened);
    let c = rusqlite::Connection::open(t.path().join("root/fabric.sqlite")).unwrap();
    c.execute("INSERT INTO receipt_history(id,tenant,intent,source_revision,canonical,digest) VALUES(?,?,?,?,?,?)",
        rusqlite::params![forged_id.as_str(), caller(&p).tenant().as_str(), id.as_str(), revision, bytes, forged_digest.as_str()]).unwrap();
    c.execute(
        "INSERT INTO receipt_index VALUES(?,?,?,?)",
        rusqlite::params![
            forged_id.as_str(),
            forged_digest.as_str(),
            caller(&p).tenant().as_str(),
            id.as_str()
        ],
    )
    .unwrap();
    drop(c);
    assert!(StoreRoot::open_existing(t.path().join("root")).is_err());
}

fn receipt_temporal() -> TemporalProvenance {
    let time = TimeEvidence {
        at: now(),
        source: opaque("fixture-clock"),
        assurance: opaque("fixture"),
        uncertainty_nanos: 0,
    };
    TemporalProvenance {
        occurred_at: None,
        observed_at: None,
        received_at: time.clone(),
        admitted_at: time,
        dispatched_at: None,
        committed_at: None,
        reconciled_at: None,
        sequence: None,
    }
}

fn receipt_command(
    r: &StoreRoot,
    p: &AuthorizationSnapshot,
    o: &OutcomeEvidence,
) -> ValidatedEffectCommand {
    validate_command(
        &r.effect_snapshot(caller(p).tenant()).unwrap(),
        &caller(p),
        now(),
        EffectAction::Outcome {
            id: o.intent.clone(),
            evidence: VerifiedOutcome::from_trusted_verifier(o.clone()),
        },
    )
    .unwrap()
}

fn execution_for(o: &OutcomeEvidence, evidence: Vec<EvidenceRef>) -> ExecutionRecord {
    ExecutionRecord {
        classification: None,
        connector_execution: None,
        intent_id: o.intent.clone(),
        attempt: o.attempt.clone(),
        outcome: o.outcome.clone(),
        evidence,
        recorded_at: now(),
    }
}

#[test]
fn historical_receipt_survives_reconciliation_retry_rebuild_and_restore() {
    let (s, mut p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, o) = start(&r, &s, &p);
    let receipt_id = ReceiptId::generate();
    let original = r
        .effect_transact_with_receipt(
            &receipt_command(&r, &p, &o),
            receipt_id.clone(),
            &execution_for(&o, o.evidence.clone()),
            receipt_temporal(),
        )
        .unwrap();
    Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Reconcile {
                id: id.clone(),
                evidence: VerifiedReconciliation::from_trusted_verifier(reconciliation(
                    &p,
                    &o,
                    ReconciliationResult::ConfirmedNotCommitted,
                )),
            },
        )
        .unwrap();
    p.policy.retry_after_noncommit = true;
    p.policy.revision = opaque("policy:next");
    Egress::new(&r)
        .apply(&caller(&p), now(), EffectAction::PermitRetry { id, snapshot: trusted(&p) })
        .unwrap();
    let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        caller(&p).tenant().clone(),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Full,
    );
    drop(r);
    let path = t.path().join("root");
    let reopened = StoreRoot::open_existing(&path).unwrap();
    assert_eq!(
        reopened.inspect_receipt(&access, &receipt_id).unwrap().unwrap().digest,
        Some(original.clone())
    );
    reopened.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(
        reopened.inspect_receipt(&access, &receipt_id).unwrap().unwrap().digest,
        Some(original.clone())
    );
    drop(reopened);
    StoreRoot::backup_offline(&path, t.path().join("backup")).unwrap();
    StoreRoot::restore_offline(t.path().join("backup"), t.path().join("restored")).unwrap();
    let restored = StoreRoot::open_existing(t.path().join("restored")).unwrap();
    assert_eq!(
        restored.inspect_receipt(&access, &receipt_id).unwrap().unwrap().digest,
        Some(original)
    );
}

#[test]
fn exact_receipt_retry_and_conflicting_bindings() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (_, o) = start(&r, &s, &p);
    let cmd = receipt_command(&r, &p, &o);
    let execution = execution_for(&o, o.evidence.clone());
    let temporal = receipt_temporal();
    let id = ReceiptId::generate();
    let original =
        r.effect_transact_with_receipt(&cmd, id.clone(), &execution, temporal.clone()).unwrap();
    drop(r);
    let r = StoreRoot::open_existing(t.path().join("root")).unwrap();
    assert_eq!(
        r.effect_transact_with_receipt(&cmd, id.clone(), &execution, temporal.clone()).unwrap(),
        original
    );
    let mut changed = execution.clone();
    changed.evidence.clear();
    assert!(r.effect_transact_with_receipt(&cmd, id.clone(), &changed, temporal.clone()).is_err());
    let mut changed_time = temporal;
    changed_time.sequence = Some(opaque("different"));
    assert!(r.effect_transact_with_receipt(&cmd, id.clone(), &execution, changed_time).is_err());
    assert!(r
        .effect_transact_with_receipt(&cmd, ReceiptId::generate(), &execution, receipt_temporal())
        .is_err());
}

#[test]
fn receipt_transaction_rejects_a_command_without_a_new_outcome() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, o) = start(&r, &s, &p);
    let execution = execution_for(&o, o.evidence.clone());
    r.effect_transact_with_receipt(
        &receipt_command(&r, &p, &o),
        ReceiptId::generate(),
        &execution,
        receipt_temporal(),
    )
    .unwrap();
    let before = r.effect_snapshot(caller(&p).tenant()).unwrap();
    let receipts = r.effect_receipts(caller(&p).tenant(), &id).unwrap();
    let command = validate_command(
        &before,
        &caller(&p),
        now(),
        EffectAction::Reconcile {
            id: id.clone(),
            evidence: VerifiedReconciliation::from_trusted_verifier(reconciliation(
                &p,
                &o,
                ReconciliationResult::ConfirmedNotCommitted,
            )),
        },
    )
    .unwrap();
    let error = r
        .effect_transact_with_receipt(
            &command,
            ReceiptId::generate(),
            &execution,
            receipt_temporal(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("receipt requires exactly one new outcome"));
    assert_eq!(r.effect_snapshot(caller(&p).tenant()).unwrap(), before);
    assert_eq!(r.effect_receipts(caller(&p).tenant(), &id).unwrap(), receipts);
    // The rejected receipt write did not consume the valid reconciliation.
    r.effect_transact(&command).unwrap();
    drop(r);
    let r = StoreRoot::open_existing(t.path().join("root")).unwrap();
    assert_eq!(r.effect_receipts(caller(&p).tenant(), &id).unwrap(), receipts);
    assert_eq!(r.effect_snapshot(caller(&p).tenant()).unwrap().revision, before.revision + 1);
}

#[test]
fn receipt_execution_evidence_is_bounded_by_authoritative_outcome() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (_, o) = start(&r, &s, &p);
    let cmd = receipt_command(&r, &p, &o);
    let mut foreign = o.evidence[0].clone();
    foreign.artifact.scope = opaque("another-tenant");
    let mut invented = o.evidence[0].clone();
    invented.artifact.content = ContentHash::of(b"invented");
    for extra in [foreign, invented] {
        let result = r.effect_transact_with_receipt(
            &cmd,
            ReceiptId::generate(),
            &execution_for(&o, vec![extra]),
            receipt_temporal(),
        );
        assert!(result.is_err());
    }
    let id = ReceiptId::generate();
    r.effect_transact_with_receipt(
        &cmd,
        id.clone(),
        &execution_for(&o, vec![]),
        receipt_temporal(),
    )
    .unwrap();
    let access = worldinterface_domain::receipt::ReceiptAccess::from_trusted_authorizer(
        caller(&p).tenant().clone(),
        caller(&p).principal().clone(),
        worldinterface_domain::receipt::DisclosureProfile::Full,
    );
    let view = r.inspect_receipt(&access, &id).unwrap().unwrap();
    let Record::BoundaryReceipt(receipt) = view.canonical.unwrap().record else { panic!() };
    assert_eq!(receipt.evidence, o.evidence);
    drop(r);
    StoreRoot::open_existing(t.path().join("root")).unwrap();
}

#[test]
fn developmental_comparison_exposes_outcome_and_unknown() {
    use worldinterface_domain::developmental::{compare_arms, inspect, Comparison};
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, _) = start(&r, &s, &p);
    let state = r.effect_snapshot(caller(&p).tenant()).unwrap();
    let mut a = inspect(&state.effects[&id], None).unwrap();
    a.outcome = Some(ExecutionOutcome::Succeeded);
    a.uncertainty = false;
    for outcome in [
        ExecutionOutcome::FailedBeforeCommit,
        ExecutionOutcome::CanceledBeforeCommit,
        ExecutionOutcome::Uncertain,
    ] {
        let mut b = a.clone();
        b.outcome = Some(outcome);
        let compared = compare_arms(&a, &b).unwrap();
        assert!(compared
            .fields
            .iter()
            .any(|(field, state)| field.as_str() == "outcome" && *state == Comparison::Difference));
        assert!(compared.confounds.iter().any(|field| field.as_str() == "outcome"));
    }
    let mut b = a.clone();
    b.outcome = None;
    let compared = compare_arms(&a, &b).unwrap();
    assert!(compared
        .fields
        .iter()
        .any(|(field, state)| field.as_str() == "outcome" && *state == Comparison::Unknown));
    let mut b = a.clone();
    b.commit = CommitObservation::ConfirmedCommitted;
    b.reconciliation_history = vec![ReconciliationResult::ConfirmedCommitted];
    let compared = compare_arms(&a, &b).unwrap();
    for field in ["commit", "reconciliation_history"] {
        assert!(compared
            .fields
            .iter()
            .any(|(name, state)| name.as_str() == field && *state == Comparison::Difference));
        assert!(compared.confounds.iter().any(|name| name.as_str() == field));
    }
}
#[test]
fn developmental_comparison_retains_resolution_and_provider_versions() {
    use worldinterface_domain::developmental::{compare_arms, inspect, Comparison};
    let (s, p) = fixture();
    let mut changed = p.clone();
    changed.registry.entries[0].version = opaque("resolver:changed");
    changed.registry.entries[0].descriptor = ContentHash::of(b"descriptor:changed");
    changed.facts[0].registry = digest(&changed.registry).unwrap();
    changed.worker.as_mut().unwrap().aq_version = ContentHash::of(b"aq:changed");
    requalify(&mut changed);
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = root(&left_dir);
    let right = root(&right_dir);
    let (left_id, _) = start(&left, &s, &p);
    let (right_id, _) = start(&right, &s, &changed);
    let left_state = left.effect_snapshot(caller(&p).tenant()).unwrap();
    let right_state = right.effect_snapshot(caller(&changed).tenant()).unwrap();
    let a = inspect(&left_state.effects[&left_id], None).unwrap();
    let mut b = inspect(&right_state.effects[&right_id], None).unwrap();
    let comparison = compare_arms(&a, &b).unwrap();
    for field in ["resolver", "registry", "descriptor", "provider_profile", "aq_revision"] {
        assert!(
            comparison
                .fields
                .iter()
                .any(|(name, state)| name.as_str() == field && *state == Comparison::Difference),
            "{field}: {comparison:?}"
        );
    }
    for field in ["connector", "environment"] {
        assert!(
            comparison
                .fields
                .iter()
                .any(|(name, state)| name.as_str() == field && *state == Comparison::Match),
            "{field}: {comparison:?}"
        );
    }
    b.provider_profile = None;
    let comparison = compare_arms(&a, &b).unwrap();
    assert!(comparison
        .fields
        .iter()
        .any(|(name, state)| name.as_str() == "provider_profile" && *state == Comparison::Unknown));
}
#[test]
fn uncertainty_reconciliation_and_retry_require_noncommit_policy_and_authority() {
    for result in [
        ReconciliationResult::StillUncertain,
        ReconciliationResult::Escalated,
        ReconciliationResult::ConfirmedCommitted,
        ReconciliationResult::ConfirmedNotCommitted,
    ] {
        let (s, mut p) = fixture();
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let (id, o) = start(&r, &s, &p);
        outcome(&r, &p, &id, o.clone()).unwrap();
        let rec = reconciliation(&p, &o, result.clone());
        Egress::new(&r)
            .apply(
                &caller(&p),
                now(),
                EffectAction::Reconcile {
                    id: id.clone(),
                    evidence: VerifiedReconciliation::from_trusted_verifier(rec.clone()),
                },
            )
            .unwrap();
        let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
        assert_ne!(v.phase, Phase::Succeeded);
        assert_eq!(v.reconciliation, Some(rec));
        assert!(Egress::new(&r)
            .apply(
                &caller(&p),
                now(),
                EffectAction::PermitRetry { id: id.clone(), snapshot: trusted(&p) }
            )
            .is_err());
        p.policy.retry_after_noncommit = true;
        let retry = Egress::new(&r).apply(
            &caller(&p),
            now(),
            EffectAction::PermitRetry { id: id.clone(), snapshot: trusted(&p) },
        );
        assert_eq!(retry.is_ok(), result == ReconciliationResult::ConfirmedNotCommitted);
        if retry.is_ok() {
            let mut q = Queue::default();
            Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap();
            assert_eq!(q.calls[0].operation_index, 1);
        }
    }
}
#[test]
fn reconciliation_cannot_change_confirmed_commit_or_skip_evidence() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, mut o) = start(&r, &s, &p);
    o.commit = CommitObservation::ConfirmedCommitted;
    outcome(&r, &p, &id, o.clone()).unwrap();
    let rec = reconciliation(&p, &o, ReconciliationResult::ConfirmedNotCommitted);
    assert!(Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Reconcile {
                id: id.clone(),
                evidence: VerifiedReconciliation::from_trusted_verifier(rec)
            }
        )
        .is_err());
    let mut rec = reconciliation(&p, &o, ReconciliationResult::ConfirmedCommitted);
    rec.evidence.clear();
    assert!(Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Reconcile {
                id,
                evidence: VerifiedReconciliation::from_trusted_verifier(rec)
            }
        )
        .is_err());
}
#[test]
fn cancellation_before_dispatch_and_after_handoff_are_distinct() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    Egress::new(&r).apply(&caller(&p), now(), EffectAction::Cancel { id: id.clone() }).unwrap();
    let v = Egress::new(&r).inspect(&caller(&p), &id).unwrap();
    assert_eq!(v.phase, Phase::Canceled);
    assert_eq!(v.commit, CommitObservation::NotAttempted);
    let mut s = s;
    s.client_request_id = "other".to_owned().try_into().unwrap();
    let mut p = p;
    allow_submission(&mut p, &s);
    let (id, o) = start(&r, &s, &p);
    assert!(Egress::new(&r)
        .apply(&caller(&p), now(), EffectAction::Cancel { id: id.clone() })
        .is_err());
    outcome(&r, &p, &id, o).unwrap();
    assert_eq!(Egress::new(&r).inspect(&caller(&p), &id).unwrap().phase, Phase::Uncertain);
}
#[test]
fn compensation_is_a_separate_intent_without_inherited_authority() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, o) = start(&r, &s, &p);
    outcome(&r, &p, &id, o).unwrap();
    let mut compensation = s;
    compensation.client_request_id = "compensation".to_owned().try_into().unwrap();
    let second = Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(compensation),
                snapshot: trusted(&p),
                link: Some(IntentLink { original: id.clone(), kind: LinkKind::Compensation }),
            },
        )
        .unwrap();
    assert_ne!(id, second);
    resolve(&r, &second, &p);
    assert_eq!(evaluate(&r, &second, &p).phase, Phase::Denied);
    assert_eq!(Egress::new(&r).inspect(&caller(&p), &second).unwrap().link.unwrap().original, id);
}
#[test]
fn approval_and_separation_of_duties_are_bound_to_exact_inputs() {
    let (s, mut p) = fixture();
    p.policy.approval_required = true;
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = submit(&r, &s, &p);
    resolve(&r, &id, &p);
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::AwaitingApproval);
    let e = &r.effect_snapshot(caller(&p).tenant()).unwrap().effects[&id];
    let reviewer = PrincipalId::generate();
    p.policy.approvers.insert(reviewer.clone());
    let a = Approval {
        id: opaque("approval"),
        version: 1,
        approver: reviewer,
        exact_binding: approval_binding(&e.intent, &e.agency, e.resolution.as_ref().unwrap(), &p)
            .unwrap(),
        expires_at: p.policy.expires_at.clone(),
        revoked: false,
        max_uses: 1,
        evidence: p.grants[0].evidence.clone(),
    };
    p.approvals = vec![a];
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::Authorized);
    let mut changed = p.clone();
    changed.approvals[0].approver = p.bindings[0].principal.clone();
    changed.policy.approvers.insert(changed.approvals[0].approver.clone());
    let e = &r.effect_snapshot(caller(&p).tenant()).unwrap().effects[&id];
    changed.approvals[0].exact_binding =
        approval_binding(&e.intent, &e.agency, e.resolution.as_ref().unwrap(), &changed).unwrap();
    assert_eq!(evaluate(&r, &id, &changed).phase, Phase::Denied);
}
#[test]
fn backup_rebuild_replay_and_generic_bypass_integrity() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    let mut q = Queue::default();
    Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap();
    let original = r.effect_snapshot(caller(&p).tenant()).unwrap();
    let entity: EntityId = id.as_str().to_owned().try_into().unwrap();
    let mut batch = Batch::new(entity.clone(), worldinterface_domain::Machine::Effect, 0);
    batch.events.push(worldinterface_domain::Event {
        entity,
        machine: worldinterface_domain::Machine::Effect,
        action: worldinterface_domain::Action::Submit,
        at: now(),
    });
    assert!(r.repository(StoreKind::Fabric).unwrap().append(&batch).is_err());
    r.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(original, r.effect_snapshot(caller(&p).tenant()).unwrap());
    drop(r);
    StoreRoot::backup_offline(t.path().join("root"), t.path().join("backup")).unwrap();
    let restored = StoreRoot::open_existing(t.path().join("backup")).unwrap();
    assert_eq!(original, restored.effect_snapshot(caller(&p).tenant()).unwrap());
    drop(restored);
    let c = rusqlite::Connection::open(t.path().join("backup/fabric.sqlite")).unwrap();
    c.execute("UPDATE effect_state SET state=?", [b"{}".as_slice()]).unwrap();
    drop(c);
    assert!(StoreRoot::open_existing(t.path().join("backup")).is_err());
}
#[test]
fn unsupported_store_profile_is_rejected_without_upgrade() {
    let t = tempfile::tempdir().unwrap();
    drop(root(&t));
    let path = t.path().join("root/manifest.json");
    let bytes = std::fs::read(&path).unwrap();
    let s = String::from_utf8(bytes).unwrap().replace("local-receipt-3", "local-ingress-1");
    std::fs::write(&path, &s).unwrap();
    fn files(path: &std::path::Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
        let mut snapshot = std::collections::BTreeMap::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap().path();
            if entry.is_dir() {
                snapshot.extend(files(&entry));
            } else {
                snapshot.insert(entry.clone(), std::fs::read(&entry).unwrap());
            }
        }
        snapshot
    }
    let before = files(&t.path().join("root"));
    assert!(StoreRoot::open_existing(t.path().join("root")).is_err());
    assert_eq!(files(&t.path().join("root")), before);
    assert_eq!(std::fs::read_to_string(path).unwrap(), s);
}
#[test]
fn removing_authority_never_increases_allowed_demands() {
    TestRunner::default()
        .run(&(0u8..32), |mask| {
            let (s, mut p) = fixture();
            let c = caller(&p);
            let a = resolve_agency(&c, &s, &p.bindings, &now()).unwrap();
            let intent = EffectIntent {
                id: EffectIntentId::generate(),
                submission: s.clone(),
                agency: a.context.clone(),
                stable_idempotency_identity: opaque("test"),
                submitted_at: now(),
            };
            let r = resolve_resource(&intent, c.tenant(), &p.registry, &p.facts, &now()).unwrap();
            for (i, set) in [
                &mut p.bindings[0].capacity_authority,
                &mut p.policy.platform,
                &mut p.policy.resource,
                &mut p.policy.information_use,
                &mut p.grants[0].authority,
            ]
            .into_iter()
            .enumerate()
            {
                if mask & (1 << i) != 0 {
                    set.clear();
                }
            }
            let result = authorize(&intent, &a, &r, &trusted(&p), &now()).unwrap();
            proptest::prop_assert_eq!(result.decision == Decision::Allow, mask == 0);
            for pair in result.trace.windows(2) {
                proptest::prop_assert!(pair[1].remaining.is_subset(&pair[0].remaining));
            }
            Ok(())
        })
        .unwrap();
}
#[test]
fn deterministic_command_replay_preserves_state_and_rejects_stale_commands() {
    TestRunner::default()
        .run(&proptest::prelude::any::<u64>(), |seed| {
            let (mut s, p) = fixture();
            let c = caller(&p);
            s.client_request_id = format!("request-{seed}").try_into().unwrap();
            let action = EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(s),
                snapshot: trusted(&p),
                link: None,
            };
            let old = EffectState::default();
            let cmd = validate_command(&old, &c, now(), action).unwrap();
            let (a, id) = replay_command(&old, c.tenant(), cmd.bytes()).unwrap();
            let (b, other) = replay_command(&old, c.tenant(), cmd.bytes()).unwrap();
            proptest::prop_assert_eq!(&a, &b);
            proptest::prop_assert_eq!(id, other);
            proptest::prop_assert!(replay_command(&a, c.tenant(), cmd.bytes()).is_err());
            Ok(())
        })
        .unwrap();
}

#[test]
fn unknown_control_and_revoked_qualification_fail_closed() {
    let (_, mut p) = fixture();
    let control = p.worker.as_ref().unwrap().controls[&opaque("cpu")].clone();
    p.worker.as_mut().unwrap().controls.insert(opaque("unknown-isolation"), control);
    p.worker.as_mut().unwrap().mandatory.insert(opaque("unknown-isolation"));
    requalify(&mut p);
    deny(p, Reason::MandatoryControl(opaque("unknown-isolation")));
    let (_, mut p) = fixture();
    p.fixture.as_mut().unwrap().revoked = true;
    deny(p, Reason::ProfileDrift);
}
#[test]
fn resolution_attests_symlinks_dns_redirects_and_exact_audience() {
    let (mut s, mut p) = fixture();
    let c = caller(&p);
    for target in [
        Target::Filesystem {
            root: opaque("root"),
            components: vec!["file".into()],
            symlink_chain: ContentHash::of(b"no-links"),
        },
        Target::Network {
            scheme: opaque("https"),
            host: opaque("example.com"),
            port: 443,
            addresses: BTreeSet::from([opaque("192.0.2.1")]),
            redirects: vec![],
            path: "/".into(),
        },
        Target::Communication {
            account: opaque("account"),
            sender: c.principal().clone(),
            channel: opaque("channel"),
            thread: None,
            audience: BTreeSet::from([c.principal().clone()]),
            visibility: opaque("private"),
        },
    ] {
        s.resource.namespace = opaque(target.namespace());
        p.registry.entries[0].namespace = s.resource.namespace.clone();
        p.facts[0].selector = s.resource.clone();
        p.facts[0].document.target = target;
        p.facts[0].registry = digest(&p.registry).unwrap();
        p.facts[0].attested_document = digest(&p.facts[0].document).unwrap();
        s.information_use = Some(InformationUseEnvelope {
            purpose_refs: vec![],
            allowed_audiences: vec![c.principal().clone()],
            retention_policy_ref: None,
            disclosure_policy_ref: None,
            derivation_policy_ref: None,
            redistribution: RedistributionPolicy::NamedAudiences,
            expiration: None,
            source_obligation_refs: vec![],
        });
        let agency = resolve_agency(&c, &s, &p.bindings, &now()).unwrap();
        let intent = EffectIntent {
            id: EffectIntentId::generate(),
            submission: s.clone(),
            agency: agency.context,
            stable_idempotency_identity: opaque("identity"),
            submitted_at: now(),
        };
        assert!(resolve_resource(&intent, c.tenant(), &p.registry, &p.facts, &now()).is_ok());
        match &mut p.facts[0].document.target {
            Target::Filesystem { symlink_chain, .. } => {
                *symlink_chain = ContentHash::of(b"changed-link")
            }
            Target::Network { addresses, redirects, .. } => {
                addresses.insert(opaque("127.0.0.1"));
                redirects.push(opaque("internal"));
            }
            Target::Communication { audience, .. } => {
                audience.insert(PrincipalId::generate());
            }
            _ => unreachable!(),
        }
        assert!(resolve_resource(&intent, c.tenant(), &p.registry, &p.facts, &now()).is_err());
        if matches!(p.facts[0].document.target, Target::Communication { .. }) {
            p.facts[0].attested_document = digest(&p.facts[0].document).unwrap();
            assert!(resolve_resource(&intent, c.tenant(), &p.registry, &p.facts, &now()).is_err());
        }
    }
}
#[test]
fn purpose_candidate_and_lifecycle_labels_never_supply_authority() {
    for class in [EffectClass::Publish, EffectClass::Lifecycle, EffectClass::Mutate] {
        let (mut s, p) = fixture();
        s.effect_class = class;
        s.purpose = Some(opaque("self-initiated-approved-candidate"));
        let t = tempfile::tempdir().unwrap();
        let r = root(&t);
        let id = submit(&r, &s, &p);
        resolve(&r, &id, &p);
        assert_eq!(evaluate(&r, &id, &p).phase, Phase::Denied);
    }
}
#[test]
fn inspection_rejects_missing_and_contradictory_evidence() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, mut o) = start(&r, &s, &p);
    o.commit = CommitObservation::ConfirmedCommitted;
    o.outcome = ExecutionOutcome::Succeeded;
    o.durable_outputs = o.required_outputs.clone();
    o.boundary_record = Some(p.grants[0].evidence.clone());
    outcome(&r, &p, &id, o).unwrap();
    let original = r.effect_snapshot(caller(&p).tenant()).unwrap().effects[&id].clone();
    for mode in 0..5 {
        let mut e = original.clone();
        match mode {
            0 => e.authorization = None,
            1 => e.commit = CommitObservation::Unknown,
            2 => e.outcomes.clear(),
            3 => e.outcomes[0].durable_outputs.clear(),
            _ => {
                e.dispatches[0].acknowledgment.as_mut().unwrap().request_digest =
                    ContentHash::of(b"wrong")
            }
        };
        assert!(inspect_effect(&e).is_err());
    }
}
#[test]
fn unpublished_output_cannot_be_marked_successful_or_survive_reopen() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let (id, mut o) = start(&r, &s, &p);
    o.commit = CommitObservation::ConfirmedCommitted;
    o.outcome = ExecutionOutcome::Succeeded;
    o.durable_outputs = o.required_outputs.clone();
    o.boundary_record = Some(p.grants[0].evidence.clone());
    assert!(Egress::new(&r)
        .apply(
            &caller(&p),
            now(),
            EffectAction::Outcome {
                id: id.clone(),
                evidence: VerifiedOutcome::from_trusted_verifier(o.clone())
            }
        )
        .is_err());
    outcome(&r, &p, &id, o).unwrap();
    drop(r);
    let c = rusqlite::Connection::open(t.path().join("root/context.sqlite")).unwrap();
    c.execute("DELETE FROM artifacts WHERE content=?", [p.required_outputs[0].content.as_str()])
        .unwrap();
    drop(c);
    assert!(StoreRoot::open_existing(t.path().join("root")).is_err());
}
#[test]
fn concurrent_conflicting_requests_commit_only_one_intent() {
    let (s, p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let mut changed = s.clone();
    changed.budget.timeout_millis += 1;
    let results = std::thread::scope(|scope| {
        [s, changed]
            .into_iter()
            .map(|s| {
                let r = &r;
                let p = &p;
                scope.spawn(move || {
                    Egress::new(r)
                        .apply(
                            &caller(p),
                            now(),
                            EffectAction::Submit {
                                id: EffectIntentId::generate(),
                                submission: Box::new(s),
                                snapshot: trusted(p),
                                link: None,
                            },
                        )
                        .is_ok()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.into_iter().filter(|b| *b).count(), 1);
    let state = r.effect_snapshot(caller(&p).tenant()).unwrap();
    assert_eq!(state.effects.len(), 1);
    assert_eq!(state.requests.len(), 1);
    assert_eq!(state.revision, 1);
}
#[test]
fn profile_change_requires_explicit_exact_reevaluation() {
    let (s, mut p) = fixture();
    let t = tempfile::tempdir().unwrap();
    let r = root(&t);
    let id = ready(&r, &s, &p);
    p.worker.as_mut().unwrap().manifest.configuration =
        r.publish(opaque("tenant:fixture"), b"new reviewed configuration").unwrap();
    requalify(&mut p);
    let mut q = Queue::default();
    assert!(Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).is_err());
    assert!(q.calls.is_empty());
    assert_eq!(evaluate(&r, &id, &p).phase, Phase::Authorized);
    assert!(Egress::new(&r).deliver_test(&caller(&p), now(), &id, trusted(&p), &mut q).unwrap());
}
