use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::OpaqueRef as AqRef,
    causal::ControlMutationContext,
    continuation::{
        AdmitSignalRequest, CheckpointRef, SignalEligibility, SignalFilter, SignalIngressContext,
        SignalKind, SignalNamespace, WaitMatchPolicy, WaitSpec,
    },
    control::{ControlScope, HostControlContext},
    data_ref::DataRef,
    disposition::AttemptDisposition,
    ids::{CheckpointId, CorrelationId, SignalId, SignalSequence, TaskId, WaitId},
    limits::{ContinuationLimits, SignalLimits, SignalRetentionPolicy},
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_executor_local::handler::{ExecutorContext, ExecutorHandler};
use actionqueue_runtime::{
    config::RuntimeConfig,
    engine::{ActionQueueEngine, BootstrappedEngine},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use worldinterface_domain::ingress::{provider_artifact_scope, IngressResult};
use worldinterface_ingress::{
    transport::{
        streaming::{SessionHandshake, StreamSession},
        ProviderCredentials,
    },
    AuthenticatedScope, BindingAuthority, BindingGrant, Ingress,
};
use worldinterface_protocol::*;
use worldinterface_runtime::{
    ingress::{
        AdmissionOutcome, AqDispatchFailure, AqIngressPort, BoundAqIngress, IngressDeliveryHandler,
        IngressRuntime, SubscriberTransport, WakeOutcome,
    },
    storage::StoreBundle,
};
use worldinterface_storage::StoreProfile;

struct Authority(IngressBinding);
impl BindingAuthority for Authority {
    fn resolve_and_authorize(
        &self,
        _: &AuthenticatedScope,
        _: &IngressBindingSubmission,
        _: u64,
    ) -> worldinterface_ingress::Result<BindingGrant> {
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
struct Complete;
impl ExecutorHandler for Complete {
    fn execute(&self, _: ExecutorContext) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
struct Subscriber(std::sync::Mutex<Vec<String>>);
impl SubscriberTransport for Subscriber {
    fn deliver(
        &self,
        delivery: &worldinterface_ingress::IssuedDelivery,
    ) -> worldinterface_runtime::Result<()> {
        self.0.lock().unwrap().push(delivery.token.expose_to_subscriber().to_owned());
        Ok(())
    }
}
struct CursorHandler(std::sync::Arc<std::sync::Mutex<Vec<u64>>>);
impl ExecutorHandler for CursorHandler {
    fn execute(&self, ctx: ExecutorContext) -> AttemptDisposition {
        if ctx.input.payload != b"cursor" {
            return AttemptDisposition::complete(None);
        }
        let cursor = match ctx.input.resume_context.as_ref().map(|r| &r.wake) {
            Some(actionqueue_core::continuation::WakeReason::Signal {
                signal_sequence, ..
            }) => {
                assert_eq!(
                    ctx.input.resume_context.as_ref().unwrap().checkpoint.as_ref().unwrap().data,
                    DataRef::from_bytes(b"cursor-checkpoint".to_vec()).unwrap()
                );
                self.0.lock().unwrap().push(signal_sequence.get());
                if signal_sequence.get() >= 2 {
                    return AttemptDisposition::complete(None);
                }
                SignalEligibility::After(*signal_sequence)
            }
            _ => SignalEligibility::AnyRetained,
        };
        let filter = SignalFilter {
            tenant_id: None,
            namespace: SignalNamespace::new("wi_ingress").unwrap(),
            kind: SignalKind::new("cursor").unwrap(),
            correlation_id: Some(CorrelationId::new("cursor").unwrap()),
            source_ref: None,
        };
        AttemptDisposition::awaiting(
            WaitSpec::new(WaitId::new(), filter, WaitMatchPolicy::FirstMatch, cursor, None)
                .unwrap(),
            Some(CheckpointRef {
                checkpoint_id: CheckpointId::new(),
                data: DataRef::from_bytes(b"cursor-checkpoint".to_vec()).unwrap(),
                created_by_attempt: ctx.input.attempt_id,
            }),
        )
    }
}
fn task(id: TaskId, payload: &[u8]) -> EnsureTaskRequest {
    EnsureTaskRequest::for_task(
        TaskSpec::new(
            id,
            TaskPayload::new(payload.to_vec()),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap(),
        vec![],
    )
    .unwrap()
}
fn cursor_signal(n: u8) -> AdmitSignalRequest {
    AdmitSignalRequest::new(
        SignalId::new(format!("cursor-{n}")).unwrap(),
        SignalNamespace::new("wi_ingress").unwrap(),
        SignalKind::new("cursor").unwrap(),
        Some(CorrelationId::new("cursor").unwrap()),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap()
}
struct LostAcknowledgment<'a> {
    aq: &'a mut BootstrappedEngine<Complete>,
    tenant: OpaqueRef,
    after_task: bool,
}
impl AqIngressPort for LostAcknowledgment<'_> {
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn ensure_delivery_task(
        &mut self,
        request: EnsureTaskRequest,
    ) -> std::result::Result<AdmissionOutcome, AqDispatchFailure> {
        self.aq.ensure_task(request).map_err(|_| AqDispatchFailure::Uncertain)?;
        if self.after_task {
            Err(AqDispatchFailure::Uncertain)
        } else {
            Ok(AdmissionOutcome::AlreadyExists)
        }
    }
    fn admit_wake(
        &mut self,
        request: AdmitSignalRequest,
    ) -> std::result::Result<WakeOutcome, AqDispatchFailure> {
        let _ = self.aq.admit_signal(request).map_err(|_| AqDispatchFailure::Uncertain)?;
        Err(AqDispatchFailure::Uncertain)
    }
}
fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqRef::new("trusted-ingress-host").unwrap()),
    }
}
fn sign(bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

#[tokio::test]
async fn captured_fanout_routes_each_delivery_to_its_trusted_subscriber() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = std::sync::Arc::new(
        StoreBundle::initialize(
            &temp.path().join("bundle"),
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap(),
    );
    let mut first: IngressBinding =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/ingress/binding.json"))
            .unwrap();
    let current = now();
    first.expires_at_ms = current + 1_000_000;
    first.policy.retention_ms = 1_000_000;
    first.policy.acknowledgment_timeout_ms = 10_000;
    let mut second = first.clone();
    second.id = IngressBindingId::generate();
    second.subscriber = PrincipalId::generate();
    second.submission.client_request_id = "request-2".to_owned().try_into().unwrap();
    second.submission.subscriber = "subscriber-endpoint-2".to_owned().try_into().unwrap();
    let root = bundle.wi();
    root.publish(first.profile.scope.tenant.clone(), b"authorization").unwrap();
    let owner = AuthenticatedScope {
        tenant: first.profile.scope.tenant.clone(),
        principal: first.principal.clone(),
    };
    for binding in [&first, &second] {
        Ingress::new(root)
            .submit_binding(
                &owner,
                binding.submission.clone(),
                binding.id.clone(),
                &Authority(binding.clone()),
                current,
            )
            .unwrap();
    }
    let event = ProviderEvent {
        scope: first.profile.scope.clone(),
        event_id: Some("fanout-event".to_owned().try_into().unwrap()),
        offset: Some(0),
        produced_at_ms: current,
        asserted_sender: "asserted".to_owned().try_into().unwrap(),
        asserted_audience: vec![],
        content: root
            .publish(provider_artifact_scope(&first.profile.scope).unwrap(), b"message")
            .unwrap(),
        attachments: vec![],
        information_use: InformationUseEnvelope {
            purpose_refs: vec![],
            allowed_audiences: vec![first.subscriber.clone(), second.subscriber.clone()],
            retention_policy_ref: None,
            disclosure_policy_ref: None,
            derivation_policy_ref: None,
            redistribution: RedistributionPolicy::Prohibited,
            expiration: None,
            source_obligation_refs: vec![],
        },
        profile: ObservationProfile::Ordinary,
    };
    let body = canonical_data(&event).unwrap();
    let service = IngressRuntime::new(root, first.profile.scope.tenant.clone());
    let credential = ProviderCredentials::new(first.profile.clone(), vec![7; 32]).unwrap();
    let admitted = service.accept_http(&credential, &body, &sign(&body), current).unwrap();
    assert!(
        matches!(admitted, IngressResult::Admitted { ref deliveries, .. } if deliveries.len() == 2)
    );
    let first_transport = std::sync::Arc::new(Subscriber(std::sync::Mutex::new(Vec::new())));
    let second_transport = std::sync::Arc::new(Subscriber(std::sync::Mutex::new(Vec::new())));
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, first.profile.scope.tenant.clone()),
                10,
                current + 1,
            )
            .unwrap(),
        2
    );
    aq.shutdown().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let handler = IngressDeliveryHandler::with_subscribers(
        bundle.clone(),
        first.profile.scope.tenant.clone(),
        [
            (
                first.subscriber.clone(),
                first_transport.clone() as std::sync::Arc<dyn SubscriberTransport>,
            ),
            (
                second.subscriber.clone(),
                second_transport.clone() as std::sync::Arc<dyn SubscriberTransport>,
            ),
        ],
    );
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    for _ in 0..4 {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
        if first_transport.0.lock().unwrap().len() == 1
            && second_transport.0.lock().unwrap().len() == 1
        {
            break;
        }
    }
    assert_eq!(first_transport.0.lock().unwrap().len(), 1);
    assert_eq!(second_transport.0.lock().unwrap().len(), 1);
    aq.shutdown().unwrap();
}

#[test]
fn process_death_after_wi_commit_recovers_aq_obligation() {
    if let Some(path) = std::env::var_os("WI_INGRESS_CRASH_BUNDLE") {
        let path = std::path::PathBuf::from(path);
        let bundle = StoreBundle::initialize(
            &path,
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        let root = bundle.wi();
        let mut binding: IngressBinding = serde_json::from_str(include_str!(
            "../../../conformance/wi-fabric-2/ingress/binding.json"
        ))
        .unwrap();
        let current = now();
        binding.expires_at_ms = current + 1_000_000;
        binding.policy.retention_ms = 1_000_000;
        root.publish(binding.profile.scope.tenant.clone(), b"authorization").unwrap();
        Ingress::new(root)
            .submit_binding(
                &AuthenticatedScope {
                    tenant: binding.profile.scope.tenant.clone(),
                    principal: binding.principal.clone(),
                },
                binding.submission.clone(),
                binding.id.clone(),
                &Authority(binding.clone()),
                current,
            )
            .unwrap();
        let event = ProviderEvent {
            scope: binding.profile.scope.clone(),
            event_id: Some("crash-event".to_owned().try_into().unwrap()),
            offset: Some(0),
            produced_at_ms: current,
            asserted_sender: "asserted".to_owned().try_into().unwrap(),
            asserted_audience: vec![],
            content: root
                .publish(provider_artifact_scope(&binding.profile.scope).unwrap(), b"message")
                .unwrap(),
            attachments: vec![],
            information_use: InformationUseEnvelope {
                purpose_refs: vec![],
                allowed_audiences: vec![binding.subscriber.clone()],
                retention_policy_ref: None,
                disclosure_policy_ref: None,
                derivation_policy_ref: None,
                redistribution: RedistributionPolicy::Prohibited,
                expiration: None,
                source_obligation_refs: vec![],
            },
            profile: ObservationProfile::Ordinary,
        };
        let body = canonical_data(&event).unwrap();
        let credential = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
        IngressRuntime::new(root, binding.profile.scope.tenant)
            .accept_http(&credential, &body, &sign(&body), current)
            .unwrap();
        std::fs::write(std::env::var_os("WI_INGRESS_CRASH_READY").unwrap(), b"committed").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let ready = temp.path().join("ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_death_after_wi_commit_recovers_aq_obligation", "--nocapture"])
        .env("WI_INGRESS_CRASH_BUNDLE", &path)
        .env("WI_INGRESS_CRASH_READY", &ready)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready.exists() && std::time::Instant::now() < deadline {
        assert!(child.try_wait().unwrap().is_none(), "child exited before WI commit");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let committed = ready.exists();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(committed, "child did not reach committed state");
    let bundle = StoreBundle::open(&path).unwrap();
    let tenant: OpaqueRef = "tenant-a".to_owned().try_into().unwrap();
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.admissions.len(), 1);
    assert_eq!(state.queue.len(), 1);
    assert!(state.queue.values().all(|q| !q.task_admitted && q.signal_sequence.is_none()));
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        IngressRuntime::new(bundle.wi(), tenant.clone())
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap(),
        1
    );
    let replayed = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert!(replayed.queue.values().all(|q| q.task_admitted && q.signal_sequence.is_some()));
    aq.shutdown().unwrap();
}
#[tokio::test]
async fn ab_wi_007_commit_replay_identity_and_subscriber_ack() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let bundle = std::sync::Arc::new(
        StoreBundle::initialize(
            &path,
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap(),
    );
    let mut binding: IngressBinding =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/ingress/binding.json"))
            .unwrap();
    let current = now();
    binding.expires_at_ms = current + 1_000_000;
    binding.policy.retention_ms = 1_000_000;
    binding.policy.acknowledgment_timeout_ms = 1000;
    binding.policy.backoff_ms = 1000;
    let root = bundle.wi();
    assert_eq!(
        root.publish(binding.profile.scope.tenant.clone(), b"authorization").unwrap(),
        binding.authorization.artifact
    );
    let owner = AuthenticatedScope {
        tenant: binding.profile.scope.tenant.clone(),
        principal: binding.principal.clone(),
    };
    Ingress::new(root)
        .submit_binding(
            &owner,
            binding.submission.clone(),
            binding.id.clone(),
            &Authority(binding.clone()),
            current,
        )
        .unwrap();
    let event = ProviderEvent {
        scope: binding.profile.scope.clone(),
        event_id: Some("event-1".to_owned().try_into().unwrap()),
        offset: Some(0),
        produced_at_ms: current,
        asserted_sender: "asserted".to_owned().try_into().unwrap(),
        asserted_audience: vec![],
        content: root
            .publish(provider_artifact_scope(&binding.profile.scope).unwrap(), b"message")
            .unwrap(),
        attachments: vec![],
        information_use: InformationUseEnvelope {
            purpose_refs: vec![],
            allowed_audiences: vec![binding.subscriber.clone()],
            retention_policy_ref: None,
            disclosure_policy_ref: None,
            derivation_policy_ref: None,
            redistribution: RedistributionPolicy::Prohibited,
            expiration: None,
            source_obligation_refs: vec![],
        },
        profile: ObservationProfile::Ordinary,
    };
    let body = canonical_data(&event).unwrap();
    let credential = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
    let service = IngressRuntime::new(root, binding.profile.scope.tenant.clone());
    assert!(service.accept_http(&credential, &body, &[0; 32], current).is_err());
    assert!(IngressRuntime::new(root, "wrong-tenant".to_owned().try_into().unwrap())
        .accept_http(&credential, &body, &sign(&body), current)
        .is_err());
    assert!(root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().admissions.is_empty());
    let first = service.accept_http(&credential, &body, &sign(&body), current).unwrap();
    let id = match first {
        IngressResult::Admitted { deliveries, duplicate: false, .. } => deliveries[0].clone(),
        _ => panic!("first admission"),
    };
    let state = root.ingress_snapshot(&binding.profile.scope.tenant).unwrap();
    assert_eq!(state.queue.len(), 1);
    assert!(!state.queue[&id].task_admitted);
    assert_eq!(state.admissions.len(), 1);
    let aq_path = bundle.aq_path();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: aq_path.clone(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert!(service
        .dispatch_pending(
            &mut BoundAqIngress::new(&mut aq, "other-tenant".to_owned().try_into().unwrap()),
            10,
            current + 1
        )
        .is_err());
    assert!(service
        .dispatch_pending(
            &mut LostAcknowledgment {
                aq: &mut aq,
                tenant: binding.profile.scope.tenant.clone(),
                after_task: true
            },
            10,
            current + 1
        )
        .is_err());
    assert!(
        !root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id].task_admitted
    );
    aq.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: aq_path.clone(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert!(service
        .dispatch_pending(
            &mut LostAcknowledgment {
                aq: &mut aq,
                tenant: binding.profile.scope.tenant.clone(),
                after_task: false
            },
            10,
            current + 2
        )
        .is_err());
    assert!(root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id].task_admitted);
    assert!(root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id]
        .signal_sequence
        .is_none());
    aq.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: aq_path.clone(), ..Default::default() },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
                10,
                current + 3
            )
            .unwrap(),
        1
    );
    let seq = root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id]
        .signal_sequence
        .unwrap();
    assert!(seq > 0);
    let record = aq.projection().signals().by_sequence(SignalSequence::new(seq)).unwrap();
    assert!(record.envelope().control_context.is_some());
    assert!(record.envelope().received_at > 0);
    let bytes =
        &root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id].signal_request;
    let replay: AdmitSignalRequest = serde_json::from_slice(bytes).unwrap();
    assert_eq!(aq.admit_signal(replay).unwrap().sequence().get(), seq);
    let changed = AdmitSignalRequest::new(
        SignalId::new(format!("wi-delivery-{}", id.as_str())).unwrap(),
        SignalNamespace::new("wi_ingress").unwrap(),
        SignalKind::new("changed").unwrap(),
        Some(CorrelationId::new(format!("task/{}", id.as_str())).unwrap()),
        None,
        Some(AqRef::new(format!("delivery:{}", id.as_str())).unwrap()),
        None,
        None,
        None,
    )
    .unwrap();
    assert!(aq.admit_signal(changed).is_err(), "changed producer content must conflict");
    aq.shutdown().unwrap();
    let mut aq =
        ActionQueueEngine::new(RuntimeConfig { data_dir: aq_path, ..Default::default() }, Complete)
            .bootstrap()
            .unwrap()
            .with_host(host());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
                10,
                current + 4
            )
            .unwrap(),
        0
    );
    let duplicate = service.accept_http(&credential, &body, &sign(&body), current + 4).unwrap();
    assert!(matches!(duplicate, IngressResult::Admitted { duplicate: true, .. }));
    assert!(matches!(
        worldinterface_runtime::ingress::http::accept(
            &service,
            &credential,
            std::io::Cursor::new(body.as_slice()),
            &sign(&body),
            current + 4,
        )
        .unwrap(),
        IngressResult::Admitted { duplicate: true, .. }
    ));
    assert!(worldinterface_runtime::ingress::http::accept(
        &service,
        &credential,
        std::io::Cursor::new(vec![0; credential.profile().max_body_bytes as usize + 1]),
        &[0; 32],
        current + 4,
    )
    .is_err());
    let handshake = SessionHandshake {
        session: "session-1".to_owned().try_into().unwrap(),
        scope: binding.profile.scope.clone(),
        reconnect_from: 0,
        expires_at_ms: current + 1000,
    };
    let handshake_bytes = canonical_data(&handshake).unwrap();
    let session = StreamSession::authenticate(
        &handshake_bytes,
        &sign(&handshake_bytes),
        ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap(),
        current,
    )
    .unwrap();
    let frame_signature = sign(&canonical_data(&(handshake.session, body.as_slice())).unwrap());
    assert!(matches!(
        service.accept_frame(&session, &body, &frame_signature, current + 4).unwrap(),
        IngressResult::Admitted { duplicate: true, .. }
    ));
    assert!(matches!(
        worldinterface_runtime::ingress::streaming::accept_frame(
            &service,
            &session,
            std::io::Cursor::new(body.as_slice()),
            &frame_signature,
            current + 4,
        )
        .unwrap(),
        IngressResult::Admitted { duplicate: true, contiguous_next: 1, .. }
    ));
    assert!(worldinterface_runtime::ingress::streaming::accept_frame(
        &service,
        &session,
        std::io::Cursor::new(vec![0; session.profile().max_body_bytes as usize + 1]),
        &[0; 32],
        current + 4,
    )
    .is_err());
    assert_eq!(root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue.len(), 1);
    aq.shutdown().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let subscriber = std::sync::Arc::new(Subscriber(std::sync::Mutex::new(Vec::new())));
    let scope = AuthenticatedScope {
        tenant: binding.profile.scope.tenant.clone(),
        principal: binding.subscriber.clone(),
    };
    let handler = IngressDeliveryHandler::new(bundle.clone(), scope.clone(), subscriber.clone());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    let issued = subscriber.0.lock().unwrap().clone();
    assert_eq!(issued.len(), 1, "AQ must resume the exact delivery wait");
    let token = worldinterface_ingress::DeliveryToken::from_subscriber(&issued[0]).unwrap();
    aq.shutdown().unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let handler = IngressDeliveryHandler::new(bundle.clone(), scope.clone(), subscriber.clone());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        subscriber.0.lock().unwrap().len(),
        2,
        "unacknowledged delivery must redeliver with the same DeliveryId"
    );
    service.acknowledge(&scope, &id, &token, now()).unwrap();
    assert!(root.ingress_snapshot(&binding.profile.scope.tenant).unwrap().deliveries[&id]
        .acknowledgment
        .is_some());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
                10,
                now()
            )
            .unwrap(),
        1
    );
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(subscriber.0.lock().unwrap().len(), 2);
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);
    let backup = temp.path().join("backup");
    StoreBundle::backup(&path, &backup).unwrap();
    let restored = temp.path().join("restored");
    StoreBundle::restore(&backup, &restored).unwrap();
    let reopened = StoreBundle::open(&restored).unwrap();
    assert!(reopened
        .wi()
        .ingress_snapshot(&binding.profile.scope.tenant)
        .unwrap()
        .acknowledgments_to_signal[&id]
        .sequence
        .is_some());
}

#[tokio::test]
async fn ab_wi_006_009_one_wait_and_strict_signal_cursor_across_restart() {
    assert!(SignalNamespace::new("*").is_err());
    let broad = SignalFilter {
        tenant_id: None,
        namespace: SignalNamespace::new("wi_ingress").unwrap(),
        kind: SignalKind::new("cursor").unwrap(),
        correlation_id: None,
        source_ref: None,
    };
    assert!(WaitSpec::new(
        WaitId::new(),
        broad,
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::AnyRetained,
        None
    )
    .is_err());
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let bundle = StoreBundle::initialize(
        &path,
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let aq_path = bundle.aq_path();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let task_id = TaskId::new();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: aq_path.clone(), ..Default::default() },
        CursorHandler(seen.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    aq.ensure_task(task(task_id, b"cursor")).unwrap();
    let first = aq.admit_signal(cursor_signal(1)).unwrap().sequence();
    assert_eq!(first, SignalSequence::new(1));
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    let run = aq.projection().run_ids_for_task(task_id)[0];
    assert!(aq.get_run(run).unwrap().active_wait_id.is_some());
    assert_eq!(*seen.lock().unwrap(), vec![1]);
    assert!(aq.projection().signals().by_sequence(first).unwrap().wal_sequence() > first.get());
    aq.ensure_task(task(TaskId::new(), b"unrelated mutation")).unwrap();
    aq.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: aq_path, ..Default::default() },
        CursorHandler(seen.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert!(aq.get_run(run).unwrap().active_wait_id.is_some());
    let filter = SignalFilter {
        tenant_id: None,
        namespace: SignalNamespace::new("wi_ingress").unwrap(),
        kind: SignalKind::new("cursor").unwrap(),
        correlation_id: Some(CorrelationId::new("cursor").unwrap()),
        source_ref: None,
    };
    assert!(aq.projection().signals().retained_candidates(&filter, first, 10).is_empty());
    let second = aq.admit_signal(cursor_signal(2)).unwrap().sequence();
    assert_eq!(second, SignalSequence::new(2));
    assert_eq!(
        aq.projection().signals().retained_candidates(&filter, first, 10)[0].sequence(),
        second
    );
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    assert!(aq.get_run(run).unwrap().active_wait_id.is_none());
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn ab_wi_022_retirement_keeps_resident_capacity_and_alarm() {
    let temp = tempfile::tempdir().unwrap();
    let bundle = StoreBundle::initialize(
        &temp.path().join("bundle"),
        StoreProfile {
            instance: InstanceId::generate(),
            created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
            platform: false,
        },
    )
    .unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            signal_limits: SignalLimits { identities: 2, ..Default::default() },
            signal_retention: SignalRetentionPolicy {
                minimum_age_secs: 0,
                minimum_sequence_window: 0,
            },
            ..Default::default()
        },
        Complete,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let first = aq.admit_signal(cursor_signal(1)).unwrap().sequence();
    aq.admit_signal(cursor_signal(2)).unwrap();
    let before = aq.signal_statistics();
    assert!(aq.admit_signal(cursor_signal(3)).is_err());
    assert!(aq.signal_statistics().capacity_rejections > before.capacity_rejections);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert_eq!(aq.retire_signals(vec![first], SignalIngressContext::default()).unwrap(), 1);
    let after = aq.signal_statistics();
    assert_eq!((after.retained, after.retired), (1, 1));
    assert_eq!(after.bytes, before.bytes);
    assert!(aq.projection().signals().by_sequence(first).is_some());
    assert!(
        aq.admit_signal(cursor_signal(3)).is_err(),
        "retirement must not pretend to reclaim resident identity capacity"
    );
    aq.shutdown().unwrap();
    for (global, tenant) in [(1, 2), (2, 1)] {
        let wait_bundle = StoreBundle::initialize(
            &temp.path().join(format!("wait-{global}-{tenant}")),
            StoreProfile {
                instance: InstanceId::generate(),
                created_at: "2026-09-29T00:00:00.000000000Z".to_owned().try_into().unwrap(),
                platform: false,
            },
        )
        .unwrap();
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: wait_bundle.aq_path(),
                continuation_limits: ContinuationLimits {
                    active_waits: global,
                    active_waits_per_tenant: tenant,
                    ..Default::default()
                },
                ..Default::default()
            },
            CursorHandler(std::sync::Arc::new(std::sync::Mutex::new(Vec::new()))),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        let a = TaskId::new();
        let b = TaskId::new();
        aq.ensure_task(task(a, b"cursor")).unwrap();
        aq.ensure_task(task(b, b"cursor")).unwrap();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
        let active = [a, b]
            .into_iter()
            .filter(|id| {
                let run = aq.projection().run_ids_for_task(*id)[0];
                aq.get_run(run).unwrap().active_wait_id.is_some()
            })
            .count();
        assert_eq!(active, 1, "configured active-wait limit must apply");
        aq.shutdown().unwrap();
    }
}
