use actionqueue_core::{
    bounded::OpaqueRef as AqRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    ids::TaskId,
    limits::ContinuationLimits,
};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use worldinterface_domain::ingress::{provider_artifact_scope, IngressResult};
use worldinterface_ingress::{
    transport::ProviderCredentials, AuthenticatedScope, BindingAuthority, BindingGrant, Ingress,
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
fn setup(path: &Path) -> (Arc<StoreBundle>, IngressBinding, DeliveryId) {
    setup_with_timeout(path, 1_000)
}
fn setup_with_timeout(
    path: &Path,
    acknowledgment_timeout_ms: u64,
) -> (Arc<StoreBundle>, IngressBinding, DeliveryId) {
    setup_with_policy(path, acknowledgment_timeout_ms, 1_000_000)
}
fn setup_with_policy(
    path: &Path,
    acknowledgment_timeout_ms: u64,
    retention_ms: u64,
) -> (Arc<StoreBundle>, IngressBinding, DeliveryId) {
    setup_with_binding_expiry(path, acknowledgment_timeout_ms, retention_ms, 1_000_000)
}
fn setup_with_binding_expiry(
    path: &Path,
    acknowledgment_timeout_ms: u64,
    retention_ms: u64,
    binding_lifetime_ms: u64,
) -> (Arc<StoreBundle>, IngressBinding, DeliveryId) {
    setup_with_event_quarantine(
        path,
        acknowledgment_timeout_ms,
        retention_ms,
        binding_lifetime_ms,
        false,
    )
}
fn setup_with_event_quarantine(
    path: &Path,
    acknowledgment_timeout_ms: u64,
    retention_ms: u64,
    binding_lifetime_ms: u64,
    quarantine_content: bool,
) -> (Arc<StoreBundle>, IngressBinding, DeliveryId) {
    let bundle = Arc::new(
        StoreBundle::initialize(
            path,
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
    binding.expires_at_ms = current + binding_lifetime_ms;
    binding.policy.retention_ms = retention_ms;
    binding.policy.acknowledgment_timeout_ms = acknowledgment_timeout_ms;
    binding.policy.backoff_ms = 1_000;
    let root = bundle.wi();
    root.publish(binding.profile.scope.tenant.clone(), b"authorization").unwrap();
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
    let content =
        root.publish(provider_artifact_scope(&binding.profile.scope).unwrap(), b"message").unwrap();
    let event = ProviderEvent {
        scope: binding.profile.scope.clone(),
        event_id: Some("recovery-event".to_owned().try_into().unwrap()),
        offset: Some(0),
        produced_at_ms: current,
        asserted_sender: "asserted".to_owned().try_into().unwrap(),
        asserted_audience: vec![],
        content: content.clone(),
        attachments: if quarantine_content {
            vec![GovernedAttachment {
                artifact: content,
                quarantine: Some("quarantine-1".to_owned().try_into().unwrap()),
            }]
        } else {
            vec![]
        },
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
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&body);
    let signature = mac.finalize().into_bytes();
    let credential = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
    let result = IngressRuntime::new(root, binding.profile.scope.tenant.clone())
        .accept_http(&credential, &body, &signature, current)
        .unwrap();
    let id = match result {
        IngressResult::Admitted { deliveries, .. } => deliveries[0].clone(),
        _ => panic!("admission failed"),
    };
    (bundle, binding, id)
}

#[tokio::test]
async fn expired_before_dispatch_stays_expired_across_restart_and_repeated_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup_with_policy(&path, 1_000, 30_000);
    let tenant = binding.profile.scope.tenant.clone();
    let expires_at = bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].expires_at_ms;
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    for _ in 0..3 {
        assert_eq!(
            service.dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, tenant.clone()),
                10,
                expires_at,
            ).unwrap(),
            0
        );
    }
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.deliveries[&id].state, DeliveryState::Expired);
    assert!(state.deliveries[&id].token_invalidated);
    assert_eq!(state.queue[&id].generation, 0);
    assert!(state.queue[&id].signal_sequence.is_none());
    assert!(aq
        .projection()
        .run_ids_for_task(TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap()))
        .is_empty());
    assert!(recorder.0.lock().unwrap().is_empty());
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);

    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    for _ in 0..3 {
        assert_eq!(
            IngressRuntime::new(bundle.wi(), tenant.clone())
                .dispatch_pending(
                    &mut BoundAqIngress::new(&mut aq, tenant.clone()),
                    10,
                    expires_at + 1,
                )
                .unwrap(),
            0
        );
    }
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation, 0);
    assert!(aq
        .projection()
        .run_ids_for_task(TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap()))
        .is_empty());
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn expiration_during_ack_wait_completes_without_recovery_generation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup_with_policy(&path, 3_000, 3_000);
    let tenant = binding.profile.scope.tenant.clone();
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    assert_eq!(
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap(),
        1
    );
    let _ = aq.run_until_idle().await.unwrap();
    let task = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
    let run = aq.projection().run_ids_for_task(task)[0];
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    let token = {
        let captured = recorder.0.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0], id);
        worldinterface_ingress::DeliveryToken::from_subscriber(&recorder.1.lock().unwrap()[0])
            .unwrap()
    };
    tokio::time::sleep(Duration::from_millis(4100)).await;
    assert_eq!(
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap(),
        0
    );
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Completed);
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.deliveries[&id].state, DeliveryState::Expired);
    assert!(state.deliveries[&id].token_invalidated);
    assert_eq!(state.deliveries[&id].attempts.len(), 1);
    assert_eq!(state.queue[&id].generation, 0);
    assert_eq!(aq.projection().run_ids_for_task(task).len(), 1);
    let subscriber =
        AuthenticatedScope { tenant: tenant.clone(), principal: binding.subscriber.clone() };
    assert!(service.claim(&subscriber, &id, now()).is_err());
    assert!(service.acknowledge(&subscriber, &id, &token, now()).is_err());
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);

    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    for _ in 0..3 {
        assert_eq!(
            IngressRuntime::new(bundle.wi(), tenant.clone())
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now(),)
                .unwrap(),
            0
        );
    }
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation, 0);
    assert_eq!(aq.projection().run_ids_for_task(task).len(), 1);
    assert_eq!(recorder.0.lock().unwrap().len(), 1);
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn inactive_binding_and_missing_registration_never_regenerate_aq_work() {
    for mode in ["revoked", "suspended", "binding-expired", "unregistered"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bundle");
        let lifetime = if mode == "binding-expired" { 1000 } else { 30_000 };
        let (bundle, binding, id) = setup_with_binding_expiry(&path, 1000, 30_000, lifetime);
        let tenant = binding.profile.scope.tenant.clone();
        let owner =
            AuthenticatedScope { tenant: tenant.clone(), principal: binding.principal.clone() };
        let recorder = Arc::new(Recorder::default());
        let subscriber_handler = |bundle: &Arc<StoreBundle>| {
            if mode == "unregistered" {
                IngressDeliveryHandler::with_subscribers(bundle.clone(), tenant.clone(), [])
            } else {
                handler(bundle, &binding, recorder.clone())
            }
        };
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
            subscriber_handler(&bundle),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        let service = IngressRuntime::new(bundle.wi(), tenant.clone());
        assert_eq!(
            service
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
                .unwrap(),
            1
        );
        match mode {
            "revoked" | "suspended" => {
                let status = if mode == "revoked" {
                    BindingStatus::Revoked
                } else {
                    BindingStatus::Suspended
                };
                Ingress::new(bundle.wi())
                    .change_binding(
                        &owner,
                        &BindingVersion { id: binding.id.clone(), revision: 1 },
                        status,
                        now(),
                    )
                    .unwrap();
            }
            "binding-expired" => tokio::time::sleep(Duration::from_millis(1100)).await,
            _ => {}
        }
        let _ = aq.run_until_idle().await.unwrap();
        let task = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
        let run = aq.projection().run_ids_for_task(task)[0];
        assert_eq!(
            aq.get_run(run).unwrap().state,
            actionqueue_core::run::state::RunState::Awaiting,
            "{mode}"
        );
        for _ in 0..3 {
            assert_eq!(
                service
                    .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
                    .unwrap(),
                0,
                "{mode}"
            );
        }
        assert_eq!(
            bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation,
            0,
            "{mode}"
        );
        assert!(recorder.0.lock().unwrap().is_empty(), "{mode}");
        aq.shutdown().unwrap();
        drop(service);
        drop(bundle);

        let reopened = Arc::new(StoreBundle::open(&path).unwrap());
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig { data_dir: reopened.aq_path(), ..Default::default() },
            subscriber_handler(&reopened),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        for _ in 0..3 {
            assert_eq!(
                IngressRuntime::new(reopened.wi(), tenant.clone())
                    .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
                    .unwrap(),
                0,
                "{mode}"
            );
        }
        assert_eq!(
            reopened.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation,
            0,
            "{mode}"
        );
        assert_eq!(aq.projection().run_ids_for_task(task).len(), 1, "{mode}");
        assert!(recorder.0.lock().unwrap().is_empty(), "{mode}");
        aq.shutdown().unwrap();
    }
}

#[tokio::test]
async fn quarantined_required_content_waits_without_aq_failure_then_releases_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup_with_event_quarantine(&path, 1_000, 30_000, 30_000, true);
    let tenant = binding.profile.scope.tenant.clone();
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    for _ in 0..3 {
        assert_eq!(
            IngressRuntime::new(bundle.wi(), tenant.clone())
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now(),)
                .unwrap(),
            0
        );
        let _ = aq.run_until_idle().await.unwrap();
    }
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.queue[&id].generation, 0);
    assert!(state.queue[&id].signal_sequence.is_none());
    assert!(state.deliveries[&id].attempts.is_empty());
    assert!(recorder.0.lock().unwrap().is_empty());
    aq.shutdown().unwrap();
    drop(bundle);

    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        IngressRuntime::new(bundle.wi(), tenant.clone())
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now(),)
            .unwrap(),
        0
    );
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    let admission = state.admissions.values().next().unwrap();
    let content = admission.event.content.clone();
    assert!(Ingress::new(bundle.wi())
        .inspect_delivery(
            &AuthenticatedScope { tenant: tenant.clone(), principal: binding.subscriber.clone() },
            &id,
            now(),
        )
        .is_err());
    let release = bundle.wi().publish(tenant.clone(), b"trusted release").unwrap();
    Ingress::new(bundle.wi())
        .release_attachment(
            &tenant,
            content,
            "quarantine-1".to_owned().try_into().unwrap(),
            EvidenceRef { artifact: release, schema: "release-v1".to_owned().try_into().unwrap() },
            now(),
        )
        .unwrap();
    assert_eq!(
        IngressRuntime::new(bundle.wi(), tenant.clone())
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now(),)
            .unwrap(),
        1
    );
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.queue[&id].generation, 0);
    assert_eq!(state.deliveries[&id].attempts.len(), 1);
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn newly_quarantined_required_content_keeps_existing_aq_run_awaiting() {
    let temp = tempfile::tempdir().unwrap();
    let (bundle, binding, id) = setup_with_policy(&temp.path().join("bundle"), 1_000, 30_000);
    let tenant = binding.profile.scope.tenant.clone();
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    assert_eq!(
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
            .unwrap(),
        1
    );
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    let mut event = state.admissions.values().next().unwrap().event.clone();
    let original_content = event.content.clone();
    event.event_id = Some("quarantine-later".to_owned().try_into().unwrap());
    event.offset = Some(1);
    event.produced_at_ms = now();
    event.content = bundle
        .wi()
        .publish(provider_artifact_scope(&binding.profile.scope).unwrap(), b"another message")
        .unwrap();
    event.attachments = vec![GovernedAttachment {
        artifact: original_content.clone(),
        quarantine: Some("quarantine-1".to_owned().try_into().unwrap()),
    }];
    let body = canonical_data(&event).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&body);
    let credential = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
    service.accept_http(&credential, &body, &mac.finalize().into_bytes(), now()).unwrap();
    let _ = aq.run_until_idle().await.unwrap();
    let task = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
    let run = aq.projection().run_ids_for_task(task)[0];
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    assert!(recorder.0.lock().unwrap().is_empty());
    assert!(bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].attempts.is_empty());
    for _ in 0..3 {
        let _ = service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
            .unwrap();
        assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation, 0);
        assert_eq!(
            aq.get_run(run).unwrap().state,
            actionqueue_core::run::state::RunState::Awaiting
        );
    }
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation, 0);
    let evidence = bundle.wi().publish(tenant.clone(), b"trusted release").unwrap();
    Ingress::new(bundle.wi())
        .release_attachment(
            &tenant,
            original_content,
            "quarantine-1".to_owned().try_into().unwrap(),
            EvidenceRef { artifact: evidence, schema: "release-v1".to_owned().try_into().unwrap() },
            now(),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while !recorder.0.lock().unwrap().contains(&id) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let _ = aq.run_until_idle().await.unwrap();
    }
    assert_eq!(recorder.0.lock().unwrap().iter().filter(|d| *d == &id).count(), 1);
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].generation, 0);
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn small_dispatch_limit_skips_waiting_prefix_and_publishes_ack_after_restart() {
    small_dispatch_limit_progress_for(BindingStatus::Suspended).await;
}

#[tokio::test]
async fn revoked_prefix_at_small_dispatch_limit_preserves_ready_and_ack_progress() {
    small_dispatch_limit_progress_for(BindingStatus::Revoked).await;
}

async fn small_dispatch_limit_progress_for(inactive_status: BindingStatus) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, _original) = setup_with_policy(&path, 1_000, 30_000);
    let tenant = binding.profile.scope.tenant.clone();
    let owner = AuthenticatedScope { tenant: tenant.clone(), principal: binding.principal.clone() };
    let mut another = binding.clone();
    another.id = IngressBindingId::generate();
    another.submission.client_request_id = "request-2".to_owned().try_into().unwrap();
    Ingress::new(bundle.wi())
        .submit_binding(
            &owner,
            another.submission.clone(),
            another.id.clone(),
            &Authority(another.clone()),
            now(),
        )
        .unwrap();
    let original_event = bundle
        .wi()
        .ingress_snapshot(&tenant)
        .unwrap()
        .admissions
        .values()
        .next()
        .unwrap()
        .event
        .clone();
    let mut event = original_event;
    event.event_id = Some("recovery-event-2".to_owned().try_into().unwrap());
    event.offset = Some(1);
    event.produced_at_ms = now();
    let body = canonical_data(&event).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
    mac.update(&body);
    let credential = ProviderCredentials::new(binding.profile.clone(), vec![7; 32]).unwrap();
    let result = IngressRuntime::new(bundle.wi(), tenant.clone())
        .accept_http(&credential, &body, &mac.finalize().into_bytes(), now())
        .unwrap();
    let admitted = match result {
        IngressResult::Admitted { deliveries, .. } => deliveries,
        _ => panic!("second event not admitted"),
    };
    assert_eq!(admitted.len(), 2);
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    let first = state.queue.keys().next().unwrap().clone();
    let waiting_binding = state.deliveries[&first].binding.id.clone();
    let ready =
        state.deliveries.values().find(|d| d.binding.id != waiting_binding).unwrap().id.clone();
    assert!(first < ready);
    Ingress::new(bundle.wi())
        .change_binding(
            &owner,
            &BindingVersion { id: waiting_binding, revision: 1 },
            inactive_status,
            now(),
        )
        .unwrap();
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    assert_eq!(
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
            .unwrap(),
        0
    );
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().dispatch_cursor, Some(first.clone()));
    for _ in 0..3 {
        let _ = service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now())
            .unwrap();
    }
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert!(state.queue[&ready].signal_sequence.is_some());
    assert!(state.queue[&first].signal_sequence.is_none());
    assert_eq!(state.queue[&first].generation, 0);
    let _ = aq.run_until_idle().await.unwrap();
    let delivered = recorder.0.lock().unwrap().clone();
    let tokens = recorder.1.lock().unwrap().clone();
    let position = delivered.iter().position(|id| id == &ready).unwrap();
    let token = worldinterface_ingress::DeliveryToken::from_subscriber(&tokens[position]).unwrap();
    service
        .acknowledge(
            &AuthenticatedScope { tenant: tenant.clone(), principal: binding.subscriber.clone() },
            &ready,
            &token,
            now(),
        )
        .unwrap();
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);

    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        IngressRuntime::new(bundle.wi(), tenant.clone())
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now(),)
            .unwrap(),
        1
    );
    for _ in 0..3 {
        assert_eq!(
            IngressRuntime::new(bundle.wi(), tenant.clone())
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 1, now(),)
                .unwrap(),
            0
        );
    }
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert!(state.acknowledgments_to_signal[&ready].sequence.is_some());
    assert_eq!(state.queue[&first].generation, 0);
    assert_eq!(state.deliveries[&first].attempts.len(), 0);
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn expired_terminal_aq_task_does_not_allocate_recovery_generation() {
    let temp = tempfile::tempdir().unwrap();
    let (bundle, binding, id) = setup_with_policy(&temp.path().join("bundle"), 1_000, 30_000);
    let tenant = binding.profile.scope.tenant.clone();
    let expires_at = bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].expires_at_ms;
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 0, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    assert_eq!(
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap(),
        1
    );
    let _ = aq.run_until_idle().await.unwrap();
    let task = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
    let run = aq.projection().run_ids_for_task(task)[0];
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Failed);
    for _ in 0..3 {
        assert_eq!(service.dispatch_pending(
            &mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, expires_at,
        ).unwrap(), 0);
    }
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    assert_eq!(state.deliveries[&id].state, DeliveryState::Expired);
    assert!(state.deliveries[&id].attempts.is_empty());
    assert_eq!(state.queue[&id].generation, 0);
    assert_eq!(aq.projection().run_ids_for_task(task).len(), 1);
    assert!(recorder.0.lock().unwrap().is_empty());
    aq.shutdown().unwrap();
}

// Simulate a process dying after AQ task admission, or a failed AQ signal call.
// Both leave a runnable task with no delivery signal; neither may retain an
// unbounded first wait after WI expires the delivery.
struct InterruptedWake<'a> {
    aq: &'a mut actionqueue_runtime::engine::BootstrappedEngine<IngressDeliveryHandler>,
    tenant: OpaqueRef,
    fail_after_task: bool,
}
impl AqIngressPort for InterruptedWake<'_> {
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn ensure_delivery_task(
        &mut self,
        request: actionqueue_core::admission::EnsureTaskRequest,
    ) -> std::result::Result<AdmissionOutcome, AqDispatchFailure> {
        let outcome = BoundAqIngress::new(&mut *self.aq, self.tenant.clone())
            .ensure_delivery_task(request)?;
        if self.fail_after_task {
            Err(AqDispatchFailure::Uncertain)
        } else {
            Ok(outcome)
        }
    }
    fn admit_wake(
        &mut self,
        _request: actionqueue_core::continuation::AdmitSignalRequest,
    ) -> std::result::Result<WakeOutcome, AqDispatchFailure> {
        Err(AqDispatchFailure::Uncertain)
    }
    fn delivery_task_terminal(
        &mut self,
        id: TaskId,
    ) -> std::result::Result<bool, AqDispatchFailure> {
        BoundAqIngress::new(&mut *self.aq, self.tenant.clone()).delivery_task_terminal(id)
    }
}

async fn initial_wait_expires_after_interrupted_wake(replacement: bool, fail_after_task: bool) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup_with_policy(&path, 1_000, 5_000);
    let tenant = binding.profile.scope.tenant.clone();
    let expires_at = bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].expires_at_ms;
    let recorder = Arc::new(Recorder::default());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    if replacement {
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: bundle.aq_path(),
                continuation_limits: ContinuationLimits { active_waits: 0, ..Default::default() },
                ..Default::default()
            },
            handler(&bundle, &binding, recorder.clone()),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        assert_eq!(
            service
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now(),)
                .unwrap(),
            1
        );
        let _ = aq.run_until_idle().await.unwrap();
        let original = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
        let run = aq.projection().run_ids_for_task(original)[0];
        assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Failed);
        aq.shutdown().unwrap();
    }
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 1, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert!(service
        .dispatch_pending(
            &mut InterruptedWake { aq: &mut aq, tenant: tenant.clone(), fail_after_task },
            10,
            now(),
        )
        .is_err());
    let q = bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].clone();
    assert_eq!(q.generation, u32::from(replacement));
    assert_eq!(q.task_admitted, !fail_after_task);
    assert!(q.signal_sequence.is_none());
    let task: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(&q.task_request).unwrap();
    let task_id = task.task_spec().id();
    let _ = aq.run_until_idle().await.unwrap();
    let run = aq.projection().run_ids_for_task(task_id)[0];
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    assert_eq!(aq.projection().waits().active_count(), 1);
    assert!(aq.projection().waits().active(run).unwrap().spec.deadline().is_some());
    assert!(recorder.0.lock().unwrap().is_empty());
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);

    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 1, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    assert_eq!(aq.projection().waits().active_count(), 1);
    let remaining = expires_at.saturating_sub(now());
    tokio::time::sleep(Duration::from_millis(remaining + 1200)).await;
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    for _ in 0..3 {
        assert_eq!(
            service
                .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now(),)
                .unwrap(),
            0
        );
    }
    assert_eq!(
        bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].state,
        DeliveryState::Expired
    );
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Completed);
    assert_eq!(aq.projection().waits().active_count(), 0);
    let q = &bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id];
    assert_eq!(q.generation, u32::from(replacement));
    assert!(q.signal_sequence.is_none());
    assert_eq!(aq.projection().run_ids_for_task(task_id).len(), 1);
    assert!(recorder.0.lock().unwrap().is_empty());
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn failed_signal_admission_releases_initial_wait_after_expiration() {
    initial_wait_expires_after_interrupted_wake(false, false).await;
}

#[tokio::test]
async fn interrupted_task_admission_releases_initial_wait_after_expiration() {
    initial_wait_expires_after_interrupted_wake(false, true).await;
}

#[tokio::test]
async fn replacement_generation_releases_initial_wait_after_expiration() {
    initial_wait_expires_after_interrupted_wake(true, false).await;
}

fn handler(
    bundle: &Arc<StoreBundle>,
    binding: &IngressBinding,
    transport: Arc<dyn SubscriberTransport>,
) -> IngressDeliveryHandler {
    IngressDeliveryHandler::new(
        bundle.clone(),
        AuthenticatedScope {
            tenant: binding.profile.scope.tenant.clone(),
            principal: binding.subscriber.clone(),
        },
        transport,
    )
}
#[derive(Default)]
struct Recorder(Mutex<Vec<DeliveryId>>, Mutex<Vec<String>>);
impl SubscriberTransport for Recorder {
    fn deliver(
        &self,
        delivery: &worldinterface_ingress::IssuedDelivery,
    ) -> worldinterface_runtime::Result<()> {
        self.0.lock().unwrap().push(delivery.view.id.clone());
        self.1.lock().unwrap().push(delivery.token.expose_to_subscriber().to_owned());
        Ok(())
    }
}
struct ConcurrentWriter {
    recorder: Recorder,
    bundle: Arc<StoreBundle>,
    binding: IngressBinding,
}
impl SubscriberTransport for ConcurrentWriter {
    fn deliver(
        &self,
        delivery: &worldinterface_ingress::IssuedDelivery,
    ) -> worldinterface_runtime::Result<()> {
        self.recorder.deliver(delivery)?;
        let mut binding = self.binding.clone();
        binding.id = IngressBindingId::generate();
        binding.submission.client_request_id = binding.id.as_str().to_owned().try_into()?;
        let scope = AuthenticatedScope {
            tenant: binding.profile.scope.tenant.clone(),
            principal: binding.principal.clone(),
        };
        Ingress::new(self.bundle.wi()).submit_binding(
            &scope,
            binding.submission.clone(),
            binding.id.clone(),
            &Authority(binding),
            now(),
        )?;
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_journal_write_preserves_ack_wait_and_redelivery() {
    let temp = tempfile::tempdir().unwrap();
    let (bundle, binding, id) = setup(&temp.path().join("bundle"));
    let writer = Arc::new(ConcurrentWriter {
        recorder: Recorder::default(),
        bundle: bundle.clone(),
        binding: binding.clone(),
    });
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, writer.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), binding.profile.scope.tenant.clone());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
                10,
                now(),
            )
            .unwrap(),
        1
    );
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(writer.recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    assert_eq!(
        bundle.wi().ingress_snapshot(&binding.profile.scope.tenant).unwrap().deliveries[&id]
            .attempts[0]
            .status,
        AttemptStatus::WakeSucceeded
    );
    aq.shutdown().unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, writer.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(writer.recorder.0.lock().unwrap().as_slice(), &[id.clone(), id]);
    aq.shutdown().unwrap();
}

struct DurableStop(std::path::PathBuf);
impl SubscriberTransport for DurableStop {
    fn deliver(
        &self,
        delivery: &worldinterface_ingress::IssuedDelivery,
    ) -> worldinterface_runtime::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(self.0.with_extension("record"))?;
        file.write_all(delivery.view.id.as_str().as_bytes())?;
        file.sync_all()?;
        std::fs::File::open(self.0.parent().unwrap())?.sync_all()?;
        std::fs::write(&self.0, b"stored")?;
        loop {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
#[tokio::test]
async fn process_death_after_subscriber_storage_redelivers_same_id() {
    if let Some(path) = std::env::var_os("WI_RECOVERY_CHILD_BUNDLE") {
        let (bundle, binding, _) = setup_with_timeout(Path::new(&path), 10_000);
        let stop: Arc<dyn SubscriberTransport> =
            Arc::new(DurableStop(std::env::var_os("WI_RECOVERY_CHILD_READY").unwrap().into()));
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: bundle.aq_path(),
                lease_timeout_secs: 3,
                ..Default::default()
            },
            handler(&bundle, &binding, stop),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        IngressRuntime::new(bundle.wi(), binding.profile.scope.tenant.clone())
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant),
                10,
                now(),
            )
            .unwrap();
        let _ = aq.run_until_idle().await.unwrap();
        panic!("subscriber should block until process death");
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let ready = temp.path().join("stored");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_death_after_subscriber_storage_redelivers_same_id",
            "--nocapture",
        ])
        .env("WI_RECOVERY_CHILD_BUNDLE", &path)
        .env("WI_RECOVERY_CHILD_READY", &ready)
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !ready.exists() && std::time::Instant::now() < deadline {
        assert!(child.try_wait().unwrap().is_none(), "child exited before subscriber storage");
        std::thread::sleep(Duration::from_millis(10));
    }
    let stored = ready.exists();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(stored, "subscriber storage was not reached");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let tenant: OpaqueRef = "tenant-a".to_owned().try_into().unwrap();
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    let delivery = state.deliveries.values().next().unwrap();
    let id = delivery.id.clone();
    assert_eq!(std::fs::read_to_string(ready.with_extension("record")).unwrap(), id.as_str());
    assert_eq!(delivery.attempts.len(), 1);
    let binding = state.bindings.values().next().unwrap().clone();
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), lease_timeout_secs: 3, ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let service = IngressRuntime::new(bundle.wi(), tenant.clone());
    let task = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
    let run = aq.projection().run_ids_for_task(task)[0];
    // AQ may still be in its lease-recovery retry wait while other real-AQ
    // crash cases run in the same test binary. Poll for the actual restored
    // acknowledgment wait rather than assuming it appears within one lease.
    let wait_deadline = std::time::Instant::now() + Duration::from_secs(25);
    while aq.get_run(run).unwrap().state != actionqueue_core::run::state::RunState::Awaiting
        && std::time::Instant::now() < wait_deadline
    {
        let _ = aq.run_until_idle().await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    // The retry can become due while AQ restores the wait. Either it has
    // already redelivered once or the loop below will drive that transition.
    assert!(recorder.0.lock().unwrap().len() <= 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(16);
    while recorder.0.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap();
        let _ = aq.run_until_idle().await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].attempts.len(), 2);
    aq.shutdown().unwrap();
}

#[tokio::test]
async fn terminal_wait_capacity_recovers_from_journaled_generation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup(&path);
    let recorder = Arc::new(Recorder::default());
    let service = IngressRuntime::new(bundle.wi(), binding.profile.scope.tenant.clone());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 0, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    service
        .dispatch_pending(
            &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
            10,
            now(),
        )
        .unwrap();
    let _ = aq.run_until_idle().await.unwrap();
    let original = TaskId::from_uuid(uuid::Uuid::parse_str(id.as_str()).unwrap());
    let run = aq.projection().run_ids_for_task(original)[0];
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Failed);
    aq.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    assert_eq!(
        service
            .dispatch_pending(
                &mut BoundAqIngress::new(&mut aq, binding.profile.scope.tenant.clone()),
                10,
                now(),
            )
            .unwrap(),
        1
    );
    let _ = aq.run_until_idle().await.unwrap();
    assert_eq!(recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    let queue = &bundle.wi().ingress_snapshot(&binding.profile.scope.tenant).unwrap().queue[&id];
    assert_eq!(queue.generation, 1);
    assert_eq!(queue.prior.len(), 1);
    assert_eq!(queue.prior[0].generation, 0);
    aq.shutdown().unwrap();
    drop(service);
    drop(bundle);
    StoreBundle::open(&path).unwrap();
}

// Reproduce a paused generation-0 dispatch completion after another dispatcher
// records that completion, observes terminal AQ failure, and journals generation 1.
struct PauseRecoveryWake<'a> {
    aq: &'a mut actionqueue_runtime::engine::BootstrappedEngine<IngressDeliveryHandler>,
    tenant: OpaqueRef,
}
impl worldinterface_runtime::ingress::AqIngressPort for PauseRecoveryWake<'_> {
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn ensure_delivery_task(
        &mut self,
        r: actionqueue_core::admission::EnsureTaskRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::AdmissionOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        BoundAqIngress::new(self.aq, self.tenant.clone()).ensure_delivery_task(r)
    }
    fn admit_wake(
        &mut self,
        _: actionqueue_core::continuation::AdmitSignalRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::WakeOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        Err(worldinterface_runtime::ingress::AqDispatchFailure::Uncertain)
    }
    fn delivery_task_terminal(
        &mut self,
        id: TaskId,
    ) -> std::result::Result<bool, worldinterface_runtime::ingress::AqDispatchFailure> {
        BoundAqIngress::new(self.aq, self.tenant.clone()).delivery_task_terminal(id)
    }
}
struct DelayedOldCompletion {
    aq: Arc<Mutex<actionqueue_runtime::engine::BootstrappedEngine<IngressDeliveryHandler>>>,
    tenant: OpaqueRef,
    ready: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
    pause: bool,
}
struct DelayedOldTaskCompletion {
    aq: Arc<Mutex<actionqueue_runtime::engine::BootstrappedEngine<IngressDeliveryHandler>>>,
    tenant: OpaqueRef,
    ready: std::sync::mpsc::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
    pause: bool,
}
impl worldinterface_runtime::ingress::AqIngressPort for DelayedOldTaskCompletion {
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn ensure_delivery_task(
        &mut self,
        r: actionqueue_core::admission::EnsureTaskRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::AdmissionOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        let old = BoundAqIngress::new(&mut *self.aq.lock().unwrap(), self.tenant.clone())
            .ensure_delivery_task(r)?;
        if self.pause {
            self.pause = false;
            self.ready.send(()).unwrap();
            self.resume.recv_timeout(Duration::from_secs(20)).unwrap();
        }
        Ok(old)
    }
    fn admit_wake(
        &mut self,
        r: actionqueue_core::continuation::AdmitSignalRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::WakeOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        BoundAqIngress::new(&mut *self.aq.lock().unwrap(), self.tenant.clone()).admit_wake(r)
    }
}
impl worldinterface_runtime::ingress::AqIngressPort for DelayedOldCompletion {
    fn bound_tenant(&self) -> &OpaqueRef {
        &self.tenant
    }
    fn ensure_delivery_task(
        &mut self,
        r: actionqueue_core::admission::EnsureTaskRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::AdmissionOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        BoundAqIngress::new(&mut *self.aq.lock().unwrap(), self.tenant.clone())
            .ensure_delivery_task(r)
    }
    fn admit_wake(
        &mut self,
        r: actionqueue_core::continuation::AdmitSignalRequest,
    ) -> std::result::Result<
        worldinterface_runtime::ingress::WakeOutcome,
        worldinterface_runtime::ingress::AqDispatchFailure,
    > {
        let old = BoundAqIngress::new(&mut *self.aq.lock().unwrap(), self.tenant.clone())
            .admit_wake(r)?;
        if self.pause {
            self.pause = false;
            self.ready.send(()).unwrap();
            self.resume.recv_timeout(Duration::from_secs(20)).unwrap();
        }
        Ok(old)
    }
}
#[test]
fn g3_paused_old_dispatch_ack_cannot_complete_replacement_generation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup(&path);
    let tenant = binding.profile.scope.tenant.clone();
    let recorder = Arc::new(Recorder::default());
    let aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 0, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let aq = Arc::new(Mutex::new(aq));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (worker_bundle, worker_tenant, worker_aq) = (bundle.clone(), tenant.clone(), aq.clone());
    let worker = std::thread::spawn(move || {
        IngressRuntime::new(worker_bundle.wi(), worker_tenant.clone()).dispatch_pending(
            &mut DelayedOldCompletion {
                aq: worker_aq,
                tenant: worker_tenant,
                ready: ready_tx,
                resume: resume_rx,
                pause: true,
            },
            10,
            now(),
        )
    });
    ready_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    {
        let mut engine = aq.lock().unwrap();
        let service = IngressRuntime::new(bundle.wi(), tenant.clone());
        assert_eq!(
            service
                .dispatch_pending(&mut BoundAqIngress::new(&mut *engine, tenant.clone()), 10, now())
                .unwrap(),
            1
        );
        let _ = rt.block_on(engine.run_until_idle()).unwrap();
        assert!(service
            .dispatch_pending(
                &mut PauseRecoveryWake { aq: &mut engine, tenant: tenant.clone() },
                10,
                now()
            )
            .is_err());
        let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
        assert_eq!(state.queue[&id].generation, 1);
        assert!(state.queue[&id].task_admitted);
        assert!(state.queue[&id].signal_sequence.is_none());
    }
    resume_tx.send(()).unwrap();
    let result = worker.join().unwrap();
    let aq = Arc::try_unwrap(aq).ok().unwrap().into_inner().unwrap();
    assert_eq!(result.unwrap(), 1);
    let state = bundle.wi().ingress_snapshot(&tenant).unwrap();
    let q = &state.queue[&id];
    assert_eq!(q.generation, 1);
    assert!(q.signal_sequence.is_some());
    assert_eq!(q.prior[0].signal_sequence, Some(1));
    let task: actionqueue_core::admission::EnsureTaskRequest =
        serde_json::from_slice(&q.task_request).unwrap();
    aq.shutdown().unwrap();
    drop(state);
    drop(bundle);
    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let count = IngressRuntime::new(bundle.wi(), tenant.clone())
        .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
        .unwrap();
    let _ = rt.block_on(aq.run_until_idle()).unwrap();
    let run = aq.projection().run_ids_for_task(task.task_spec().id())[0];
    let recovered_state = aq.get_run(run).unwrap().state;
    assert_eq!(count, 0);
    assert_eq!(recovered_state, actionqueue_core::run::state::RunState::Awaiting);
    assert_eq!(recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    aq.shutdown().unwrap();
    drop(bundle);
    StoreBundle::open(&path).unwrap();
}

#[test]
fn paused_old_task_ack_cannot_complete_replacement_generation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let (bundle, binding, id) = setup(&path);
    let tenant = binding.profile.scope.tenant.clone();
    let recorder = Arc::new(Recorder::default());
    let aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            continuation_limits: ContinuationLimits { active_waits: 0, ..Default::default() },
            ..Default::default()
        },
        handler(&bundle, &binding, recorder),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let aq = Arc::new(Mutex::new(aq));
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (worker_bundle, worker_tenant, worker_aq) = (bundle.clone(), tenant.clone(), aq.clone());
    let worker = std::thread::spawn(move || {
        IngressRuntime::new(worker_bundle.wi(), worker_tenant.clone()).dispatch_pending(
            &mut DelayedOldTaskCompletion {
                aq: worker_aq,
                tenant: worker_tenant,
                ready: ready_tx,
                resume: resume_rx,
                pause: true,
            },
            10,
            now(),
        )
    });
    ready_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    {
        let mut engine = aq.lock().unwrap();
        let service = IngressRuntime::new(bundle.wi(), tenant.clone());
        assert_eq!(
            service
                .dispatch_pending(&mut BoundAqIngress::new(&mut *engine, tenant.clone()), 10, now())
                .unwrap(),
            1
        );
        let _ = tokio::runtime::Runtime::new().unwrap().block_on(engine.run_until_idle()).unwrap();
        assert!(service
            .dispatch_pending(
                &mut PauseRecoveryWake { aq: &mut engine, tenant: tenant.clone() },
                10,
                now()
            )
            .is_err());
        let q = &bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id];
        assert_eq!(q.generation, 1);
        assert!(q.task_admitted);
        assert!(q.signal_sequence.is_none());
    }
    resume_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap().unwrap(), 1);
    Arc::try_unwrap(aq).ok().unwrap().into_inner().unwrap().shutdown().unwrap();
    let q = bundle.wi().ingress_snapshot(&tenant).unwrap().queue[&id].clone();
    assert_eq!(q.generation, 1);
    assert!(q.signal_sequence.is_some());
    drop(bundle);
    let bundle = Arc::new(StoreBundle::open(&path).unwrap());
    let recorder = Arc::new(Recorder::default());
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler(&bundle, &binding, recorder.clone()),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ = tokio::runtime::Runtime::new().unwrap().block_on(aq.run_until_idle()).unwrap();
    assert_eq!(recorder.0.lock().unwrap().as_slice(), std::slice::from_ref(&id));
    aq.shutdown().unwrap();
    drop(bundle);
    StoreBundle::open(&path).unwrap();
}
