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
    ingress::{BoundAqIngress, IngressDeliveryHandler, IngressRuntime, SubscriberTransport},
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
    binding.expires_at_ms = current + 1_000_000;
    binding.policy.retention_ms = 1_000_000;
    binding.policy.acknowledgment_timeout_ms = 1_000;
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
    let event = ProviderEvent {
        scope: binding.profile.scope.clone(),
        event_id: Some("recovery-event".to_owned().try_into().unwrap()),
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
struct Recorder(Mutex<Vec<DeliveryId>>);
impl SubscriberTransport for Recorder {
    fn deliver(
        &self,
        delivery: &worldinterface_ingress::IssuedDelivery,
    ) -> worldinterface_runtime::Result<()> {
        self.0.lock().unwrap().push(delivery.view.id.clone());
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
        let (bundle, binding, _) = setup(Path::new(&path));
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
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while recorder.0.lock().unwrap().is_empty() && std::time::Instant::now() < deadline {
        service
            .dispatch_pending(&mut BoundAqIngress::new(&mut aq, tenant.clone()), 10, now())
            .unwrap();
        let _ = aq.run_until_idle().await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(recorder.0.lock().unwrap().as_slice(), &[id]);
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
