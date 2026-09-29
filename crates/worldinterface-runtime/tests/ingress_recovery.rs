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
    setup_with_timeout(path, 1_000)
}
fn setup_with_timeout(
    path: &Path,
    acknowledgment_timeout_ms: u64,
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
    binding.expires_at_ms = current + 1_000_000;
    binding.policy.retention_ms = 1_000_000;
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
    let wait_deadline = std::time::Instant::now() + Duration::from_secs(8);
    while aq.get_run(run).unwrap().state != actionqueue_core::run::state::RunState::Awaiting
        && std::time::Instant::now() < wait_deadline
    {
        let _ = aq.run_until_idle().await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(aq.get_run(run).unwrap().state, actionqueue_core::run::state::RunState::Awaiting);
    assert!(recorder.0.lock().unwrap().is_empty());
    assert_eq!(bundle.wi().ingress_snapshot(&tenant).unwrap().deliveries[&id].attempts.len(), 1);
    let deadline = std::time::Instant::now() + Duration::from_secs(16);
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
