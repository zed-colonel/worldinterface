use worldinterface_runtime::connector::{
    Connector as EffectExecutionPort, PreparedInvocation as BoundExecution,
};
#[path = "support/child.rs"]
mod child_guard;
#[path = "support/spi_fixture.rs"]
mod spi_fixture;
use actionqueue_core::{
    bounded::OpaqueRef as AqRef,
    causal::ControlMutationContext,
    control::{ControlScope, HostControlContext},
    data_ref::DataRef,
    disposition::DispositionOutcome,
    ids::{AttemptId, RunId},
};
use actionqueue_executor_local::handler::{
    AttemptMetadata, CancellationContext, CancellationToken, ExecutorContext, HandlerInput,
};
use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
use child_guard::KillOnDrop;
use std::{
    fs,
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
    time::{Duration, Instant},
};
use worldinterface_domain::{
    agency::AuthenticatedCaller,
    authorization::{AuthorizationSnapshot, VerifiedSnapshot},
    effect::{CommitObservation, EffectAction},
};
use worldinterface_egress::Egress;
use worldinterface_protocol::{
    ContentHash, EffectIntentId, EffectSubmission, EvidenceRef, ExecutionOutcome, InstanceId,
    OpaqueRef, Timestamp,
};
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::{EvidenceAuthority, TrustedClock},
    completion::{EffectAttemptHandler, EvidenceBytes, PortObservation},
    run_discovery,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::{StoreProfile, StoreRoot};

fn at() -> Timestamp {
    "2026-09-21T00:00:00.000000000Z".to_owned().try_into().unwrap()
}
fn fixture() -> (EffectSubmission, AuthorizationSnapshot) {
    let data: serde_json::Value =
        serde_json::from_str(include_str!("../../../conformance/wi-fabric-2/egress/fixture.json"))
            .unwrap();
    (
        serde_json::from_value(data["submission"].clone()).unwrap(),
        spi_fixture::pin(serde_json::from_value(data["snapshot"].clone()).unwrap()),
    )
}
struct Clock;
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        Ok(at())
    }
}
struct Evidence(AuthorizationSnapshot);
impl EvidenceAuthority for Evidence {
    fn current_snapshot(
        &self,
        _: &OpaqueRef,
        _: &EffectSubmission,
    ) -> Result<AuthorizationSnapshot> {
        Ok(self.0.clone())
    }
    fn verify(&self, _: &OpaqueRef, reference: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        if reference == &self.0.facts[0].evidence {
            Ok(self.0.facts[0].attested_document.clone())
        } else {
            Ok(ContentHash::of(b"verified"))
        }
    }
}
fn publish_inputs(root: &StoreRoot, tenant: &OpaqueRef) {
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
        root.publish(tenant.clone(), name.as_bytes()).unwrap();
    }
}
fn host() -> HostControlContext {
    HostControlContext {
        actor_id: None,
        scope: ControlScope::SingleTenant,
        attribution: ControlMutationContext::new(AqRef::new("trusted-host").unwrap()),
    }
}
#[derive(Clone)]
struct AqClock(Arc<AtomicU64>);
impl actionqueue_core::time::clock::Clock for AqClock {
    fn now(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
struct SignalAfterDisposition<H> {
    inner: H,
    sent: tokio::sync::mpsc::UnboundedSender<()>,
}
struct CrashBeforeDisposition<H>(H);
impl<H: actionqueue_executor_local::handler::ExecutorHandler>
    actionqueue_executor_local::handler::ExecutorHandler for CrashBeforeDisposition<H>
{
    fn execute(&self, ctx: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        let disposition = self.0.execute(ctx);
        barrier("before-aq-disposition");
        disposition
    }
}
impl<H: actionqueue_executor_local::handler::ExecutorHandler>
    actionqueue_executor_local::handler::ExecutorHandler for SignalAfterDisposition<H>
{
    fn execute(&self, ctx: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        let disposition = self.inner.execute(ctx);
        let _ = self.sent.send(());
        disposition
    }
}
struct Noop;
impl actionqueue_executor_local::handler::ExecutorHandler for Noop {
    fn execute(&self, _: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        actionqueue_core::disposition::AttemptDisposition::complete(None)
    }
}
fn barrier(name: &str) {
    if std::env::var("WI_PORT_CRASH_POINT").as_deref() == Ok(name) {
        fs::write(std::env::var_os("WI_CRASH_READY").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
struct MockSuccessfulPort {
    count: std::path::PathBuf,
}
struct HeldResultPort {
    count: std::path::PathBuf,
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<Barrier>,
}
impl EffectExecutionPort for HeldResultPort {
    fn descriptor(&self) -> &worldinterface_protocol::ConnectorDescriptor {
        spi_fixture::descriptor()
    }
    fn resolve(
        &self,
        r: worldinterface_runtime::connector::ResolveRequest,
    ) -> Result<worldinterface_runtime::connector::ResolveObservation> {
        Ok(worldinterface_runtime::connector::ResolveObservation { resource: r.resource })
    }
    fn prepare(
        &self,
        _: &worldinterface_runtime::connector::AuthorizedInvocation,
    ) -> worldinterface_runtime::connector::PrepareOutcome {
        worldinterface_runtime::connector::PrepareOutcome::Ready
    }
    fn reconcile(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::ReconcileOutcome> {
        Ok(worldinterface_runtime::connector::ReconcileOutcome::Unknown(vec![]))
    }
    fn cancel(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::CancelObservation> {
        Ok(worldinterface_runtime::connector::CancelObservation::Unsupported)
    }

    fn invoke(
        &self,
        request: BoundExecution,
        cancellation: &CancellationToken,
    ) -> Result<PortObservation> {
        let observed =
            MockSuccessfulPort { count: self.count.clone() }.invoke(request, cancellation)?;
        self.entered.send(()).unwrap();
        self.release.wait();
        Ok(observed)
    }
}
impl EffectExecutionPort for MockSuccessfulPort {
    fn descriptor(&self) -> &worldinterface_protocol::ConnectorDescriptor {
        spi_fixture::descriptor()
    }
    fn resolve(
        &self,
        r: worldinterface_runtime::connector::ResolveRequest,
    ) -> Result<worldinterface_runtime::connector::ResolveObservation> {
        Ok(worldinterface_runtime::connector::ResolveObservation { resource: r.resource })
    }
    fn prepare(
        &self,
        _: &worldinterface_runtime::connector::AuthorizedInvocation,
    ) -> worldinterface_runtime::connector::PrepareOutcome {
        worldinterface_runtime::connector::PrepareOutcome::Ready
    }
    fn reconcile(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::ReconcileOutcome> {
        Ok(worldinterface_runtime::connector::ReconcileOutcome::Unknown(vec![]))
    }
    fn cancel(
        &self,
        _: worldinterface_runtime::connector::ReconcileRequest,
    ) -> Result<worldinterface_runtime::connector::CancelObservation> {
        Ok(worldinterface_runtime::connector::CancelObservation::Unsupported)
    }

    fn invoke(&self, request: BoundExecution, _: &CancellationToken) -> Result<PortObservation> {
        assert_eq!(request.resolved.intent_id, request.intent);
        assert!(!request.idempotency.as_str().is_empty());
        assert!(!request.execution_binding.as_str().is_empty());
        barrier("before-port");
        let mut file = fs::OpenOptions::new().append(true).create(true).open(&self.count)?;
        file.write_all(b"1")?;
        file.sync_all()?;
        barrier("after-port");
        Ok(PortObservation {
            commit: CommitObservation::ConfirmedCommitted,
            outcome: ExecutionOutcome::Succeeded,
            outputs: vec![b"output".to_vec()],
            evidence: vec![EvidenceBytes {
                schema: "mock-execution:1".to_owned().try_into()?,
                bytes: b"executed".to_vec(),
            }],
            boundary_record: Some(EvidenceBytes {
                schema: "mock-boundary:1".to_owned().try_into()?,
                bytes: b"provider assertion".to_vec(),
            }),
        })
    }
}
fn context(
    root: &StoreRoot,
    tenant: &OpaqueRef,
    id: &EffectIntentId,
    run: RunId,
    attempt: AttemptId,
) -> ExecutorContext {
    let state = root.effect_snapshot(tenant).unwrap();
    let admission = state.effects[id].dispatches.last().unwrap().admission.as_ref().unwrap();
    let request = admission::load_request(root, admission).unwrap();
    ExecutorContext {
        input: HandlerInput {
            resume_context: None,
            causal_context: None,
            run_id: run,
            attempt_id: attempt,
            payload: request.task_spec().task_payload().bytes().to_vec(),
            metadata: AttemptMetadata {
                failure_attempt_count: 0,
                max_attempts: 3,
                attempt_number: 1,
                timeout_secs: Some(30),
                safety_level: actionqueue_core::task::safety::SafetyLevel::Idempotent,
            },
            cancellation_context: CancellationContext::new(),
        },
        children: None,
    }
}
fn invoke(
    path: &Path,
    count: &Path,
    run: RunId,
    attempt: AttemptId,
) -> Result<actionqueue_core::disposition::AttemptDisposition> {
    let bundle = Arc::new(StoreBundle::open(path)?);
    let (_, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    let id = bundle.wi().effect_snapshot(&tenant)?.effects.keys().next().unwrap().clone();
    let ctx = context(bundle.wi(), &tenant, &id, run, attempt);
    let handler = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: MockSuccessfulPort { count: count.to_path_buf() },
        evidence: Evidence(snapshot),
        clock: Clock,
    };
    handler.execute_bound(&ctx)
}

#[test]
fn completion_kill_matrix() {
    if let Some(path) = std::env::var_os("WI_COMPLETION_CHILD_BUNDLE") {
        let count = std::path::PathBuf::from(std::env::var_os("WI_COMPLETION_COUNT").unwrap());
        let run: RunId = std::env::var("WI_COMPLETION_RUN").unwrap().parse().unwrap();
        let attempt: AttemptId = std::env::var("WI_COMPLETION_ATTEMPT").unwrap().parse().unwrap();
        invoke(Path::new(&path), &count, run, attempt).unwrap();
        return;
    }
    for point in [
        "before-port",
        "after-port",
        "publication-identified",
        "artifact-durable",
        "publication-durable",
        "publication-availability",
        "fabric-receipt-history",
        "fabric-receipt-state",
        "fabric-receipt-outbox",
        "fabric-receipt-index",
        "fabric-receipt-committed",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bundle");
        let count = temp.path().join("port-count");
        let ready = temp.path().join("ready");
        let bundle = StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        let (submission, snapshot) = fixture();
        let tenant = snapshot.bindings[0].tenant.clone();
        publish_inputs(bundle.wi(), &tenant);
        let caller = AuthenticatedCaller::from_trusted_authentication(
            tenant.clone(),
            snapshot.bindings[0].principal.clone(),
        );
        let egress = Egress::new(bundle.wi());
        let id = egress
            .apply(
                &caller,
                at(),
                EffectAction::Submit {
                    id: EffectIntentId::generate(),
                    submission: Box::new(submission),
                    snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                    link: None,
                },
            )
            .unwrap();
        for action in [
            EffectAction::Resolve {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            },
            EffectAction::Authorize {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            },
        ] {
            egress.apply(&caller, at(), action).unwrap();
        }
        let mut queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
        admission::prepare(
            &bundle,
            &caller,
            at(),
            &id,
            VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        )
        .unwrap();
        admission::deliver(
            &mut queue,
            &caller,
            at(),
            &id,
            VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        )
        .unwrap();
        assert!(run_discovery::discover(&queue, &caller, at(), &id, 8).unwrap().complete);
        let state = bundle.wi().effect_snapshot(&tenant).unwrap();
        let run: RunId = state.effects[&id].dispatches[0].admission.as_ref().unwrap().observed_runs
            [0]
        .as_str()
        .parse()
        .unwrap();
        queue.shutdown().unwrap();
        drop(bundle);
        // Retain a reader opened before the child, so visibility is tested before
        // reopen recovery resumes any known publication obligation.
        let observer = StoreRoot::open_existing(path.join("wi")).unwrap();
        let attempt = AttemptId::new();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "completion_kill_matrix", "--nocapture"])
            .env("WI_COMPLETION_CHILD_BUNDLE", &path)
            .env("WI_COMPLETION_COUNT", &count)
            .env("WI_COMPLETION_RUN", run.to_string())
            .env("WI_COMPLETION_ATTEMPT", attempt.to_string())
            .env("WI_CRASH_READY", &ready)
            .env(
                if point.starts_with("fabric-")
                    || point.starts_with("publication-")
                    || point == "artifact-durable"
                {
                    "WI_CRASH_POINT"
                } else {
                    "WI_PORT_CRASH_POINT"
                },
                point,
            )
            .stdout(std::process::Stdio::null())
            .spawn()
            .map(KillOnDrop)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while !ready.exists() && Instant::now() < deadline {
            assert!(child.try_wait().unwrap().is_none(), "child exited before {point}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(ready.exists(), "child did not reach {point}");
        child.kill().unwrap();
        child.wait().unwrap();
        if !point.starts_with("fabric-") {
            let output = worldinterface_protocol::ArtifactRef {
                scope: tenant.clone(),
                content: ContentHash::of(b"output"),
                size: 6,
            };
            assert!(
                observer.read_artifact(&output).is_err(),
                "incomplete output visible at {point}"
            );
        }
        assert_eq!(
            observer.effect_receipts(&tenant, &id).unwrap().len(),
            usize::from(point == "fabric-receipt-committed")
        );
        drop(observer);
        let disposition = invoke(&path, &count, run, AttemptId::new()).unwrap();
        let reopened = Arc::new(StoreBundle::open(&path).unwrap());
        let state = reopened.wi().effect_snapshot(&tenant).unwrap();
        let outcome = state.effects[&id].outcomes.last().unwrap();
        let receipts = reopened.wi().effect_receipts(&tenant, &id).unwrap();
        assert_eq!(receipts.len(), 1, "{point}");
        assert_eq!(
            fs::read(&count).map(|b| b.len()).unwrap_or(0),
            usize::from(point != "before-port"),
            "{point}"
        );
        if point == "fabric-receipt-committed" {
            assert_eq!(outcome.outcome, ExecutionOutcome::Succeeded);
            assert!(matches!(disposition.outcome(), DispositionOutcome::Complete));
            let Some(DataRef::External(reference)) = disposition.output() else {
                panic!("missing completion locator");
            };
            let (_, hash) = reference.locator.expose().rsplit_once('/').unwrap();
            let artifact = worldinterface_protocol::ArtifactRef {
                scope: tenant.clone(),
                content: hash.to_owned().try_into().unwrap(),
                size: reference.size_bytes.unwrap(),
            };
            let bytes = reopened.wi().read_artifact(&artifact).unwrap();
            let locator: worldinterface_runtime::completion::CompletionLocator =
                serde_json::from_slice(&bytes).unwrap();
            assert_eq!(locator.receipt, receipts[0].0);
            assert_eq!(locator.receipt_digest, receipts[0].1);
        } else {
            assert_eq!(outcome.outcome, ExecutionOutcome::Uncertain, "{point}");
            assert!(matches!(disposition.outcome(), DispositionOutcome::TerminalFailure { .. }));
        }
        let handler = EffectAttemptHandler {
            bundle: reopened.clone(),
            port: MockSuccessfulPort { count: count.clone() },
            evidence: Evidence(snapshot),
            clock: Clock,
        };
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig { data_dir: reopened.aq_path(), ..Default::default() },
            handler,
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let _ = tokio::time::timeout(Duration::from_secs(20), aq.run_until_idle())
                .await
                .unwrap()
                .unwrap();
        });
        let expected = if point == "fabric-receipt-committed" {
            actionqueue_core::run::state::RunState::Completed
        } else {
            actionqueue_core::run::state::RunState::Failed
        };
        assert_eq!(aq.projection().get_run_state(&run), Some(&expected), "{point}");
        aq.shutdown().unwrap();
        let references =
            worldinterface_runtime::aq_adapter::QueueReferences::open(&reopened.aq_path()).unwrap();
        if expected == actionqueue_core::run::state::RunState::Failed {
            assert_eq!(
                references.terminal_failure_code(&run.to_string()).unwrap().as_deref(),
                Some("wi.effect.uncertain"),
                "{point}"
            );
        }
        assert_eq!(
            fs::read(&count).map(|b| b.len()).unwrap_or(0),
            usize::from(point != "before-port"),
            "{point}"
        );
        drop(references);
        drop(reopened);
        assert!(StoreBundle::open(&path).unwrap().convergence_faults().is_empty());
    }
}

#[tokio::test]
async fn aq_completion_exposes_only_published_locator() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let count = temp.path().join("port-count");
    let bundle = Arc::new(
        StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap(),
    );
    let (submission, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    publish_inputs(bundle.wi(), &tenant);
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &caller,
            at(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                link: None,
            },
        )
        .unwrap();
    for action in [
        EffectAction::Resolve {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
        EffectAction::Authorize {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
    ] {
        egress.apply(&caller, at(), action).unwrap();
    }
    let mut admission_queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
    admission::prepare(
        &bundle,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    admission::deliver(
        &mut admission_queue,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    assert!(run_discovery::discover(&admission_queue, &caller, at(), &id, 8).unwrap().complete);
    let run: RunId = bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    admission_queue.shutdown().unwrap();
    let handler = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: MockSuccessfulPort { count: count.clone() },
        evidence: Evidence(snapshot),
        clock: Clock,
    };
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ =
        tokio::time::timeout(Duration::from_secs(20), aq.run_until_idle()).await.unwrap().unwrap();
    assert_eq!(
        aq.projection().get_run_state(&run),
        Some(&actionqueue_core::run::state::RunState::Completed)
    );
    aq.shutdown().unwrap();
    assert_eq!(fs::read(&count).unwrap().len(), 1);
    let reopened = StoreBundle::open(&path).unwrap();
    assert_eq!(reopened.wi().effect_receipts(&tenant, &id).unwrap().len(), 1);
    assert!(reopened.convergence_faults().is_empty());
    let state = reopened.wi().effect_snapshot(&tenant).unwrap();
    let output = state.effects[&id].outcomes[0].durable_outputs[0].clone();
    fs::remove_file(path.join("wi/artifacts").join(output.content.as_str())).unwrap();
    let reopened = Arc::new(reopened);
    let ctx = context(reopened.wi(), &tenant, &id, run, AttemptId::new());
    let handler = EffectAttemptHandler {
        bundle: reopened.clone(),
        port: MockSuccessfulPort { count: count.clone() },
        evidence: Evidence(fixture().1),
        clock: Clock,
    };
    assert!(
        handler.execute_bound(&ctx).is_err(),
        "committed success must recheck required publications"
    );
    assert_eq!(fs::read(&count).unwrap().len(), 1);
    assert_eq!(reopened.wi().effect_receipts(&tenant, &id).unwrap().len(), 1);
}

#[tokio::test]
async fn three_expired_deliveries_record_convergence_fault_without_second_port_call() {
    use actionqueue_runtime::config::BackoffStrategyConfig;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let count = temp.path().join("port-count");
    let bundle = Arc::new(
        StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap(),
    );
    let (submission, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    publish_inputs(bundle.wi(), &tenant);
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &caller,
            at(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                link: None,
            },
        )
        .unwrap();
    for action in [
        EffectAction::Resolve {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
        EffectAction::Authorize {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
    ] {
        egress.apply(&caller, at(), action).unwrap();
    }
    let mut admission_queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
    admission::prepare(
        &bundle,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    admission::deliver(
        &mut admission_queue,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    assert!(run_discovery::discover(&admission_queue, &caller, at(), &id, 8).unwrap().complete);
    let run: RunId = bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    admission_queue.shutdown().unwrap();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    let handler = SignalAfterDisposition {
        inner: EffectAttemptHandler {
            bundle: bundle.clone(),
            port: MockSuccessfulPort { count: count.clone() },
            evidence: Evidence(snapshot),
            clock: Clock,
        },
        sent,
    };
    let start =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() + 1;
    let clock = AqClock(Arc::new(AtomicU64::new(start)));
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: bundle.aq_path(),
            lease_timeout_secs: 3,
            dispatch_concurrency: std::num::NonZeroUsize::new(1).unwrap(),
            backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
            ..Default::default()
        },
        handler,
    )
    .bootstrap_with_clock(clock.clone())
    .unwrap()
    .with_host(host());
    assert_eq!(aq.tick().await.unwrap().dispatched, 1);
    tokio::time::timeout(Duration::from_secs(10), received.recv()).await.unwrap().unwrap();
    for expired in 1..=3 {
        clock.0.store(start + expired * 4, Ordering::SeqCst);
        let retire = aq.tick().await.unwrap();
        if expired < 3 {
            assert_eq!(retire.dispatched, 1);
            tokio::time::timeout(Duration::from_secs(10), received.recv()).await.unwrap().unwrap();
        }
    }
    let run_state = aq.projection().get_run_instance(&run).unwrap();
    assert_eq!(run_state.state(), actionqueue_core::run::state::RunState::Failed);
    assert_eq!(run_state.attempt_count(), 3);
    assert_eq!(run_state.failure_attempt_count(), 3);
    aq.shutdown().unwrap();
    assert_eq!(fs::read(&count).unwrap().len(), 1);
    drop(bundle);
    let reopened = StoreBundle::open(&path).unwrap();
    assert_eq!(reopened.wi().effect_receipts(&tenant, &id).unwrap().len(), 1);
    assert_eq!(reopened.convergence_faults().len(), 1);
    assert_eq!(reopened.convergence_faults()[0].fabric, ExecutionOutcome::Succeeded);
    assert_eq!(
        reopened.convergence_faults()[0].queue,
        actionqueue_core::run::state::RunState::Failed
    );
}

#[test]
fn aq_disposition_process_kills_replay_the_original_locator() {
    if let Some(path) = std::env::var_os("WI_AQ_CHILD_BUNDLE") {
        let bundle = Arc::new(StoreBundle::open(Path::new(&path)).unwrap());
        let (_, snapshot) = fixture();
        let count = std::path::PathBuf::from(std::env::var_os("WI_COMPLETION_COUNT").unwrap());
        let handler = CrashBeforeDisposition(EffectAttemptHandler {
            bundle: bundle.clone(),
            port: MockSuccessfulPort { count },
            evidence: Evidence(snapshot),
            clock: Clock,
        });
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
            handler,
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let _ = tokio::time::timeout(Duration::from_secs(20), aq.run_until_idle())
                .await
                .unwrap()
                .unwrap();
        });
        barrier("after-aq-disposition");
        aq.shutdown().unwrap();
        return;
    }
    for point in ["before-aq-disposition", "after-aq-disposition"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bundle");
        let count = temp.path().join("port-count");
        let ready = temp.path().join("ready");
        let bundle = StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap();
        let (submission, snapshot) = fixture();
        let tenant = snapshot.bindings[0].tenant.clone();
        publish_inputs(bundle.wi(), &tenant);
        let caller = AuthenticatedCaller::from_trusted_authentication(
            tenant.clone(),
            snapshot.bindings[0].principal.clone(),
        );
        let egress = Egress::new(bundle.wi());
        let id = egress
            .apply(
                &caller,
                at(),
                EffectAction::Submit {
                    id: EffectIntentId::generate(),
                    submission: Box::new(submission),
                    snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                    link: None,
                },
            )
            .unwrap();
        for action in [
            EffectAction::Resolve {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            },
            EffectAction::Authorize {
                id: id.clone(),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
            },
        ] {
            egress.apply(&caller, at(), action).unwrap();
        }
        let mut admission_queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
        admission::prepare(
            &bundle,
            &caller,
            at(),
            &id,
            VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        )
        .unwrap();
        admission::deliver(
            &mut admission_queue,
            &caller,
            at(),
            &id,
            VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        )
        .unwrap();
        assert!(run_discovery::discover(&admission_queue, &caller, at(), &id, 8).unwrap().complete);
        let run: RunId = bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id].dispatches[0]
            .admission
            .as_ref()
            .unwrap()
            .observed_runs[0]
            .as_str()
            .parse()
            .unwrap();
        admission_queue.shutdown().unwrap();
        drop(bundle);
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "aq_disposition_process_kills_replay_the_original_locator",
                "--nocapture",
            ])
            .env("WI_AQ_CHILD_BUNDLE", &path)
            .env("WI_COMPLETION_COUNT", &count)
            .env("WI_PORT_CRASH_POINT", point)
            .env("WI_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .map(KillOnDrop)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready.exists() && Instant::now() < deadline {
            assert!(child.try_wait().unwrap().is_none(), "child exited before {point}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(ready.exists(), "child missed {point}");
        child.kill().unwrap();
        child.wait().unwrap();
        let bundle = Arc::new(StoreBundle::open(&path).unwrap());
        let handler = EffectAttemptHandler {
            bundle: bundle.clone(),
            port: MockSuccessfulPort { count: count.clone() },
            evidence: Evidence(snapshot),
            clock: Clock,
        };
        let now =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let retry_clock = AqClock(Arc::new(AtomicU64::new(now + 10)));
        let mut aq = ActionQueueEngine::new(
            RuntimeConfig {
                data_dir: bundle.aq_path(),
                backoff_strategy: actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
                    interval: Duration::ZERO,
                },
                ..Default::default()
            },
            handler,
        )
        .bootstrap_with_clock(retry_clock.clone())
        .unwrap()
        .with_host(host());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            for _ in 0..4 {
                let _ = tokio::time::timeout(Duration::from_secs(30), aq.run_until_idle())
                    .await
                    .unwrap()
                    .unwrap();
                if aq.projection().get_run_state(&run)
                    == Some(&actionqueue_core::run::state::RunState::Completed)
                {
                    break;
                }
                retry_clock.0.fetch_add(10, Ordering::SeqCst);
            }
        });
        assert_eq!(
            aq.projection().get_run_state(&run),
            Some(&actionqueue_core::run::state::RunState::Completed),
            "{point}"
        );
        aq.shutdown().unwrap();
        assert_eq!(fs::read(&count).unwrap().len(), 1, "{point}");
        drop(bundle);
        let reopened = StoreBundle::open(&path).unwrap();
        assert_eq!(reopened.wi().effect_receipts(&tenant, &id).unwrap().len(), 1, "{point}");
        assert!(reopened.convergence_faults().is_empty(), "{point}");
    }
}

#[test]
fn overlapping_delivery_preserves_uncertainty_and_single_port_call() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let count = temp.path().join("port-count");
    let bundle = Arc::new(
        StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap(),
    );
    let (submission, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    publish_inputs(bundle.wi(), &tenant);
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &caller,
            at(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                link: None,
            },
        )
        .unwrap();
    for action in [
        EffectAction::Resolve {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
        EffectAction::Authorize {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
    ] {
        egress.apply(&caller, at(), action).unwrap();
    }
    let mut admission_queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
    admission::prepare(
        &bundle,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    admission::deliver(
        &mut admission_queue,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    assert!(run_discovery::discover(&admission_queue, &caller, at(), &id, 8).unwrap().complete);
    let run: RunId = bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    admission_queue.shutdown().unwrap();
    let (entered, ready) = std::sync::mpsc::channel();
    let release = Arc::new(Barrier::new(2));
    let first = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: HeldResultPort { count: count.clone(), entered, release: release.clone() },
        evidence: Evidence(snapshot.clone()),
        clock: Clock,
    };
    let first_context = context(bundle.wi(), &tenant, &id, run, AttemptId::new());
    let first_thread = std::thread::spawn(move || first.execute_bound(&first_context));
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let second = EffectAttemptHandler {
        bundle: bundle.clone(),
        port: MockSuccessfulPort { count: count.clone() },
        evidence: Evidence(snapshot),
        clock: Clock,
    };
    let second_context = context(bundle.wi(), &tenant, &id, run, AttemptId::new());
    let second_disposition = second.execute_bound(&second_context).unwrap();
    assert!(matches!(second_disposition.outcome(), DispositionOutcome::TerminalFailure { .. }));
    release.wait();
    let late_disposition = first_thread.join().unwrap().unwrap();
    assert_eq!(late_disposition.outcome(), second_disposition.outcome());
    assert_eq!(fs::read(&count).unwrap().len(), 1);
    let effect = &bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id];
    assert_eq!(effect.outcomes.len(), 1);
    assert_eq!(effect.outcomes[0].outcome, ExecutionOutcome::Uncertain);
    assert_eq!(bundle.wi().effect_receipts(&tenant, &id).unwrap().len(), 1);
    drop(bundle);
    assert!(StoreBundle::open(&path).unwrap().convergence_faults().is_empty());
}

#[tokio::test]
async fn completed_aq_run_without_fabric_receipt_or_locator_rejects_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("bundle");
    let bundle = Arc::new(
        StoreBundle::initialize(
            &path,
            StoreProfile { instance: InstanceId::generate(), created_at: at(), platform: false },
        )
        .unwrap(),
    );
    let (submission, snapshot) = fixture();
    let tenant = snapshot.bindings[0].tenant.clone();
    publish_inputs(bundle.wi(), &tenant);
    let caller = AuthenticatedCaller::from_trusted_authentication(
        tenant.clone(),
        snapshot.bindings[0].principal.clone(),
    );
    let egress = Egress::new(bundle.wi());
    let id = egress
        .apply(
            &caller,
            at(),
            EffectAction::Submit {
                id: EffectIntentId::generate(),
                submission: Box::new(submission),
                snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
                link: None,
            },
        )
        .unwrap();
    for action in [
        EffectAction::Resolve {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
        EffectAction::Authorize {
            id: id.clone(),
            snapshot: VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
        },
    ] {
        egress.apply(&caller, at(), action).unwrap();
    }
    let mut admission_queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
    admission::prepare(
        &bundle,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    admission::deliver(
        &mut admission_queue,
        &caller,
        at(),
        &id,
        VerifiedSnapshot::from_trusted_verifier(snapshot.clone()).unwrap(),
    )
    .unwrap();
    assert!(run_discovery::discover(&admission_queue, &caller, at(), &id, 8).unwrap().complete);
    let run: RunId = bundle.wi().effect_snapshot(&tenant).unwrap().effects[&id].dispatches[0]
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    admission_queue.shutdown().unwrap();
    let mut aq = ActionQueueEngine::new(
        RuntimeConfig { data_dir: bundle.aq_path(), ..Default::default() },
        Noop,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let _ =
        tokio::time::timeout(Duration::from_secs(20), aq.run_until_idle()).await.unwrap().unwrap();
    assert_eq!(
        aq.projection().get_run_state(&run),
        Some(&actionqueue_core::run::state::RunState::Completed)
    );
    assert!(bundle.wi().effect_receipts(&tenant, &id).unwrap().is_empty());
    aq.shutdown().unwrap();
    drop(bundle);
    assert!(StoreBundle::open(&path).is_err());
}
