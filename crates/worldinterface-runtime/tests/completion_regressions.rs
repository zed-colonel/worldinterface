use support::effect_fixture::spi_fixture;
use worldinterface_runtime::connector::{
    Connector as EffectExecutionPort, PreparedInvocation as BoundExecution,
};
mod support {
    pub mod effect_fixture;
}
use actionqueue_core::{
    disposition::DispositionOutcome,
    ids::{AttemptId, RunId},
};
use actionqueue_executor_local::handler::{
    AttemptMetadata, CancellationContext, CancellationToken, ExecutorContext, ExecutorHandler,
    HandlerInput,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use support::effect_fixture::*;
use worldinterface_domain::{
    authorization::AuthorizationSnapshot,
    effect::{CommitObservation, EffectAction},
    receipt::{DisclosureProfile, ReceiptAccess},
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::{EvidenceAuthority, TrustedClock},
    completion::{EffectAttemptHandler, EvidenceBytes, PortObservation},
    run_discovery,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::StoreProfile;

struct Noop(Option<&'static str>);
impl ExecutorHandler for Noop {
    fn execute(&self, _: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        match self.0 {
            None => actionqueue_core::disposition::AttemptDisposition::complete(None),
            Some(code) => actionqueue_core::disposition::AttemptDisposition::terminal_failure(
                actionqueue_core::bounded::BoundedError {
                    code: actionqueue_core::bounded::BoundedCode::new(code).unwrap(),
                    message: actionqueue_core::bounded::BoundedMessage::new(code).unwrap(),
                },
            ),
        }
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
    fn verify(&self, _: &OpaqueRef, r: &EvidenceRef, _: &Timestamp) -> Result<ContentHash> {
        Ok(if r == &self.0.facts[0].evidence {
            self.0.facts[0].attested_document.clone()
        } else {
            ContentHash::of(b"verified")
        })
    }
}
fn second(n: u32) -> Timestamp {
    format!("2026-09-21T00:{:02}:{:02}.000000000Z", n / 60, n % 60).try_into().unwrap()
}
struct Clock;
impl TrustedClock for Clock {
    fn now(&self) -> Result<Timestamp> {
        Ok(second(2))
    }
}
struct BrokenCompletionClock(AtomicUsize);
impl TrustedClock for BrokenCompletionClock {
    fn now(&self) -> Result<Timestamp> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(second(2))
        } else {
            Err("clock unavailable".into())
        }
    }
}
struct SequenceClock(AtomicUsize);
impl TrustedClock for SequenceClock {
    fn now(&self) -> Result<Timestamp> {
        Ok(second(2 + self.0.fetch_add(1, Ordering::SeqCst) as u32))
    }
}
struct InterleavingClock<F>(AtomicUsize, usize, F);
impl<F: Fn(usize)> TrustedClock for InterleavingClock<F> {
    fn now(&self) -> Result<Timestamp> {
        let tick = self.0.fetch_add(1, Ordering::SeqCst);
        if tick > 1 && tick <= self.1 + 1 {
            (self.2)(tick);
        }
        Ok(second(2 + 2 * tick as u32))
    }
}
struct InterleavingPort<F>(Port, F);
impl<F: Fn() + Send + Sync> EffectExecutionPort for InterleavingPort<F> {
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
        let observation = self.0.invoke(request, cancellation)?;
        (self.1)();
        Ok(observation)
    }
}
struct Port {
    calls: Arc<AtomicUsize>,
    variant: &'static str,
}
impl EffectExecutionPort for Port {
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

    fn invoke(&self, _: BoundExecution, _: &CancellationToken) -> Result<PortObservation> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut o = PortObservation {
            commit: CommitObservation::ConfirmedCommitted,
            outcome: ExecutionOutcome::Succeeded,
            outputs: vec![b"output".to_vec()],
            evidence: vec![EvidenceBytes {
                schema: "execution:1".to_owned().try_into()?,
                bytes: b"executed".to_vec(),
            }],
            boundary_record: Some(EvidenceBytes {
                schema: "boundary:1".to_owned().try_into()?,
                bytes: b"provider assertion".to_vec(),
            }),
        };
        match self.variant {
            "contradictory" => o.commit = CommitObservation::ProvenImpossible,
            "incomplete" => o.boundary_record = None,
            "oversized" => o.outputs = vec![vec![1; 16 * 1024 * 1024 + 1]],
            "too-many-outputs" => {
                o.outputs = vec![
                    b"output".to_vec();
                    worldinterface_domain::effect::MAX_OUTCOME_OUTPUTS + 1
                ];
            }
            "too-many-evidence" => {
                o.evidence = (0..=worldinterface_domain::effect::MAX_OUTCOME_EVIDENCE)
                    .map(|_| EvidenceBytes {
                        schema: "execution:1".to_owned().try_into().unwrap(),
                        bytes: b"executed".to_vec(),
                    })
                    .collect();
            }
            "oversized-evidence" => o.evidence[0].bytes = vec![1; 16 * 1024 * 1024 + 1],
            "oversized-boundary" => {
                o.boundary_record.as_mut().unwrap().bytes = vec![1; 16 * 1024 * 1024 + 1]
            }
            "oversized-total" => {
                o.outputs = vec![vec![1; 8 * 1024 * 1024]];
                o.evidence[0].bytes = vec![2; 8 * 1024 * 1024];
            }
            "missing-evidence" => o.evidence.clear(),
            "empty-evidence" => o.evidence[0].bytes.clear(),
            "empty-boundary" => o.boundary_record.as_mut().unwrap().bytes.clear(),
            "wrong-output" => o.outputs = vec![b"different output".to_vec()],
            "noncommit" => {
                o.commit = CommitObservation::ProvenImpossible;
                o.outcome = ExecutionOutcome::FailedBeforeCommit;
                o.outputs.clear();
                o.boundary_record = None;
            }
            "uncertain" => {
                o.commit = CommitObservation::Unknown;
                o.outcome = ExecutionOutcome::Uncertain;
                o.outputs.clear();
                o.boundary_record = None;
            }
            _ => {}
        }
        Ok(o)
    }
}
struct Prepared {
    temp: tempfile::TempDir,
    bundle: Arc<StoreBundle>,
    snapshot: AuthorizationSnapshot,
    id: EffectIntentId,
    run: RunId,
}
impl Prepared {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let bundle = Arc::new(
            StoreBundle::initialize(
                &temp.path().join("bundle"),
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: at(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let (submission, snapshot) = fixture();
        let id = authorize(&bundle, submission, &snapshot);
        let actor = caller(&snapshot);
        let mut queue = QueueEngine::bootstrap(&bundle, Noop(None), host()).unwrap();
        admission::prepare(&bundle, &actor, at(), &id, verified(&snapshot)).unwrap();
        admission::deliver(&mut queue, &actor, second(1), &id, verified(&snapshot)).unwrap();
        run_discovery::discover(&queue, &actor, second(1), &id, 8).unwrap();
        let state = bundle.wi().effect_snapshot(actor.tenant()).unwrap();
        let run =
            state.effects[&id].dispatches.last().unwrap().admission.as_ref().unwrap().observed_runs
                [0]
            .as_str()
            .parse()
            .unwrap();
        queue.shutdown().unwrap();
        Self { temp, bundle, snapshot, id, run }
    }
    fn ctx(&self, run: RunId) -> ExecutorContext {
        let actor = caller(&self.snapshot);
        let state = self.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
        let request = admission::load_request(
            self.bundle.wi(),
            state.effects[&self.id].dispatches.last().unwrap().admission.as_ref().unwrap(),
        )
        .unwrap();
        ExecutorContext {
            input: HandlerInput {
                resume_context: None,
                causal_context: None,
                run_id: run,
                attempt_id: AttemptId::new(),
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
    fn receipt(&self) -> BoundaryReceipt {
        let actor = caller(&self.snapshot);
        let receipts = self.bundle.wi().effect_receipts(actor.tenant(), &self.id).unwrap();
        assert_eq!(receipts.len(), 1);
        let view = self
            .bundle
            .wi()
            .inspect_receipt(
                &ReceiptAccess::from_trusted_authorizer(
                    actor.tenant().clone(),
                    actor.principal().clone(),
                    DisclosureProfile::Full,
                ),
                &receipts[0].0,
            )
            .unwrap()
            .unwrap();
        let Record::BoundaryReceipt(receipt) = view.canonical.unwrap().record else {
            panic!("receipt")
        };
        receipt
    }
}
fn assert_uncertain(disposition: &actionqueue_core::disposition::AttemptDisposition) {
    assert!(
        matches!(disposition.outcome(), DispositionOutcome::TerminalFailure { error } if error.code.as_str() == "wi.effect.uncertain")
    );
}
#[test]
fn invalid_port_observations_commit_inline_uncertainty_without_reinvocation() {
    for variant in ["contradictory", "incomplete", "oversized", "missing-evidence"] {
        let case = Prepared::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Port { calls: calls.clone(), variant },
            evidence: Evidence(case.snapshot.clone()),
            clock: Clock,
        };
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        let state = case.bundle.wi().effect_snapshot(caller(&case.snapshot).tenant()).unwrap();
        assert_eq!(state.effects[&case.id].phase, Phase::Uncertain, "{variant}");
        let receipt = case.receipt();
        assert_eq!(receipt.temporal.committed_at.is_some(), variant != "contradictory");
        assert!(receipt
            .evidence
            .iter()
            .any(|e| e.schema.as_str() == "wi.completion.uncertainty:1"));
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn oversized_observations_publish_no_port_artifacts() {
    for variant in [
        "too-many-outputs",
        "too-many-evidence",
        "oversized",
        "oversized-evidence",
        "oversized-boundary",
        "oversized-total",
    ] {
        let case = Prepared::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Port { calls: calls.clone(), variant },
            evidence: Evidence(case.snapshot.clone()),
            clock: Clock,
        };
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{variant}");
        let receipt = case.receipt();
        let ReceiptBody::Egress(body) = &receipt.body else { panic!("egress") };
        assert!(body.output_refs.is_empty(), "{variant}");
        assert_eq!(receipt.evidence.len(), 1, "{variant}");
        assert_eq!(receipt.evidence[0].schema.as_str(), "wi.completion.uncertainty:1");
        let c =
            rusqlite::Connection::open(case.temp.path().join("bundle/wi/context.sqlite")).unwrap();
        let count: usize = c
            .query_row(
                "SELECT count(*) FROM publication_index WHERE identity GLOB 'effect-*:port:*'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "{variant}");
    }
}

#[test]
fn structurally_invalid_observations_publish_no_port_artifacts() {
    for variant in [
        "contradictory",
        "incomplete",
        "missing-evidence",
        "empty-evidence",
        "empty-boundary",
        "wrong-output",
    ] {
        let case = Prepared::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Port { calls: calls.clone(), variant },
            evidence: Evidence(case.snapshot.clone()),
            clock: Clock,
        };
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{variant}");
        let receipt = case.receipt();
        let ReceiptBody::Egress(body) = &receipt.body else { panic!("egress") };
        assert!(body.output_refs.is_empty(), "{variant}");
        assert_eq!(receipt.evidence.len(), 1, "{variant}");
        assert_eq!(receipt.evidence[0].schema.as_str(), "wi.completion.uncertainty:1");
        let c =
            rusqlite::Connection::open(case.temp.path().join("bundle/wi/context.sqlite")).unwrap();
        let count: usize = c
            .query_row(
                "SELECT count(*) FROM publication_index WHERE identity GLOB 'effect-*:port:*'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "{variant}");
    }
}

#[test]
fn discovery_noops_preserve_history_and_start_freezes_the_cursor() {
    let case = Prepared::new();
    let root = case.bundle.wi();
    let actor = caller(&case.snapshot);
    let egress = Egress::new(root);
    let before = root.effect_snapshot(actor.tenant()).unwrap();
    let history_count = || {
        let c =
            rusqlite::Connection::open(case.temp.path().join("bundle/wi/fabric.sqlite")).unwrap();
        c.query_row("SELECT count(*) FROM effect_history", [], |r| r.get::<_, u64>(0)).unwrap()
    };
    let rows = history_count();
    let queue = QueueEngine::bootstrap(&case.bundle, Noop(None), host()).unwrap();
    for _ in 0..50 {
        assert!(run_discovery::discover(&queue, &actor, second(1), &case.id, 8).unwrap().complete);
    }
    assert_eq!(root.effect_snapshot(actor.tenant()).unwrap(), before);
    assert_eq!(history_count(), rows);
    queue.shutdown().unwrap();
    let advance = |previous: Option<&str>, next: Option<&str>| {
        egress.apply(
            &actor,
            second(2),
            EffectAction::AdvanceDiscovery {
                id: case.id.clone(),
                previous: previous.map(str::to_owned),
                next: next.map(str::to_owned),
            },
        )
    };
    advance(None, Some("cursor:next")).unwrap();
    for _ in 0..50 {
        advance(Some("cursor:next"), Some("cursor:next")).unwrap();
    }
    assert!(advance(None, Some("cursor:stale")).is_err());
    assert_eq!(history_count(), rows + 1);
    let ctx = case.ctx(case.run);
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: case.run.to_string().try_into().unwrap(),
                attempt: ctx.input.attempt_id.to_string().try_into().unwrap(),
            },
        )
        .unwrap();
    for next in [Some("cursor:next"), Some("cursor:changed"), None] {
        assert!(advance(Some("cursor:next"), next).is_err());
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    // The existing Start means a redelivery must record uncertainty, never invoke.
    assert_uncertain(&handler.execute(ctx));
    let terminal = root.effect_snapshot(actor.tenant()).unwrap();
    assert_eq!(terminal.revision, before.revision + 3);
    assert!(advance(Some("cursor:next"), Some("cursor:changed")).is_err());
    assert!(advance(Some("cursor:next"), Some("cursor:next")).is_err());
    assert_eq!(root.effect_snapshot(actor.tenant()).unwrap(), terminal);
    assert_eq!(history_count(), rows + 3);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    case.receipt();
    let reopened =
        worldinterface_storage::StoreRoot::open_existing(case.temp.path().join("bundle/wi"))
            .unwrap();
    assert_eq!(reopened.effect_snapshot(actor.tenant()).unwrap(), terminal);
}

#[test]
fn sibling_run_cannot_publish_or_reuse_the_start_runs_receipt() {
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let original = case.ctx(case.run);
    let attempt: OpaqueRef = original.input.attempt_id.to_string().try_into().unwrap();
    let egress = Egress::new(case.bundle.wi());
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: case.run.to_string().try_into().unwrap(),
                attempt,
            },
        )
        .unwrap();
    let sibling = RunId::new();
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::ObserveRun {
                id: case.id.clone(),
                run: sibling.to_string().try_into().unwrap(),
            },
        )
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    assert!(handler.execute_bound(&case.ctx(sibling)).is_err());
    let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    assert_eq!(state.effects[&case.id].phase, Phase::Running);
    assert!(state.effects[&case.id].outcomes.is_empty());
    let c = rusqlite::Connection::open(case.temp.path().join("bundle/wi/context.sqlite")).unwrap();
    let count: usize = c
        .query_row(
            "SELECT count(*) FROM publication_index WHERE identity GLOB 'effect-*'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    assert_uncertain(&handler.execute(original));
    assert!(handler.execute_bound(&case.ctx(sibling)).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn committed_disposition_rejects_request_admission_and_outcome_binding_drift() {
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    let ctx = case.ctx(case.run);
    assert!(matches!(handler.execute_bound(&ctx).unwrap().outcome(), DispositionOutcome::Complete));
    let original = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    let c = rusqlite::Connection::open(case.temp.path().join("bundle/wi/fabric.sqlite")).unwrap();
    for drift in ["request", "index", "admission", "outcome"] {
        // Model projection drift after open; immutable journal facts still bind
        // completion even though this test bypasses trusted-host persistence.
        let mut state = original.clone();
        let e = state.effects.get_mut(&case.id).unwrap();
        let dispatch = e.dispatches.last_mut().unwrap();
        match drift {
            "request" => dispatch.request.authorization = ContentHash::of(b"other-authorization"),
            "index" => dispatch.request.operation_index += 1,
            "admission" => {
                dispatch.admission.as_mut().unwrap().execution = ContentHash::of(b"other-execution")
            }
            "outcome" => {
                e.outcomes.last_mut().unwrap().evidence[0].schema =
                    "other-schema:1".to_owned().try_into().unwrap()
            }
            _ => unreachable!(),
        }
        c.execute(
            "UPDATE effect_state SET state=? WHERE tenant=?",
            rusqlite::params![canonical_collection(&state).unwrap(), actor.tenant().as_str()],
        )
        .unwrap();
        assert!(handler.execute_bound(&ctx).is_err(), "{drift}");
    }
    c.execute(
        "UPDATE effect_state SET state=? WHERE tenant=?",
        rusqlite::params![canonical_collection(&original).unwrap(), actor.tenant().as_str()],
    )
    .unwrap();
    assert!(matches!(handler.execute_bound(&ctx).unwrap().outcome(), DispositionOutcome::Complete));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    case.receipt();
}

#[test]
fn completion_and_redelivery_bound_latency_and_writer_delay_with_long_history() {
    use std::time::{Duration, Instant};
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let path = case.temp.path().join("bundle/wi/fabric.sqlite");
    let mut c = rusqlite::Connection::open(&path).unwrap();
    let tx = c.transaction().unwrap();
    let mut state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    // Batch genuine, domain-validated discovery entries so fixture construction
    // does not dominate the test with a thousand fsyncs. No synthetic authority
    // or malformed history is needed to exercise the former full replay path.
    for n in 0..1000 {
        let previous = state.effects[&case.id]
            .dispatches
            .last()
            .unwrap()
            .admission
            .as_ref()
            .unwrap()
            .discovery_cursor
            .clone();
        let command = worldinterface_domain::effect::validate_command(
            &state,
            &actor,
            second(1),
            EffectAction::AdvanceDiscovery {
                id: case.id.clone(),
                previous,
                next: Some(format!("history-{n}")),
            },
        )
        .unwrap();
        state =
            worldinterface_domain::effect::replay_command(&state, actor.tenant(), command.bytes())
                .unwrap()
                .0;
        tx.execute(
            "INSERT INTO effect_history(tenant,revision,entry,digest) VALUES(?,?,?,?)",
            rusqlite::params![
                actor.tenant().as_str(),
                state.revision,
                command.bytes(),
                ContentHash::of(command.bytes()).as_str()
            ],
        )
        .unwrap();
    }
    tx.execute(
        "UPDATE effect_state SET state=? WHERE tenant=?",
        rusqlite::params![canonical_collection(&state).unwrap(), actor.tenant().as_str()],
    )
    .unwrap();
    // Keep the derived outbox identical to a normal sequence of transactions.
    tx.execute(
        "UPDATE effect_outbox SET dispatch=? WHERE tenant=? AND intent=?",
        rusqlite::params![
            canonical_data(&state.effects[&case.id].dispatches).unwrap(),
            actor.tenant().as_str(),
            case.id.as_str()
        ],
    )
    .unwrap();
    tx.execute(
        "INSERT INTO effect_state VALUES(?,?)",
        rusqlite::params![
            "other-tenant",
            canonical_collection(&worldinterface_domain::effect::EffectState::default()).unwrap()
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    drop(c);
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    let ctx = case.ctx(case.run);
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(0);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer = std::thread::spawn(move || {
        let c = rusqlite::Connection::open(path).unwrap();
        c.busy_timeout(Duration::from_secs(10)).unwrap();
        ready_tx.send(()).unwrap();
        let mut max_wait = Duration::ZERO;
        let mut writes = 0;
        while !writer_stop.load(Ordering::SeqCst) {
            let start = Instant::now();
            c.execute_batch("BEGIN EXCLUSIVE; UPDATE effect_state SET state=state WHERE tenant='other-tenant'; COMMIT").unwrap();
            max_wait = max_wait.max(start.elapsed());
            writes += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
        (writes, max_wait)
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let start = Instant::now();
    let disposition = handler.execute_bound(&ctx);
    let completion_time = start.elapsed();
    let mut redeliveries = Vec::new();
    for _ in 0..3 {
        let start = Instant::now();
        redeliveries.push((handler.execute_bound(&ctx), start.elapsed()));
    }
    stop.store(true, Ordering::SeqCst);
    let (writes, max_wait) = writer.join().unwrap();
    assert!(matches!(disposition.unwrap().outcome(), DispositionOutcome::Complete));
    assert!(completion_time < Duration::from_secs(5), "completion: {completion_time:?}");
    for (result, elapsed) in redeliveries {
        assert!(matches!(result.unwrap().outcome(), DispositionOutcome::Complete));
        assert!(elapsed < Duration::from_secs(5), "redelivery: {elapsed:?}");
    }
    assert!(writes > 0);
    assert!(max_wait < Duration::from_secs(1), "writer delay: {max_wait:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    case.receipt();
    eprintln!("S-026: 1000 journal entries; completion={completion_time:?}; cross-tenant writes={writes}; max_writer_delay={max_wait:?}");
}
#[test]
fn completion_clock_failure_records_inferred_uncertainty_with_effect_phase_times() {
    let case = Prepared::new();
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: Arc::new(AtomicUsize::new(0)), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: BrokenCompletionClock(AtomicUsize::new(0)),
    };
    assert_uncertain(&handler.execute(case.ctx(case.run)));
    let temporal = case.receipt().temporal;
    assert_eq!(temporal.received_at.at, at());
    assert_eq!(temporal.admitted_at.at, second(1));
    assert_eq!(temporal.dispatched_at.unwrap().at, second(2));
    assert!(temporal.committed_at.is_none());
    assert_eq!(temporal.observed_at.as_ref().unwrap().assurance.as_str(), "inferred");
    assert_eq!(temporal.observed_at.unwrap().uncertainty_nanos, 9_007_199_254_740_991);
    assert!(temporal.reconciled_at.is_none());
}
// Start a writer precisely while completion holds its snapshot. A zero-timeout
// second connection proves BEGIN IMMEDIATE already excludes it; its real writes
// then wait for that same lock and finish without invalidating the observation.
fn journal_contender(
    case: &Prepared,
    count: usize,
) -> (impl Fn(usize) + Send + Sync, std::thread::JoinHandle<()>) {
    let bundle = case.bundle.clone();
    let snapshot = case.snapshot.clone();
    let path = case.temp.path().join("bundle/wi/fabric.sqlite");
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel(0);
    let (probed_tx, probed_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        start_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        let c = rusqlite::Connection::open(path).unwrap();
        c.busy_timeout(std::time::Duration::ZERO).unwrap();
        let error = c.execute_batch("BEGIN IMMEDIATE").unwrap_err();
        assert!(worldinterface_storage::is_effect_contention(&error));
        probed_tx.send(()).unwrap();
        for n in 0..count {
            let (mut submission, _) = fixture();
            submission.client_request_id = format!("contender-{n}").try_into().unwrap();
            Egress::new(bundle.wi())
                .apply(
                    &caller(&snapshot),
                    second(10 + n as u32),
                    EffectAction::Submit {
                        id: EffectIntentId::generate(),
                        submission: Box::new(submission),
                        snapshot: verified(&snapshot),
                        link: None,
                    },
                )
                .unwrap();
        }
    });
    let probed_rx = std::sync::Mutex::new(probed_rx);
    (
        move |_| {
            start_tx.send(()).unwrap();
            probed_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        },
        writer,
    )
}

#[test]
fn contended_completion_preserves_provider_outcome_and_published_artifacts() {
    for variant in ["success", "noncommit"] {
        let case = Prepared::new();
        let actor = caller(&case.snapshot);
        let (interleave, writer) = journal_contender(&case, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Port { calls: calls.clone(), variant },
            evidence: Evidence(case.snapshot.clone()),
            clock: InterleavingClock(AtomicUsize::new(0), 1, interleave),
        };
        let disposition = handler.execute(case.ctx(case.run));
        writer.join().unwrap();
        if variant == "success" {
            assert!(matches!(disposition.outcome(), DispositionOutcome::Complete));
        } else {
            assert!(matches!(disposition.outcome(), DispositionOutcome::TerminalFailure { error }
                if error.code.as_str() == "wi.effect.failed_before_commit"));
        }
        let expected = if variant == "success" {
            ExecutionOutcome::Succeeded
        } else {
            ExecutionOutcome::FailedBeforeCommit
        };
        let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
        assert_eq!(state.effects.len(), 2);
        assert_eq!(state.effects[&case.id].outcomes.len(), 1);
        let receipt = case.receipt();
        let ReceiptBody::Egress(body) = &receipt.body else { panic!("egress receipt") };
        assert_eq!(body.outcome, expected);
        if variant == "success" {
            assert_eq!(body.output_refs.len(), 1);
            assert_eq!(case.bundle.wi().read_artifact(&body.output_refs[0]).unwrap(), b"output");
        }
        assert_eq!(receipt.temporal.observed_at.as_ref().unwrap().at, second(6));
        assert!(receipt.temporal.reconciled_at.is_none());
        assert!(!receipt.evidence.iter().any(|e| e.schema.as_str().contains("uncertainty")));
        let c =
            rusqlite::Connection::open(case.temp.path().join("bundle/wi/context.sqlite")).unwrap();
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM publication_index WHERE identity GLOB 'effect-*:port:*'",
                [],
                |r| r.get::<_, usize>(0)
            )
            .unwrap(),
            if variant == "success" { 3 } else { 1 }
        );
        assert_eq!(handler.execute(case.ctx(case.run)).outcome(), disposition.outcome());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(case.receipt(), receipt);
        StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
    }
}

#[test]
fn sustained_tenant_writes_preserve_confirmed_completion_without_reinvocation() {
    // S-018: more writes than the old optimistic retry budget cannot exhaust it.
    let case = Prepared::new();
    let (interleave, writer) = journal_contender(&case, 40);
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: InterleavingClock(AtomicUsize::new(0), 1, interleave),
    };
    let disposition = handler.execute(case.ctx(case.run));
    writer.join().unwrap();
    assert!(matches!(disposition.outcome(), DispositionOutcome::Complete));
    let state = case.bundle.wi().effect_snapshot(caller(&case.snapshot).tenant()).unwrap();
    assert_eq!(state.effects.len(), 41);
    assert_eq!(state.effects[&case.id].outcomes[0].outcome, ExecutionOutcome::Succeeded);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let receipt = case.receipt();
    assert!(!receipt.evidence.iter().any(|e| e.schema.as_str().contains("uncertainty")));
    assert_eq!(handler.execute(case.ctx(case.run)).outcome(), disposition.outcome());
    assert_eq!(case.receipt(), receipt);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
}

#[test]
fn completion_retries_publication_contention_without_losing_provider_observation() {
    let case = Prepared::new();
    let path = case.temp.path().join("bundle/wi/context.sqlite");
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let (start_tx, start_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        start_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch("BEGIN EXCLUSIVE").unwrap();
        locked_tx.send(()).unwrap();
        // Exceed the storage connection's 5s busy timeout once.
        std::thread::sleep(std::time::Duration::from_millis(5200));
        c.execute_batch("COMMIT").unwrap();
    });
    let locked_rx = std::sync::Mutex::new(locked_rx);
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: InterleavingPort(Port { calls: calls.clone(), variant: "success" }, move || {
            start_tx.send(()).unwrap();
            locked_rx.lock().unwrap().recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        }),
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    let disposition = handler.execute(case.ctx(case.run));
    writer.join().unwrap();
    assert!(matches!(disposition.outcome(), DispositionOutcome::Complete));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let receipt = case.receipt();
    let ReceiptBody::Egress(body) = receipt.body else { panic!("egress receipt") };
    assert_eq!(body.outcome, ExecutionOutcome::Succeeded);
    assert_eq!(case.bundle.wi().read_artifact(&body.output_refs[0]).unwrap(), b"output");
    assert!(!receipt.evidence.iter().any(|e| e.schema.as_str().contains("uncertainty")));
}

#[test]
fn contended_completion_preserves_a_superseding_uncertain_receipt() {
    let case = Prepared::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let contender_calls = Arc::new(AtomicUsize::new(0));
    let contender = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: contender_calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    let contender_ctx = case.ctx(case.run);
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: InterleavingPort(Port { calls: calls.clone(), variant: "success" }, move || {
            assert_uncertain(&contender.execute_bound(&contender_ctx).unwrap());
        }),
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    assert_uncertain(&handler.execute(case.ctx(case.run)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(contender_calls.load(Ordering::SeqCst), 0);
    let receipt = case.receipt();
    assert!(receipt.evidence.iter().any(|e| e.schema.as_str() == "wi.recovery.uncertainty:1"));
    assert!(!receipt.evidence.iter().any(|e| e.schema.as_str() == "wi.completion.uncertainty:1"));
    assert_uncertain(&handler.execute(case.ctx(case.run)));
    assert_eq!(case.receipt(), receipt);
    // S-024: a provider observation superseded before publication leaves no
    // orphan output/evidence/boundary publications behind.
    let context =
        rusqlite::Connection::open(case.temp.path().join("bundle/wi/context.sqlite")).unwrap();
    let orphaned: u64 = context
        .query_row(
            "SELECT count(*) FROM publication_index WHERE identity LIKE 'effect-output:port:%' OR identity LIKE 'effect-evidence:port:%' OR identity LIKE 'effect-boundary:port:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(orphaned, 0);
}

#[test]
fn unreadable_binding_never_claims_retry_safe_pre_start_failure() {
    // S-015: the durable phase cannot be inferred from a failed input read,
    // including on a delivery after an earlier durable Start.
    for started in [false, true] {
        let case = Prepared::new();
        let actor = caller(&case.snapshot);
        let ctx = case.ctx(case.run);
        if started {
            Egress::new(case.bundle.wi())
                .apply(
                    &actor,
                    second(2),
                    EffectAction::StartProduction {
                        id: case.id.clone(),
                        snapshot: verified(&case.snapshot),
                        run: case.run.to_string().try_into().unwrap(),
                        attempt: ctx.input.attempt_id.to_string().try_into().unwrap(),
                    },
                )
                .unwrap();
        }
        let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
        let input = &state.effects[&case.id].intent.submission.input.artifact;
        let path = case.temp.path().join("bundle/wi/artifacts").join(input.content.as_str());
        let bytes = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Port { calls: calls.clone(), variant: "success" },
            evidence: Evidence(case.snapshot.clone()),
            clock: Clock,
        };
        assert!(
            matches!(handler.execute(ctx).outcome(), DispositionOutcome::TerminalFailure { error }
            if error.code.as_str() == "wi.effect.phase_unknown")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(case.bundle.wi().dispatch_faults(actor.tenant(), &case.id).unwrap().is_empty());
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(case.bundle.wi().effect_snapshot(actor.tenant()).unwrap(), state);
        StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
    }
}

#[test]
fn canceled_pre_start_run_retains_cancellation_diagnostic_on_reopen() {
    // S-016: operator cancellation did not exhaust AQ's failure budget.
    let case = Prepared::new();
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            ..Default::default()
        },
        Noop(None),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    aq.cancel_run(case.run).unwrap();
    assert_eq!(
        aq.projection().get_run_state(&case.run),
        Some(&actionqueue_core::run::state::RunState::Canceled)
    );
    aq.shutdown().unwrap();
    let actor = caller(&case.snapshot);
    for _ in 0..2 {
        let reopened = StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
        let faults = reopened.wi().dispatch_faults(actor.tenant(), &case.id).unwrap();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0].code, "wi.effect.pre_start_canceled");
        assert!(reopened.wi().effect_receipts(actor.tenant(), &case.id).unwrap().is_empty());
        let state = reopened.wi().effect_snapshot(actor.tenant()).unwrap();
        assert_eq!(state.effects[&case.id].phase, Phase::DispatchPending);
        assert!(state.effects[&case.id].attempt.is_none());
    }
}

#[test]
fn possible_commit_aq_codes_converge_without_spurious_faults() {
    // S-016/S-017: these codes preserve possible commit; recovery records an
    // inferred observation without claiming that reconciliation took place.
    for code in [
        "wi.effect.uncertain_recording_error",
        "wi.effect.phase_unknown",
        "fixture.disposition_mismatch",
    ] {
        let case = Prepared::new();
        let actor = caller(&case.snapshot);
        let attempt: OpaqueRef = AttemptId::new().to_string().try_into().unwrap();
        Egress::new(case.bundle.wi())
            .apply(
                &actor,
                second(2),
                EffectAction::StartProduction {
                    id: case.id.clone(),
                    snapshot: verified(&case.snapshot),
                    run: case.run.to_string().try_into().unwrap(),
                    attempt: attempt.clone(),
                },
            )
            .unwrap();
        let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
            actionqueue_runtime::config::RuntimeConfig {
                data_dir: case.bundle.aq_path(),
                ..Default::default()
            },
            Noop(Some(code)),
        )
        .bootstrap()
        .unwrap()
        .with_host(host());
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
            async {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(20), aq.run_until_idle())
                        .await
                        .unwrap()
                        .unwrap();
            },
        );
        assert_eq!(
            aq.projection().get_run_state(&case.run),
            Some(&actionqueue_core::run::state::RunState::Failed)
        );
        aq.shutdown().unwrap();
        for _ in 0..2 {
            let reopened = StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
            assert_eq!(
                reopened.convergence_faults().len(),
                usize::from(code == "fixture.disposition_mismatch")
            );
            let state = reopened.wi().effect_snapshot(actor.tenant()).unwrap();
            assert_eq!(state.effects[&case.id].phase, Phase::Uncertain);
            assert_eq!(state.effects[&case.id].attempt.as_ref(), Some(&attempt));
            let receipt = case.receipt();
            let observed = receipt.temporal.observed_at.unwrap();
            assert_eq!(observed.at, second(2));
            assert_eq!(observed.assurance.as_str(), "inferred");
            assert_eq!(observed.uncertainty_nanos, 9_007_199_254_740_991);
            assert!(receipt.temporal.committed_at.is_none());
            assert!(receipt.temporal.reconciled_at.is_none());
        }
    }
}
#[test]
fn terminal_recovery_uses_start_run_and_skips_other_observed_terminal_runs() {
    // S-022: the model permits multiple observed runs. A terminal sibling
    // cannot prove that the run which recorded Start has become terminal.
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let attempt: OpaqueRef = AttemptId::new().to_string().try_into().unwrap();
    let started_run: OpaqueRef =
        "00000000-0000-0000-0000-000000000001".to_owned().try_into().unwrap();
    let terminal_run: OpaqueRef = case.run.to_string().try_into().unwrap();
    assert!(started_run < terminal_run);
    let egress = Egress::new(case.bundle.wi());
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::ObserveRun { id: case.id.clone(), run: started_run.clone() },
        )
        .unwrap();
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: started_run.clone(),
                attempt: attempt.clone(),
            },
        )
        .unwrap();
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            ..Default::default()
        },
        Noop(Some("wi.effect.uncertain_recording_error")),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(20), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
    });
    aq.shutdown().unwrap();
    for _ in 0..2 {
        let reopened = StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
        let state = reopened.wi().effect_snapshot(actor.tenant()).unwrap();
        assert_eq!(state.effects[&case.id].phase, Phase::Running);
        assert!(reopened.wi().effect_receipts(actor.tenant(), &case.id).unwrap().is_empty());
        assert!(reopened
            .wi()
            .effect_transact_receipt_with(
                actor.tenant(),
                &case.id,
                &attempt,
                &terminal_run,
                |_, _| panic!("a sibling run must not prepare a receipt for Start"),
            )
            .unwrap()
            .is_none());
        assert_eq!(
            reopened.wi().effect_execution_times(actor.tenant(), &case.id).unwrap().run,
            Some(started_run.clone())
        );
    }
}

#[test]
fn terminal_recovery_selects_start_among_multiple_terminal_runs() {
    use actionqueue_core::{
        admission::EnsureTaskRequest,
        causal::CausalContext,
        ids::{AdmissionKey, CorrelationId, TaskId, TraceId},
        task::{
            constraints::TaskConstraints,
            metadata::TaskMetadata,
            run_policy::RunPolicy,
            task_spec::{TaskPayload, TaskSpec},
        },
    };
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            ..Default::default()
        },
        Noop(Some("wi.effect.uncertain_recording_error")),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    // Admission currently uses Once. Build the model's permitted multi-run
    // shape explicitly using another real AQ run, without altering its policy.
    let sibling_task = TaskId::new();
    aq.ensure_task(
        EnsureTaskRequest::new(
            AdmissionKey::new("terminal-sibling").unwrap(),
            TaskSpec::new(
                sibling_task,
                TaskPayload::new(vec![]),
                RunPolicy::Once,
                TaskConstraints::default(),
                TaskMetadata::default(),
            )
            .unwrap(),
            vec![],
            CausalContext::new(
                TraceId::new("terminal-sibling").unwrap(),
                CorrelationId::new("terminal-sibling").unwrap(),
            ),
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let sibling: OpaqueRef =
        aq.projection().run_ids_for_task(sibling_task)[0].to_string().try_into().unwrap();
    let original: OpaqueRef = case.run.to_string().try_into().unwrap();
    let started = std::cmp::min(&sibling, &original).clone();
    let attempt: OpaqueRef = AttemptId::new().to_string().try_into().unwrap();
    let egress = Egress::new(case.bundle.wi());
    egress
        .apply(&actor, second(2), EffectAction::ObserveRun { id: case.id.clone(), run: sibling })
        .unwrap();
    egress
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: started,
                attempt: attempt.clone(),
            },
        )
        .unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(20), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
    });
    aq.shutdown().unwrap();
    for _ in 0..2 {
        let reopened = StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
        assert!(reopened.convergence_faults().is_empty());
        let state = reopened.wi().effect_snapshot(actor.tenant()).unwrap();
        assert_eq!(state.effects[&case.id].phase, Phase::Uncertain);
        assert_eq!(state.effects[&case.id].attempt.as_ref(), Some(&attempt));
        let ReceiptBody::Egress(receipt) = case.receipt().body else { panic!("egress receipt") };
        assert_eq!(receipt.attempt, attempt);
    }
}

#[test]
fn terminal_recovery_waits_for_writers_and_preserves_original_attempt() {
    // S-021: real AQ terminal state, overlapping same-tenant writes and reopen.
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let attempt: OpaqueRef = AttemptId::new().to_string().try_into().unwrap();
    let run: OpaqueRef = case.run.to_string().try_into().unwrap();
    Egress::new(case.bundle.wi())
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: run.clone(),
                attempt: attempt.clone(),
            },
        )
        .unwrap();
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            ..Default::default()
        },
        Noop(Some("wi.effect.uncertain_recording_error")),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(20), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
    });
    aq.shutdown().unwrap();
    let path = case.temp.path().join("bundle/wi/fabric.sqlite");
    let bundle = case.bundle.clone();
    let snapshot = case.snapshot.clone();
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(0);
    let writer = std::thread::spawn(move || {
        let c = rusqlite::Connection::open(path).unwrap();
        c.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        c.execute_batch("COMMIT").unwrap();
        for n in 0..40 {
            let (mut submission, _) = fixture();
            submission.client_request_id = format!("recovery-contender-{n}").try_into().unwrap();
            Egress::new(bundle.wi())
                .apply(
                    &caller(&snapshot),
                    second(10 + n),
                    EffectAction::Submit {
                        id: EffectIntentId::generate(),
                        submission: Box::new(submission),
                        snapshot: verified(&snapshot),
                        link: None,
                    },
                )
                .unwrap();
        }
    });
    locked_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
    let reopened = StoreBundle::open(&case.temp.path().join("bundle"));
    writer.join().unwrap();
    let reopened = reopened.unwrap();
    assert!(reopened.convergence_faults().is_empty());
    let state = reopened.wi().effect_snapshot(actor.tenant()).unwrap();
    assert_eq!(state.effects.len(), 41);
    let effect = &state.effects[&case.id];
    assert_eq!(effect.phase, Phase::Uncertain);
    assert_eq!(effect.attempt.as_ref(), Some(&attempt));
    assert_eq!(effect.outcomes.len(), 1);
    let receipt = case.receipt();
    assert!(receipt.temporal.reconciled_at.is_none());
    // A lost recovery/completion fence cannot run even its preparation callback.
    assert!(reopened
        .wi()
        .effect_transact_receipt_with(actor.tenant(), &case.id, &attempt, &run, |_, _| panic!(
            "a superseded completion must not prepare another outcome"
        ))
        .unwrap()
        .is_none());
    drop(reopened);
    StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
    assert_eq!(case.receipt(), receipt);
}

#[test]
fn successful_completion_preserves_distinct_journal_phase_times() {
    let case = Prepared::new();
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: Arc::new(AtomicUsize::new(0)), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: SequenceClock(AtomicUsize::new(0)),
    };
    assert!(matches!(handler.execute(case.ctx(case.run)).outcome(), DispositionOutcome::Complete));
    let temporal = case.receipt().temporal;
    assert_eq!(temporal.received_at.at, at());
    assert_eq!(temporal.admitted_at.at, second(1));
    assert_eq!(temporal.dispatched_at.unwrap().at, second(2));
    assert_eq!(temporal.committed_at.unwrap().at, second(4));
    assert!(temporal.reconciled_at.is_none());
}
#[test]
fn expired_dispatch_authorization_retries_and_keeps_a_durable_diagnostic() {
    let case = Prepared::new();
    let mut expired = case.snapshot.clone();
    expired.grants[0].expires_at = at();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(expired),
        clock: Clock,
    };
    let disposition = handler.execute(case.ctx(case.run));
    assert!(
        matches!(disposition.outcome(), DispositionOutcome::RetryableFailure { error } if error.code.as_str() == "wi.effect.pre_start_error")
    );
    let actor = caller(&case.snapshot);
    assert_eq!(
        case.bundle.wi().effect_snapshot(actor.tenant()).unwrap().effects[&case.id].phase,
        Phase::DispatchPending
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(case.bundle.wi().dispatch_faults(actor.tenant(), &case.id).unwrap().len(), 1);
    // Execute the real AQ retry budget with expired authority, then reopen. No
    // domain outcome is invented when the AQ run becomes terminal before Start.
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            backoff_strategy: actionqueue_runtime::config::BackoffStrategyConfig::Fixed {
                interval: std::time::Duration::ZERO,
            },
            ..Default::default()
        },
        handler,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), aq.run_until_idle())
            .await
            .unwrap()
            .unwrap();
    });
    assert_eq!(
        aq.projection().get_run_state(&case.run),
        Some(&actionqueue_core::run::state::RunState::Failed)
    );
    assert_eq!(aq.projection().get_attempt_history(&case.run).unwrap().len(), 3);
    aq.shutdown().unwrap();
    let reopened = StoreBundle::open(&case.temp.path().join("bundle")).unwrap();
    assert!(reopened
        .wi()
        .dispatch_faults(actor.tenant(), &case.id)
        .unwrap()
        .iter()
        .any(|f| f.code == "wi.effect.pre_start_exhausted"));
    assert!(reopened.wi().effect_receipts(actor.tenant(), &case.id).unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[test]
fn retry_dispatch_does_not_reuse_the_previous_attempt_disposition() {
    let case = Prepared::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "uncertain" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    assert_uncertain(&handler.execute(case.ctx(case.run)));
    let actor = caller(&case.snapshot);
    let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    let e = &state.effects[&case.id];
    let original_receipt = case.receipt();
    let original_source_revision =
        case.bundle.wi().effect_receipts(actor.tenant(), &case.id).unwrap()[0].2;
    let source = case
        .bundle
        .wi()
        .effect_receipt_source(actor.tenant(), &case.id, original_source_revision)
        .unwrap();
    let replay = case
        .bundle
        .wi()
        .effect_snapshot_at_revision(actor.tenant(), original_source_revision)
        .unwrap();
    let replay = &replay.effects[&case.id];
    assert_eq!(source.dispatch_index, 0);
    assert_eq!(&source.admission, replay.dispatches[0].admission.as_ref().unwrap());
    assert_eq!(&source.outcome, replay.outcomes.last().unwrap());
    // Persist an AQ/Fabric mismatch for the old dispatch. A later legitimate
    // noncommit retry must retain this diagnostic against its immutable source.
    let mut aq = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: case.bundle.aq_path(),
            ..Default::default()
        },
        Noop(Some("fixture.disposition_mismatch")),
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let _ = aq.run_until_idle().await.unwrap();
    });
    aq.shutdown().unwrap();
    assert_eq!(
        StoreBundle::open(&case.temp.path().join("bundle")).unwrap().convergence_faults().len(),
        1
    );
    let original_case = case.bundle.wi().reconciliation_cases(&actor, &case.id).unwrap().remove(0);
    case.bundle
        .wi()
        .reconciliation_apply(
            &actor,
            second(2),
            &original_case,
            worldinterface_domain::reconciliation::VerifiedCaseAction::from_trusted_runtime(
                worldinterface_domain::reconciliation::CaseAction::Start,
            ),
        )
        .unwrap();
    let running_case = case.bundle.wi().reconciliation_cases(&actor, &case.id).unwrap().remove(0);
    case.bundle
        .wi()
        .reconciliation_apply(
            &actor,
            second(2),
            &running_case,
            worldinterface_domain::reconciliation::VerifiedCaseAction::from_trusted_runtime(
                worldinterface_domain::reconciliation::CaseAction::Observe {
                    result:
                        worldinterface_domain::reconciliation::CaseStatus::ConfirmedNotCommitted,
                    evidence: e.outcomes[0].evidence.clone(),
                },
            ),
        )
        .unwrap();
    let mut snapshot = case.snapshot.clone();
    snapshot.policy.retry_after_noncommit = true;
    Egress::new(case.bundle.wi())
        .apply(
            &actor,
            second(2),
            EffectAction::PermitRetry { id: case.id.clone(), snapshot: verified(&snapshot) },
        )
        .unwrap();
    let mut queue = QueueEngine::bootstrap(&case.bundle, Noop(None), host()).unwrap();
    admission::prepare(&case.bundle, &actor, second(2), &case.id, verified(&snapshot)).unwrap();
    admission::deliver(&mut queue, &actor, second(2), &case.id, verified(&snapshot)).unwrap();
    run_discovery::discover(&queue, &actor, second(2), &case.id, 8).unwrap();
    let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    let new_run = state.effects[&case.id]
        .dispatches
        .last()
        .unwrap()
        .admission
        .as_ref()
        .unwrap()
        .observed_runs[0]
        .as_str()
        .parse()
        .unwrap();
    queue.shutdown().unwrap();
    assert!(StoreBundle::open(&case.temp.path().join("bundle"))
        .unwrap()
        .convergence_faults()
        .is_empty());
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(snapshot),
        clock: Clock,
    };
    assert!(matches!(handler.execute(case.ctx(new_run)).outcome(), DispositionOutcome::Complete));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let receipts = case.bundle.wi().effect_receipts(actor.tenant(), &case.id).unwrap();
    assert_eq!(receipts.len(), 2);
    for (index, (_, _, revision)) in receipts.iter().enumerate() {
        let source =
            case.bundle.wi().effect_receipt_source(actor.tenant(), &case.id, *revision).unwrap();
        let replay =
            case.bundle.wi().effect_snapshot_at_revision(actor.tenant(), *revision).unwrap();
        let replay = &replay.effects[&case.id];
        assert_eq!(source.dispatch_index, index);
        assert_eq!(
            &source.admission,
            replay.dispatches.last().unwrap().admission.as_ref().unwrap()
        );
        assert_eq!(&source.outcome, replay.outcomes.last().unwrap());
        assert_eq!(
            source.run.as_str(),
            if index == 0 { case.run.to_string() } else { new_run.to_string() }
        );
    }
    assert_ne!(receipts.last().unwrap().0, original_receipt.id);
    assert!(StoreBundle::open(&case.temp.path().join("bundle"))
        .unwrap()
        .convergence_faults()
        .is_empty());
    let c = rusqlite::Connection::open(case.temp.path().join("bundle/wi/fabric.sqlite")).unwrap();
    assert_eq!(
        c.query_row("SELECT count(*) FROM convergence_fault_history", [], |r| r.get::<_, usize>(0))
            .unwrap(),
        1
    );
    // An old run cannot consume the new dispatch's receipt.
    assert!(handler.execute_bound(&case.ctx(case.run)).is_err());
}
#[test]
fn reopen_rejects_outcomes_without_receipts_and_cross_direction_receipt_ids() {
    for corruption in ["collision", "missing"] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("bundle");
        let bundle = Arc::new(
            StoreBundle::initialize(
                &path,
                StoreProfile {
                    instance: InstanceId::generate(),
                    created_at: at(),
                    platform: false,
                },
            )
            .unwrap(),
        );
        let (submission, snapshot) = fixture();
        let id = authorize(&bundle, submission, &snapshot);
        complete(bundle.clone(), &id, &snapshot, vec![]);
        let receipt =
            bundle.wi().effect_receipts(caller(&snapshot).tenant(), &id).unwrap()[0].0.clone();
        let c = rusqlite::Connection::open(path.join("wi/fabric.sqlite")).unwrap();
        if corruption == "collision" {
            c.execute(
                "INSERT INTO ingress_receipt_index VALUES(?,?,?,?,?)",
                rusqlite::params![
                    receipt.as_str(),
                    caller(&snapshot).tenant().as_str(),
                    "collision",
                    "collision",
                    b"invalid".as_slice()
                ],
            )
            .unwrap();
        } else {
            c.execute_batch("DROP TRIGGER receipt_no_delete; DELETE FROM receipt_history; DELETE FROM receipt_index; CREATE TRIGGER receipt_no_delete BEFORE DELETE ON receipt_history BEGIN SELECT RAISE(ABORT,'append only'); END;").unwrap();
        }
        drop(c);
        assert!(
            worldinterface_storage::StoreRoot::open_existing(path.join("wi")).is_err(),
            "{corruption}"
        );
    }
}

#[path = "support/child.rs"]
mod child_guard;
#[test]
fn invocation_fence_rechecks_after_supersession() {
    const MODE: &str = "WI_INVOCATION_FENCE_CHILD";
    if let Some(path) = std::env::var_os(MODE) {
        let (_, snapshot) = fixture();
        let case = Prepared {
            temp: tempfile::tempdir().unwrap(),
            bundle: Arc::new(StoreBundle::open(std::path::Path::new(&path)).unwrap()),
            snapshot,
            id: std::env::var("WI_FENCE_INTENT").unwrap().try_into().unwrap(),
            run: std::env::var("WI_FENCE_RUN").unwrap().parse().unwrap(),
        };
        struct Forbidden;
        impl EffectExecutionPort for Forbidden {
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

            fn invoke(&self, _: BoundExecution, _: &CancellationToken) -> Result<PortObservation> {
                panic!("port reached after supersession")
            }
        }
        let handler = EffectAttemptHandler {
            bundle: case.bundle.clone(),
            port: Forbidden,
            evidence: Evidence(case.snapshot.clone()),
            clock: Clock,
        };
        let ctx = case.ctx(case.run);
        assert_uncertain(&handler.execute_bound(&ctx).unwrap());
        assert!(ctx.input.cancellation_context.token().is_cancelled());
        return;
    }
    let case = Prepared::new();
    let ready = case.temp.path().join("ready");
    let release = case.temp.path().join("release");
    let mut child = child_guard::KillOnDrop(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "invocation_fence_rechecks_after_supersession", "--nocapture"])
            .env(MODE, case.temp.path().join("bundle"))
            .env("WI_FENCE_INTENT", case.id.as_str())
            .env("WI_FENCE_RUN", case.run.to_string())
            .env("WI_CRASH_POINT", "fabric-start-committed")
            .env("WI_CRASH_READY", &ready)
            .env("WI_CRASH_RELEASE", &release)
            .spawn()
            .unwrap(),
    );
    let start = std::time::Instant::now();
    while !ready.exists() {
        assert!(child.try_wait().unwrap().is_none(), "child exited before Start barrier");
        assert!(start.elapsed() < std::time::Duration::from_secs(20));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = EffectAttemptHandler {
        bundle: case.bundle.clone(),
        port: Port { calls: calls.clone(), variant: "success" },
        evidence: Evidence(case.snapshot.clone()),
        clock: Clock,
    };
    assert_uncertain(&handler.execute(case.ctx(case.run)));
    std::fs::write(&release, b"release").unwrap();
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(start.elapsed() < std::time::Duration::from_secs(20));
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(case.receipt().temporal.committed_at, None);
}

#[test]
fn plain_effect_transaction_cannot_persist_a_receiptless_outcome() {
    let case = Prepared::new();
    let actor = caller(&case.snapshot);
    let attempt: OpaqueRef = AttemptId::new().to_string().try_into().unwrap();
    Egress::new(case.bundle.wi())
        .apply(
            &actor,
            second(2),
            EffectAction::StartProduction {
                id: case.id.clone(),
                snapshot: verified(&case.snapshot),
                run: case.run.to_string().try_into().unwrap(),
                attempt: attempt.clone(),
            },
        )
        .unwrap();
    let state = case.bundle.wi().effect_snapshot(actor.tenant()).unwrap();
    let command = worldinterface_domain::effect::validate_command(
        &state,
        &actor,
        second(2),
        EffectAction::Outcome {
            id: case.id.clone(),
            evidence: worldinterface_domain::effect::VerifiedOutcome::from_trusted_verifier(
                worldinterface_domain::effect::OutcomeEvidence {
                    classification: None,
                    reconciliation_admission: None,
                    intent: case.id.clone(),
                    attempt,
                    connector_semantics: state.effects[&case.id]
                        .resolution
                        .as_ref()
                        .unwrap()
                        .resolved
                        .idempotency_semantics
                        .clone(),
                    commit: CommitObservation::Unknown,
                    outcome: ExecutionOutcome::Uncertain,
                    evidence: vec![case.snapshot.grants[0].evidence.clone()],
                    required_outputs: case.snapshot.required_outputs.clone(),
                    durable_outputs: vec![],
                    boundary_record: None,
                },
            ),
        },
    )
    .unwrap();
    assert!(case
        .bundle
        .wi()
        .effect_transact(&command)
        .unwrap_err()
        .to_string()
        .contains("atomic receipt"));
    // A colliding ingress identity must reject the atomic completion write too,
    // before any outcome, receipt history or projection becomes durable.
    let receipt_id = ReceiptId::generate();
    let c = rusqlite::Connection::open(case.temp.path().join("bundle/wi/fabric.sqlite")).unwrap();
    c.execute(
        "INSERT INTO ingress_receipt_index VALUES(?,?,?,?,?)",
        rusqlite::params![
            receipt_id.as_str(),
            actor.tenant().as_str(),
            "collision",
            "collision",
            b"fixture".as_slice()
        ],
    )
    .unwrap();
    drop(c);
    let evidence_time = |at| TimeEvidence {
        at,
        source: "fixture-clock".to_string().try_into().unwrap(),
        assurance: "host-observed".to_string().try_into().unwrap(),
        uncertainty_nanos: 0,
    };
    let temporal = TemporalProvenance {
        occurred_at: None,
        observed_at: Some(evidence_time(second(2))),
        received_at: evidence_time(at()),
        admitted_at: evidence_time(second(1)),
        dispatched_at: Some(evidence_time(second(2))),
        committed_at: None,
        reconciled_at: None,
        sequence: None,
    };
    let execution = ExecutionRecord {
        classification: None,
        connector_execution: None,
        intent_id: case.id.clone(),
        attempt: state.effects[&case.id].attempt.clone().unwrap(),
        outcome: ExecutionOutcome::Uncertain,
        evidence: vec![case.snapshot.grants[0].evidence.clone()],
        recorded_at: second(2),
    };
    assert!(case
        .bundle
        .wi()
        .effect_transact_with_receipt(&command, receipt_id, &execution, temporal)
        .unwrap_err()
        .to_string()
        .contains("cross-direction"));
    assert_eq!(case.bundle.wi().effect_snapshot(actor.tenant()).unwrap(), state);
}
