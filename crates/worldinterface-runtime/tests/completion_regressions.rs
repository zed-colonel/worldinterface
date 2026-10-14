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
    effect::{CommitObservation, EffectAction, ReconciliationEvidence, VerifiedReconciliation},
    receipt::{DisclosureProfile, ReceiptAccess},
    Phase,
};
use worldinterface_egress::Egress;
use worldinterface_protocol::*;
use worldinterface_runtime::{
    admission,
    aq_adapter::QueueEngine,
    authority::{EvidenceAuthority, TrustedClock},
    completion::{
        BoundExecution, EffectAttemptHandler, EffectExecutionPort, EvidenceBytes, PortObservation,
    },
    run_discovery,
    storage::StoreBundle,
    Result,
};
use worldinterface_storage::StoreProfile;

struct Noop;
impl ExecutorHandler for Noop {
    fn execute(&self, _: ExecutorContext) -> actionqueue_core::disposition::AttemptDisposition {
        actionqueue_core::disposition::AttemptDisposition::complete(None)
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
    format!("2026-09-21T00:00:{n:02}.000000000Z").try_into().unwrap()
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
struct Port {
    calls: Arc<AtomicUsize>,
    variant: &'static str,
}
impl EffectExecutionPort for Port {
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
            "missing-evidence" => o.evidence.clear(),
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
        let mut queue = QueueEngine::bootstrap(&bundle, Noop, host()).unwrap();
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
        assert!(receipt.temporal.committed_at.is_none());
        assert!(receipt
            .evidence
            .iter()
            .any(|e| e.schema.as_str() == "wi.completion.uncertainty:1"));
        assert_uncertain(&handler.execute(case.ctx(case.run)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
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
    assert_eq!(temporal.reconciled_at.unwrap().uncertainty_nanos, 9_007_199_254_740_991);
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
    assert_eq!(temporal.committed_at.unwrap().at, second(3));
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
    let attempt = e.attempt.clone().unwrap();
    let original_receipt = case.receipt();
    Egress::new(case.bundle.wi())
        .apply(
            &actor,
            second(2),
            EffectAction::Reconcile {
                id: case.id.clone(),
                evidence: VerifiedReconciliation::from_trusted_verifier(ReconciliationEvidence {
                    intent: case.id.clone(),
                    attempt,
                    connector_semantics: e
                        .resolution
                        .as_ref()
                        .unwrap()
                        .resolved
                        .idempotency_semantics
                        .clone(),
                    result: ReconciliationResult::ConfirmedNotCommitted,
                    evidence: e.outcomes[0].evidence.clone(),
                }),
            },
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
    let mut queue = QueueEngine::bootstrap(&case.bundle, Noop, host()).unwrap();
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
    assert_ne!(receipts.last().unwrap().0, original_receipt.id);
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
