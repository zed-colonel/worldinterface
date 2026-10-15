//! Consumer proofs use AQ's published mutation, disposition and recovery authority.
mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use actionqueue_core::{
    admission::EnsureTaskRequest,
    bounded::{BoundedCode, BoundedError, BoundedMessage, ContentHash, HashAlgorithm},
    causal::{CausalContext, CausalOverride},
    continuation::*,
    data_ref::{DataRef, InlineData},
    disposition::{AttemptDisposition, ChildAdmission, DispositionOutcome, DispositionParts},
    ids::*,
    mutation::*,
    run::RunState,
    task::{
        constraints::TaskConstraints,
        metadata::TaskMetadata,
        run_policy::RunPolicy,
        task_spec::{TaskPayload, TaskSpec},
    },
};
use actionqueue_runtime::{
    claim,
    control::{self, ControlOperation},
    disposition, waits,
};
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    recovery::reducer::ReplayReducer,
    wal::{
        event::WalEvent,
        writer::{WalWriter, WalWriterError},
    },
};
use std::{
    io::{BufRead, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use support::spi_harness::host;

#[derive(Clone)]
struct Log {
    path: PathBuf,
    fail: Arc<AtomicBool>,
}
impl WalWriter for Log {
    fn append(&mut self, e: &WalEvent) -> Result<(), WalWriterError> {
        let mut f = std::fs::OpenOptions::new().append(true).create(true).open(&self.path).unwrap();
        serde_json::to_writer(&mut f, e).unwrap();
        f.write_all(b"\n").unwrap();
        Ok(())
    }
    fn flush(&mut self) -> Result<(), WalWriterError> {
        std::fs::File::open(&self.path).unwrap().sync_all().unwrap();
        if self.fail.swap(false, Ordering::SeqCst) {
            Err(WalWriterError::IoError("injected acknowledgment loss after durable flush".into()))
        } else {
            Ok(())
        }
    }
    fn close(self) -> Result<(), WalWriterError> {
        Ok(())
    }
}
type Authority = StorageMutationAuthority<Log, ReplayReducer>;
struct Clock;
impl actionqueue_core::time::clock::Clock for Clock {
    fn now(&self) -> u64 {
        100
    }
}
fn authority() -> (tempfile::TempDir, Authority) {
    let t = tempfile::tempdir().unwrap();
    let a = StorageMutationAuthority::new(
        Log { path: t.path().join("wal.jsonl"), fail: Arc::new(AtomicBool::new(false)) },
        ReplayReducer::new(),
    )
    .with_host(host());
    (t, a)
}
fn spec() -> TaskSpec {
    TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(b"proof".to_vec()),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap()
}
fn request(spec: TaskSpec, deps: Vec<TaskId>) -> EnsureTaskRequest {
    EnsureTaskRequest::new(
        AdmissionKey::new(format!("proof:{}", spec.id())).unwrap(),
        spec,
        deps,
        CausalContext::new(TraceId::new("proof").unwrap(), CorrelationId::new("proof").unwrap()),
        None,
    )
    .unwrap()
}
fn admit(a: &mut Authority, req: EnsureTaskRequest) -> RunId {
    let id = req.task_spec().id();
    let _ = actionqueue_runtime::admission::ensure_task(a, req, &Clock).unwrap();
    a.projection().run_ids_for_task(id)[0]
}
fn begin(a: &mut Authority, run: RunId, owner: &str) -> AttemptCommitExpectation {
    let seq = a.projection().latest_sequence() + 1;
    let _ = a
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                seq,
                run,
                RunState::Scheduled,
                RunState::Ready,
                100,
            )),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    let attempt = AttemptId::new();
    let fence = claim::accept(a, run, attempt, owner, 100, 1000).unwrap();
    AttemptCommitExpectation::new(
        a.projection().latest_sequence() + 1,
        run,
        attempt,
        RunState::Running,
        fence,
    )
}
fn inline(bytes: &[u8]) -> DataRef {
    DataRef::Inline(
        InlineData::new(
            None,
            bytes.to_vec(),
            ContentHash::new(
                HashAlgorithm::Sha256,
                <sha2::Sha256 as sha2::Digest>::digest(bytes).to_vec(),
            )
            .unwrap(),
        )
        .unwrap(),
    )
}
fn signal() -> SignalProposal {
    SignalProposal {
        signal_id: SignalId::new(uuid::Uuid::new_v4().to_string()).unwrap(),
        namespace: SignalNamespace::new("proof").unwrap(),
        kind: SignalKind::new("done").unwrap(),
        correlation_id: CorrelationId::new("proof").unwrap(),
        payload: None,
        payload_hash: None,
        occurred_at: None,
    }
}
fn wait() -> WaitSpec {
    WaitSpec::new(
        WaitId::new(),
        SignalFilter {
            tenant_id: None,
            namespace: SignalNamespace::new("proof").unwrap(),
            kind: SignalKind::new("never").unwrap(),
            correlation_id: Some(CorrelationId::new("proof").unwrap()),
            source_ref: None,
        },
        WaitMatchPolicy::FirstMatch,
        SignalEligibility::AnyRetained,
        Some(WaitDeadline { at: 200, policy: WaitTimeoutPolicy::ResumeWithTimeout }),
    )
    .unwrap()
}
fn checkpoint(attempt: AttemptId) -> CheckpointRef {
    CheckpointRef {
        checkpoint_id: CheckpointId::new(),
        data: inline(b"checkpoint"),
        created_by_attempt: attempt,
    }
}
fn awaiting(attempt: AttemptId) -> AttemptDisposition {
    AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(wait()),
            checkpoint: Some(checkpoint(attempt)),
            emitted_signals: vec![signal()],
            child_admissions: vec![ChildAdmission::new(
                AdmissionKey::new(format!("child:{attempt}")).unwrap(),
                spec(),
                vec![],
                CausalOverride::default(),
            )
            .unwrap()],
            ..Default::default()
        },
    )
    .unwrap()
}
fn minimal(a: &Authority, run: RunId, code: &str) {
    assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Failed));
    let record = &a
        .projection()
        .get_attempt_history(&run)
        .unwrap()
        .last()
        .unwrap()
        .disposition
        .as_ref()
        .unwrap()
        .disposition;
    assert!(
        matches!(record.outcome(),DispositionOutcome::TerminalFailure{error} if error.code.as_str()==code),
        "expected {code}, got {:?}",
        record.outcome()
    );
    assert!(
        record.output().is_none()
            && record.checkpoint().is_none()
            && record.wait().is_none()
            && record.child_admissions().is_empty()
            && record.emitted_signals().is_empty()
    );
}
#[test]
fn ab_wi_012_disposition_algebra_and_minimal_invalid_failure() {
    for kind in ["complete", "awaiting", "suspended"] {
        let (_t, mut a) = authority();
        let run = admit(&mut a, request(spec(), vec![]));
        let expected = begin(&mut a, run, "local");
        let attempt = expected.attempt_id();
        let d = match kind {
            "complete" => AttemptDisposition::new(
                DispositionOutcome::Complete,
                DispositionParts {
                    output: Some(inline(b"result")),
                    emitted_signals: vec![signal()],
                    ..Default::default()
                },
            )
            .unwrap(),
            "awaiting" => awaiting(attempt),
            _ => AttemptDisposition::suspended(Some(checkpoint(attempt)), None),
        };
        disposition::commit(&mut a, expected, d, 101).unwrap();
        assert_eq!(
            a.projection().get_run_state(&run),
            Some(&match kind {
                "complete" => RunState::Completed,
                "awaiting" => RunState::Awaiting,
                _ => RunState::Suspended,
            })
        );
        assert_eq!(a.projection().tasks().count(), if kind == "awaiting" { 2 } else { 1 });
        assert_eq!(
            a.projection().signals().statistics().retained,
            if kind == "suspended" { 0 } else { 1 }
        );
    }
    assert!(AttemptDisposition::new(
        DispositionOutcome::Complete,
        DispositionParts { wait: Some(wait()), ..Default::default() }
    )
    .is_err());
    assert!(AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(wait()),
            output: Some(inline(b"forbidden")),
            ..Default::default()
        }
    )
    .is_err());
    assert!(AttemptDisposition::new(
        DispositionOutcome::Suspended { reason: None },
        DispositionParts { emitted_signals: vec![signal()], ..Default::default() }
    )
    .is_err());
    let template = awaiting(AttemptId::new());
    for parts in [
        DispositionParts { checkpoint: template.checkpoint().cloned(), ..Default::default() },
        DispositionParts {
            child_admissions: template.child_admissions().to_vec(),
            ..Default::default()
        },
    ] {
        assert!(AttemptDisposition::new(DispositionOutcome::Complete, parts).is_err());
    }
    assert!(
        AttemptDisposition::new(DispositionOutcome::Awaiting, DispositionParts::default()).is_err()
    );
    for parts in [
        DispositionParts { wait: template.wait().cloned(), ..Default::default() },
        DispositionParts { output: Some(inline(b"forbidden")), ..Default::default() },
        DispositionParts {
            child_admissions: template.child_admissions().to_vec(),
            ..Default::default()
        },
    ] {
        assert!(
            AttemptDisposition::new(DispositionOutcome::Suspended { reason: None }, parts).is_err()
        );
    }
    let (_t, mut a) = authority();
    let run = admit(&mut a, request(spec(), vec![]));
    let expected = begin(&mut a, run, "local");
    let d = awaiting(expected.attempt_id());
    let mut limits = a.continuation_limits();
    limits.checkpoint_bytes = 0;
    a.set_continuation_limits(limits);
    disposition::commit(&mut a, expected, d, 101).unwrap();
    minimal(&a, run, "result_too_large");
    assert_eq!(a.projection().tasks().count(), 1);
    assert_eq!(a.projection().waits().active_count(), 0);
    assert_eq!(a.projection().signals().statistics().retained, 0);
}
#[test]
fn ab_wi_014_capacity_remote_fence_stale_and_uncertain() {
    let (_t, mut a) = authority();
    let run = admit(&mut a, request(spec(), vec![]));
    let expected = begin(&mut a, run, "local");
    let mut limits = a.continuation_limits();
    limits.active_waits = 0;
    a.set_continuation_limits(limits);
    let d = awaiting(expected.attempt_id());
    disposition::commit(&mut a, expected.clone(), d, 101).unwrap();
    minimal(&a, run, "wait_capacity");
    assert_eq!(a.projection().tasks().count(), 1);
    assert_eq!(a.projection().signals().statistics().retained, 0);
    let seq = a.projection().latest_sequence();
    assert!(disposition::commit(&mut a, expected, AttemptDisposition::complete(None), 101).is_err());
    assert_eq!(a.projection().latest_sequence(), seq);
    let (_t, mut a) = authority();
    let actor = ActorId::new();
    let _ = a
        .submit_command(
            MutationCommand::ActorRegister(ActorRegisterCommand::new(
                1,
                actionqueue_core::actor::ActorRegistration::new(
                    actor,
                    "remote",
                    actionqueue_core::executor::ExecutorTraits::new(vec!["proof".into()]).unwrap(),
                    10,
                ),
                100,
            )),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    let run = admit(&mut a, request(spec(), vec![]));
    let expected = begin(&mut a, run, &format!("actor:{actor}"));
    let mut remote_host = host();
    remote_host.actor_id = Some(actor);
    let remote = |d: &AttemptDisposition| RemoteResultExpectation {
        host: remote_host.clone(),
        protocol_version: 1,
        contract_revision: "AQ-CONT-1-r2".into(),
        digest: actionqueue_core::disposition_digest::disposition_digest(d),
    };
    let mut limits = a.continuation_limits();
    limits.active_waits = 0;
    a.set_continuation_limits(limits);
    let d = awaiting(expected.attempt_id());
    let seq = a.projection().latest_sequence();
    assert!(
        disposition::commit_remote(&mut a, expected.clone(), d.clone(), 101, remote(&d)).is_err()
    );
    assert_eq!(a.projection().latest_sequence(), seq);
    assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Running));
    assert_eq!(
        a.projection().get_lease_metadata(&run).unwrap().granted_at_sequence(),
        expected.expected_lease().granted_at_sequence()
    );
    let corrected = AttemptDisposition::complete(None);
    disposition::commit_remote(&mut a, expected, corrected.clone(), 102, remote(&corrected))
        .unwrap();
    assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Completed));
    let (_t, mut a) = authority();
    let run = admit(&mut a, request(spec(), vec![]));
    let expected = begin(&mut a, run, "local");
    disposition::commit(&mut a, expected.clone(), AttemptDisposition::awaiting(wait(), None), 101)
        .unwrap();
    let mut limits = a.continuation_limits();
    limits.active_waits = 0;
    limits.active_waits_per_tenant = 0;
    a.set_continuation_limits(limits);
    assert_eq!(waits::reconcile(&mut a, 201).unwrap(), 1);
    assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Ready));
    let (_t, mut a) = authority();
    let run = admit(&mut a, request(spec(), vec![]));
    let expected = begin(&mut a, run, "local");
    let (writer, p) = a.into_parts();
    writer.fail.store(true, Ordering::SeqCst);
    let mut a = StorageMutationAuthority::new(writer, p).with_host(host());
    assert!(disposition::commit(&mut a, expected, AttemptDisposition::complete(None), 101).is_err());
    assert!(a.recovery_required());
    let seq = a.projection().latest_sequence();
    let expected = AttemptCommitExpectation::new(
        seq + 1,
        run,
        AttemptId::new(),
        RunState::Running,
        LeaseFence::new("local".into(), 1),
    );
    assert!(disposition::commit(&mut a, expected, AttemptDisposition::complete(None), 102).is_err());
    assert_eq!(a.projection().latest_sequence(), seq);
}
#[test]
fn ab_wi_020_terminal_cancel_is_append_free_and_has_no_late_cascade() {
    for failed in [false, true] {
        let (_t, mut a) = authority();
        let task = spec();
        let task_id = task.id();
        let run = admit(&mut a, request(task, vec![]));
        let expected = begin(&mut a, run, "local");
        let d = if failed {
            AttemptDisposition::terminal_failure(BoundedError {
                code: BoundedCode::new("provider_failure").unwrap(),
                message: BoundedMessage::new("failed").unwrap(),
            })
        } else {
            AttemptDisposition::complete(None)
        };
        disposition::commit(&mut a, expected, d, 101).unwrap();
        let dependent = admit(&mut a, request(spec(), vec![task_id]));
        let seq = a.projection().latest_sequence();
        for target in [CancelTarget::Task(task_id), CancelTarget::Run(run)] {
            let e =
                control::execute_control(&mut a, &host(), ControlOperation::Cancel(target), &Clock)
                    .unwrap_err();
            assert_eq!(e.code(), "already_terminal");
            assert_eq!(a.projection().latest_sequence(), seq);
        }
        if !failed {
            waits::recover_cancellations(&mut a, 102).unwrap();
            assert_eq!(a.projection().get_run_state(&dependent), Some(&RunState::Scheduled));
        }
    }
    let (_t, mut a) = authority();
    let run = admit(&mut a, request(spec(), vec![]));
    let _ = control::execute_control(
        &mut a,
        &host(),
        ControlOperation::Cancel(CancelTarget::Run(run)),
        &Clock,
    )
    .unwrap();
    let seq = a.projection().latest_sequence();
    let _ = control::execute_control(
        &mut a,
        &host(),
        ControlOperation::Cancel(CancelTarget::Run(run)),
        &Clock,
    )
    .unwrap();
    assert_eq!(a.projection().latest_sequence(), seq);
}
#[test]
fn ab_wi_023_actual_uncertain_aq_mutation_recovers_stable_identity() {
    let (_t, a) = authority();
    let (writer, p) = a.into_parts();
    writer.fail.store(true, Ordering::SeqCst);
    let path = writer.path.clone();
    let mut a = StorageMutationAuthority::new(writer, p).with_host(host());
    let request = request(spec(), vec![]);
    let e = control::execute_control(
        &mut a,
        &host(),
        ControlOperation::AdmitTask(request.clone()),
        &Clock,
    )
    .unwrap_err();
    assert_eq!(e.code(), "storage_unavailable");
    assert!(a.recovery_required());
    let (writer, _) = a.into_parts();
    drop(writer);
    let mut p = ReplayReducer::new();
    for line in std::io::BufReader::new(std::fs::File::open(&path).unwrap()).lines() {
        p.apply(&serde_json::from_str::<WalEvent>(&line.unwrap()).unwrap()).unwrap();
    }
    let seq = p.latest_sequence();
    let writer = Log { path, fail: Arc::new(AtomicBool::new(false)) };
    let mut recovered = StorageMutationAuthority::new(writer, p).with_host(host());
    let outcome = control::execute_control(
        &mut recovered,
        &host(),
        ControlOperation::AdmitTask(request.clone()),
        &Clock,
    )
    .unwrap();
    let control::ControlOutcome::Task(outcome) = outcome else { panic!("task") };
    assert!(!outcome.is_created());
    assert_eq!(outcome.task_id(), request.task_spec().id());
    assert_eq!(recovered.projection().latest_sequence(), seq);
    assert_eq!(recovered.projection().tasks().count(), 1);
}
#[tokio::test]
async fn ab_wi_020_023_provider_ack_loss_is_uncertain_until_reconciled() {
    use support::{connectors::*, spi_harness::Harness};
    use worldinterface_domain::{reconciliation::CaseStatus, Phase};
    use worldinterface_protocol::IdempotencyClass;
    let h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    h.execute().await;
    assert_eq!(h.effect().phase, Phase::Uncertain);
    let mut engine = actionqueue_runtime::engine::ActionQueueEngine::new(
        actionqueue_runtime::config::RuntimeConfig {
            data_dir: h.bundle.aq_path(),
            ..Default::default()
        },
        support::spi_harness::Noop,
    )
    .bootstrap()
    .unwrap()
    .with_host(host());
    let error = engine.control(ControlOperation::Cancel(CancelTarget::Run(h.run))).unwrap_err();
    assert_eq!(error.code(), "already_terminal");
    engine.shutdown().unwrap();
    assert_eq!(h.effect().phase, Phase::Uncertain);
    h.reconcile().await;
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    assert_eq!(h.connector.commits(), 1);
}

#[tokio::test]
async fn ab_wi_020_queue_cancel_keeps_started_external_effect_uncertain() {
    use actionqueue_runtime::{config::RuntimeConfig, engine::ActionQueueEngine};
    use std::{sync::Mutex, time::Duration};
    use support::{
        connectors::*,
        spi_harness::{AqClock, Harness},
    };
    use worldinterface_domain::{reconciliation::CaseStatus, Phase};
    use worldinterface_protocol::IdempotencyClass;
    let mut h = Harness::new(IdempotencyClass::ExternallyKeyed, Mode::Ambiguous);
    let (entered, ready) = std::sync::mpsc::channel();
    let (release, held) = std::sync::mpsc::channel();
    let held = Arc::new(Mutex::new(held));
    h.connector.after_commit = Some(Arc::new(move || {
        entered.send(()).unwrap();
        held.lock().unwrap().recv_timeout(Duration::from_secs(10)).unwrap();
    }));
    let mut engine = ActionQueueEngine::new(
        RuntimeConfig {
            data_dir: h.bundle.aq_path(),
            tick_interval: Duration::from_millis(1),
            ..Default::default()
        },
        h.handler(),
    )
    .bootstrap_with_clock(AqClock(h.clock.clone()))
    .unwrap()
    .with_host(host());
    let mut accepted = false;
    for _ in 0..3000 {
        let _ = engine.tick().await.unwrap();
        if ready.try_recv().is_ok() {
            accepted = true;
            break;
        }
        if engine.projection().get_run_state(&h.run) == Some(&RunState::Scheduled) {
            h.clock.advance(1000);
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(accepted);
    assert_eq!(h.connector.commits(), 1);
    assert_eq!(h.effect().phase, Phase::Running);
    let _ = engine.control(ControlOperation::Cancel(CancelTarget::Run(h.run))).unwrap();
    release.send(()).unwrap();
    for _ in 0..3000 {
        let _ = engine.tick().await.unwrap();
        if h.effect().phase == Phase::Uncertain {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(engine.projection().get_run_state(&h.run), Some(&RunState::Canceled));
    assert_eq!(h.effect().phase, Phase::Uncertain);
    engine.shutdown().unwrap();
    h.reconcile().await;
    assert_eq!(h.case().status, CaseStatus::ConfirmedCommitted);
    assert_eq!(h.connector.commits(), 1);
}

fn replay_log(path: &std::path::Path) -> ReplayReducer {
    let mut p = ReplayReducer::new();
    for line in std::io::BufReader::new(std::fs::File::open(path).unwrap()).lines() {
        p.apply(&serde_json::from_str::<WalEvent>(&line.unwrap()).unwrap()).unwrap();
    }
    p
}
struct At(u64);
impl actionqueue_core::time::clock::Clock for At {
    fn now(&self) -> u64 {
        self.0
    }
}
fn child_set(parent: TaskId, count: usize) -> Vec<ChildAdmission> {
    let ids: Vec<_> = (0..count).map(|_| TaskId::new()).collect();
    ids.iter()
        .enumerate()
        .map(|(i, id)| {
            ChildAdmission::new(
                AdmissionKey::new(format!("child:{i}")).unwrap(),
                TaskSpec::new(
                    *id,
                    TaskPayload::new(b"node".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .unwrap()
                .with_parent(parent),
                if i == 0 { vec![] } else { vec![ids[i - 1]] },
                CausalOverride::default(),
            )
            .unwrap()
        })
        .collect()
}
fn compound(attempt: AttemptId, children: Vec<ChildAdmission>) -> AttemptDisposition {
    AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            wait: Some(
                WaitSpec::children(
                    WaitId::new(),
                    children.iter().map(|c| c.task_spec().id()).collect(),
                    ChildWaitPolicy::AllTerminal,
                    None,
                )
                .unwrap(),
            ),
            checkpoint: Some(checkpoint(attempt)),
            child_admissions: children,
            ..Default::default()
        },
    )
    .unwrap()
}
#[test]
fn ab_wi_005_bounds_are_enforced_by_aq_admission_and_disposition() {
    // Initial runs: exercise the service, not only policy construction.
    for count in [64, 65] {
        let (_t, mut a) = authority();
        let s = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(vec![]),
            RunPolicy::repeat(count, 1).unwrap(),
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap();
        let req = EnsureTaskRequest::new(
            AdmissionKey::new("bound").unwrap(),
            s,
            vec![],
            CausalContext::new(
                TraceId::new("proof").unwrap(),
                CorrelationId::new("proof").unwrap(),
            ),
            None,
        );
        let result = req.map_err(|e| e.to_string()).and_then(|req| {
            actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock)
                .map_err(|e| e.to_string())
        });
        assert_eq!(result.is_ok(), count == 64);
        assert_eq!(a.projection().run_count(), if count == 64 { 64 } else { 0 });
    }
    for count in [64, 65] {
        let (_t, mut a) = authority();
        let deps: Vec<_> = (0..count)
            .map(|_| {
                let s = spec();
                let id = s.id();
                admit(&mut a, request(s, vec![]));
                id
            })
            .collect();
        let s = spec();
        let task = s.id();
        match EnsureTaskRequest::new(
            AdmissionKey::new("dependencies").unwrap(),
            s,
            deps,
            CausalContext::new(
                TraceId::new("proof").unwrap(),
                CorrelationId::new("proof").unwrap(),
            ),
            None,
        ) {
            Ok(req) => {
                assert_eq!(count, 64);
                actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock).unwrap();
                assert!(a.projection().task_admission(task).is_some());
            }
            Err(_) => {
                assert_eq!(count, 65);
                assert!(a.projection().task_admission(task).is_none());
            }
        }
    }
    for size in [65_536, 65_537] {
        let (_t, mut a) = authority();
        let s = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(vec![0; size]),
            RunPolicy::Once,
            TaskConstraints::default(),
            TaskMetadata::default(),
        )
        .unwrap();
        let req = EnsureTaskRequest::new(
            AdmissionKey::new("bound").unwrap(),
            s,
            vec![],
            CausalContext::new(
                TraceId::new("proof").unwrap(),
                CorrelationId::new("proof").unwrap(),
            ),
            None,
        );
        let result = req.map_err(|e| e.to_string()).and_then(|req| {
            actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock)
                .map_err(|e| e.to_string())
        });
        assert_eq!(result.is_ok(), size == 65_536);
        assert_eq!(a.projection().task_count(), usize::from(size == 65_536));
    }
    for count in [64, 65] {
        let (_t, mut a) = authority();
        let parent = spec();
        let task = parent.id();
        let run = admit(&mut a, request(parent, vec![]));
        let expected = begin(&mut a, run, "local");
        let children = child_set(task, count);
        let mut limits = a.continuation_limits();
        limits.disposition_bytes = 1024 * 1024;
        a.set_continuation_limits(limits);
        let d = AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                wait: Some(wait()),
                checkpoint: Some(checkpoint(expected.attempt_id())),
                child_admissions: children.clone(),
                ..Default::default()
            },
        );
        if count == 64 {
            disposition::commit(&mut a, expected, d.unwrap(), 101).unwrap();
            assert_eq!(a.projection().task_count(), 65);
            assert_eq!(a.projection().waits().active_count(), 1);
        } else {
            assert!(d.is_err());
            assert_eq!(a.projection().task_count(), 1);
            assert_eq!(a.projection().waits().active_count(), 0);
        }
        assert_eq!(
            WaitSpec::children(
                WaitId::new(),
                children.iter().map(|c| c.task_spec().id()).collect(),
                ChildWaitPolicy::AllTerminal,
                None
            )
            .is_ok(),
            count == 64
        );
    }
    for count in [32, 33] {
        let (_t, mut a) = authority();
        let run = admit(&mut a, request(spec(), vec![]));
        let expected = begin(&mut a, run, "local");
        let d = AttemptDisposition::new(
            DispositionOutcome::Complete,
            DispositionParts {
                emitted_signals: (0..count).map(|_| signal()).collect(),
                ..Default::default()
            },
        );
        if count == 32 {
            disposition::commit(&mut a, expected, d.unwrap(), 101).unwrap();
            assert_eq!(a.projection().signals().statistics().retained, 32);
        } else {
            assert!(d.is_err());
            assert_eq!(a.projection().signals().statistics().retained, 0);
        }
    }
    let (_t, mut a) = authority();
    let root = spec();
    let mut parent = root.id();
    admit(&mut a, request(root, vec![]));
    for depth in 1..=9 {
        let s = spec().with_parent(parent);
        let id = s.id();
        let req = EnsureTaskRequest::new(
            AdmissionKey::new(format!("depth:{depth}")).unwrap(),
            s,
            vec![],
            CausalContext::new(
                TraceId::new("proof").unwrap(),
                CorrelationId::new("proof").unwrap(),
            ),
            None,
        );
        let result = req.map_err(|e| e.to_string()).and_then(|req| {
            actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock)
                .map_err(|e| e.to_string())
        });
        assert_eq!(result.is_ok(), depth <= 8, "depth {depth}");
        if depth <= 8 {
            parent = id;
        } else {
            assert!(a.projection().task_admission(id).is_none());
        }
    }
}
#[test]
fn ab_wi_005_compound_commit_replay_is_all_or_none_with_stable_children() {
    for lost_ack in [false, true] {
        let (t, mut a) = authority();
        let parent = spec();
        let task = parent.id();
        let run = admit(&mut a, request(parent, vec![]));
        let expected = begin(&mut a, run, "local");
        let mut limits = a.continuation_limits();
        limits.disposition_bytes = 1024 * 1024;
        a.set_continuation_limits(limits);
        let children = child_set(task, 64);
        let d = compound(expected.attempt_id(), children.clone());
        // A prepared WI proposal alone has no AQ effects, including after replay.
        let before = replay_log(&t.path().join("wal.jsonl"));
        assert_eq!(before.task_count(), 1);
        assert_eq!(before.waits().active_count(), 0);
        let (writer, p) = a.into_parts();
        writer.fail.store(lost_ack, Ordering::SeqCst);
        let path = writer.path.clone();
        let mut a = StorageMutationAuthority::new(writer, p).with_host(host());
        let mut limits = a.continuation_limits();
        limits.disposition_bytes = 1024 * 1024;
        a.set_continuation_limits(limits);
        let result = disposition::commit(&mut a, expected, d.clone(), 101);
        assert_eq!(result.is_err(), lost_ack);
        let recovered = replay_log(&path);
        assert_eq!(recovered.task_count(), 65);
        assert_eq!(recovered.waits().active_count(), 1);
        assert_eq!(recovered.get_run_state(&run), Some(&RunState::Awaiting));
        let record = recovered
            .get_attempt_history(&run)
            .unwrap()
            .last()
            .unwrap()
            .disposition
            .as_ref()
            .unwrap();
        assert_eq!(record.disposition, d);
        assert_eq!(recovered.parent_child_mappings().count(), 64);
        assert_eq!(
            recovered.dependency_declarations().filter(|(_, deps)| !deps.is_empty()).count(),
            63
        );
        for child in children {
            assert!(recovered.task_admission(child.task_spec().id()).is_some());
            assert_eq!(recovered.run_ids_for_task(child.task_spec().id()).len(), 1);
        }
    }
}
#[test]
fn ab_wi_010_deadline_signal_orders_and_timeout_policies_survive_replay() {
    use actionqueue_storage::mutation::wait::WaitResolutionKind;
    for policy in [
        WaitTimeoutPolicy::ResumeWithTimeout,
        WaitTimeoutPolicy::FailRun {
            code: actionqueue_core::bounded::BoundedCode::new("deadline").unwrap(),
        },
        WaitTimeoutPolicy::CancelRun,
    ] {
        for order in ["early", "signal-first", "deadline-first"] {
            let (t, mut a) = authority();
            let run = admit(&mut a, request(spec(), vec![]));
            let expected = begin(&mut a, run, "local");
            let id = WaitId::new();
            let mut w = wait();
            w = WaitSpec::new(
                id,
                w.filter().unwrap().clone(),
                WaitMatchPolicy::FirstMatch,
                SignalEligibility::AnyRetained,
                Some(WaitDeadline { at: 200, policy: policy.clone() }),
            )
            .unwrap();
            let s = AdmitSignalRequest::new(
                SignalId::new(format!("race:{order}")).unwrap(),
                SignalNamespace::new("proof").unwrap(),
                SignalKind::new("never").unwrap(),
                Some(CorrelationId::new("proof").unwrap()),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
            if order == "early" {
                control::execute_control(
                    &mut a,
                    &host(),
                    ControlOperation::AdmitSignal(s.clone()),
                    &Clock,
                )
                .unwrap();
            }
            let expected = AttemptCommitExpectation::new(
                a.projection().latest_sequence() + 1,
                run,
                expected.attempt_id(),
                RunState::Running,
                expected.expected_lease().clone(),
            );
            disposition::commit(&mut a, expected, AttemptDisposition::awaiting(w, None), 101)
                .unwrap();
            if order != "early" {
                assert_eq!(waits::reconcile(&mut a, 199).unwrap(), 0);
            }
            if order == "deadline-first" {
                assert_eq!(waits::reconcile(&mut a, 200).unwrap(), 1);
            }
            if order != "early" {
                control::execute_control(
                    &mut a,
                    &host(),
                    ControlOperation::AdmitSignal(s),
                    &At(200),
                )
                .unwrap();
            }
            waits::reconcile(&mut a, 200).unwrap();
            let resolution = a.projection().waits().get(id).unwrap().resolution.clone().unwrap();
            assert_eq!(
                matches!(resolution.kind, WaitResolutionKind::Deadline),
                order == "deadline-first"
            );
            let before = a.projection().latest_sequence();
            waits::reconcile(&mut a, 500).unwrap();
            assert_eq!(a.projection().latest_sequence(), before);
            let p = replay_log(&t.path().join("wal.jsonl"));
            assert_eq!(p.waits().get(id).unwrap().resolution.as_ref(), Some(&resolution));
            assert_eq!(
                p.get_run_state(&run),
                Some(
                    &if order != "deadline-first" || policy == WaitTimeoutPolicy::ResumeWithTimeout
                    {
                        RunState::Ready
                    } else if matches!(policy, WaitTimeoutPolicy::FailRun { .. }) {
                        RunState::Failed
                    } else {
                        RunState::Canceled
                    }
                )
            );
        }
    }
}
#[test]
fn ab_wi_013_required_detached_closure_cancel_and_mixed_runs() {
    use actionqueue_core::{
        continuation::TaskTerminalStatus, task::task_spec::ChildLifecyclePolicy,
    };
    for failed in [false, true] {
        let (_t, mut a) = authority();
        let parent = spec();
        let parent_task = parent.id();
        let run = admit(&mut a, request(parent, vec![]));
        let expected = begin(&mut a, run, "local");
        let children = child_set(parent_task, 1);
        let child_task = children[0].task_spec().id();
        let detached = spec().with_parent_policy(parent_task, ChildLifecyclePolicy::Detached);
        let detached_task = detached.id();
        let mut d = compound(expected.attempt_id(), children.clone());
        d = AttemptDisposition::new(
            DispositionOutcome::Awaiting,
            DispositionParts {
                checkpoint: d.checkpoint().cloned(),
                wait: d.wait().cloned(),
                child_admissions: vec![
                    children[0].clone(),
                    ChildAdmission::new(
                        AdmissionKey::new("detached").unwrap(),
                        detached,
                        vec![],
                        CausalOverride::default(),
                    )
                    .unwrap(),
                ],
                ..Default::default()
            },
        )
        .unwrap();
        disposition::commit(&mut a, expected, d, 101).unwrap();
        let child_run = a.projection().run_ids_for_task(child_task)[0];
        assert!(!a.projection().required_children_terminal(parent_task));
        if failed {
            let expected = begin(&mut a, child_run, "local");
            disposition::commit(
                &mut a,
                expected,
                AttemptDisposition::terminal_failure(BoundedError {
                    code: BoundedCode::new("domain-denied").unwrap(),
                    message: BoundedMessage::new("denied").unwrap(),
                }),
                102,
            )
            .unwrap();
        } else {
            control::execute_control(
                &mut a,
                &host(),
                ControlOperation::Cancel(CancelTarget::Task(child_task)),
                &At(103),
            )
            .unwrap();
        }
        waits::reconcile(&mut a, 103).unwrap();
        assert!(a.projection().required_children_terminal(parent_task));
        assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Ready));
        assert!(a.projection().task_terminal_status(detached_task).is_none());
        // Cascading parent cancellation reaches required children, never detached ones.
        control::execute_control(
            &mut a,
            &host(),
            ControlOperation::Cancel(CancelTarget::Task(parent_task)),
            &At(104),
        )
        .unwrap();
        waits::reconcile(&mut a, 104).unwrap();
        assert!(!a.projection().is_task_canceled(detached_task));
    }
    let (_t, mut a) = authority();
    let parent = spec();
    let parent_id = parent.id();
    admit(&mut a, request(parent, vec![]));
    let required = spec().with_parent(parent_id);
    let required_id = required.id();
    admit(&mut a, request(required, vec![]));
    let detached = spec().with_parent_policy(parent_id, ChildLifecyclePolicy::Detached);
    let detached_id = detached.id();
    admit(&mut a, request(detached, vec![]));
    control::execute_control(
        &mut a,
        &host(),
        ControlOperation::Cancel(CancelTarget::Task(parent_id)),
        &At(105),
    )
    .unwrap();
    waits::reconcile(&mut a, 105).unwrap();
    assert!(a.projection().is_task_canceled(required_id));
    assert!(!a.projection().is_task_canceled(detached_id));
    let (_t, mut a) = authority();
    let task = TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(vec![]),
        RunPolicy::repeat(2, 1).unwrap(),
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap();
    let id = task.id();
    admit(&mut a, request(task, vec![]));
    let runs = a.projection().run_ids_for_task(id);
    let e = begin(&mut a, runs[0], "local");
    disposition::commit(&mut a, e, AttemptDisposition::complete(None), 101).unwrap();
    let e = begin(&mut a, runs[1], "local");
    disposition::commit(
        &mut a,
        e,
        AttemptDisposition::terminal_failure(BoundedError {
            code: BoundedCode::new("denied").unwrap(),
            message: BoundedMessage::new("denied").unwrap(),
        }),
        102,
    )
    .unwrap();
    assert_eq!(a.projection().task_terminal_status(id), Some(TaskTerminalStatus::Succeeded));
    assert!(runs.iter().any(|r| a.projection().get_run_state(r) == Some(&RunState::Failed)));
}
