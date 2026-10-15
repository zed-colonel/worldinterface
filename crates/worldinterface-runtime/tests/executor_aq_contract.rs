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
