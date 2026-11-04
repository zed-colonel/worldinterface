//! Consumer proofs use AQ's published mutation, disposition and recovery authority.
mod support {
    pub mod connectors;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
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
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use support::spi_harness::host;

struct Log {
    inner: Option<actionqueue_storage::wal::fs_writer::WalFsWriter>,
    path: PathBuf,
    fail: Arc<AtomicBool>,
}
impl WalWriter for Log {
    fn store_session(&self) -> Option<&actionqueue_storage::store::StoreSession> {
        self.inner.as_ref().and_then(WalWriter::store_session)
    }
    fn append(&mut self, e: &WalEvent) -> Result<(), WalWriterError> {
        if let Some(inner) = &mut self.inner {
            inner.append(e)
        } else {
            use std::io::Write;
            let mut file =
                std::fs::OpenOptions::new().append(true).create(true).open(&self.path).unwrap();
            serde_json::to_writer(&mut file, e).unwrap();
            file.write_all(b"\n").unwrap();
            Ok(())
        }
    }
    fn flush(&mut self) -> Result<(), WalWriterError> {
        if let Some(inner) = &mut self.inner {
            inner.flush()?;
        } else {
            std::fs::File::open(&self.path).unwrap().sync_all().unwrap();
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            Err(WalWriterError::IoError("injected acknowledgment loss after durable flush".into()))
        } else {
            Ok(())
        }
    }
    fn close(self) -> Result<(), WalWriterError> {
        self.inner.map_or(Ok(()), WalWriter::close)
    }
}
fn log(path: PathBuf, initialize: bool) -> Log {
    let session = actionqueue_storage::store::open_store(
        &path,
        if initialize {
            actionqueue_storage::store::OpenOptions::Initialize { features: vec![] }
        } else {
            actionqueue_storage::store::OpenOptions::ReadWrite
        },
    )
    .unwrap();
    Log {
        inner: Some(actionqueue_storage::wal::fs_writer::WalFsWriter::new(session).unwrap()),
        path,
        fail: Arc::new(AtomicBool::new(false)),
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
        log(t.path().join("aq"), true),
        replay_log(&t.path().join("aq")),
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
    let _t = tempfile::tempdir().unwrap();
    let mut a = StorageMutationAuthority::new(
        Log {
            inner: None,
            path: _t.path().join("actor-mutation.jsonl"),
            fail: Arc::new(AtomicBool::new(false)),
        },
        ReplayReducer::new(),
    )
    .with_host(host());
    let actor = ActorId::new();
    let _ = a
        .submit_command(
            MutationCommand::ActorRegister(ActorRegisterCommand::new(
                a.projection().latest_sequence() + 1,
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
    let p = replay_log(&path);
    let seq = p.latest_sequence();
    let writer = log(path, false);
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
    use actionqueue_storage::wal::reader::WalReader;
    let mut reader =
        actionqueue_storage::wal::fs_reader::WalFsReader::new(path.join("wal/actionqueue.wal"))
            .unwrap();
    let mut p = ReplayReducer::new();
    while let Some(event) = reader.read_next().unwrap() {
        p.apply(&event).unwrap();
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
        let result = req
            .map_err(actionqueue_runtime::admission::AdmissionError::Rejected)
            .and_then(|req| actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock));
        assert_eq!(result.is_ok(), count == 64);
        if count == 65 {
            assert!(matches!(
                result,
                Err(actionqueue_runtime::admission::AdmissionError::Rejected(
                    actionqueue_core::admission::AdmissionRejection::TooLarge
                ))
            ));
        }
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
            Err(e) => {
                assert_eq!(e, actionqueue_core::admission::AdmissionRejection::TooLarge);
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
        let result = req
            .map_err(actionqueue_runtime::admission::AdmissionError::Rejected)
            .and_then(|req| actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock));
        assert_eq!(result.is_ok(), size == 65_536);
        if size == 65_537 {
            assert!(matches!(
                result,
                Err(actionqueue_runtime::admission::AdmissionError::Rejected(
                    actionqueue_core::admission::AdmissionRejection::TooLarge
                ))
            ));
        }
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
            assert_eq!(d.unwrap_err(), actionqueue_core::disposition::DispositionError::TooLarge);
            assert_eq!(a.projection().task_count(), 1);
            assert_eq!(a.projection().waits().active_count(), 0);
        }
        let child_wait = WaitSpec::children(
            WaitId::new(),
            children.iter().map(|c| c.task_spec().id()).collect(),
            ChildWaitPolicy::AllTerminal,
            None,
        );
        if count == 64 {
            child_wait.unwrap();
        } else {
            assert_eq!(child_wait.unwrap_err(), actionqueue_core::continuation::WaitSpecError);
        }
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
            assert_eq!(d.unwrap_err(), actionqueue_core::disposition::DispositionError::TooLarge);
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
        let result = req
            .map_err(actionqueue_runtime::admission::AdmissionError::Rejected)
            .and_then(|req| actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock));
        assert_eq!(result.is_ok(), depth <= 8, "depth {depth}");
        if depth <= 8 {
            parent = id;
        } else {
            assert!(matches!(
                result,
                Err(actionqueue_runtime::admission::AdmissionError::Rejected(
                    actionqueue_core::admission::AdmissionRejection::HierarchyDepth
                ))
            ));
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
        // Before disposition commit, the native AQ WAL contains only the parent.
        let before = replay_log(&t.path().join("aq"));
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
            if order == "deadline-first" {
                assert_eq!(resolution.kind, WaitResolutionKind::Deadline);
            } else {
                let WaitResolutionKind::Signal(sequence) = resolution.kind else {
                    panic!("expected signal winner");
                };
                assert_eq!(
                    a.projection().signals().by_sequence(sequence).unwrap().envelope().signal_id,
                    SignalId::new(format!("race:{order}")).unwrap()
                );
            }
            let before = a.projection().latest_sequence();
            waits::reconcile(&mut a, 500).unwrap();
            assert_eq!(a.projection().latest_sequence(), before);
            let p = replay_log(&t.path().join("aq"));
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

/// AB-WI-021: AQ's key reservation is independent of a live WI worker reservation.
#[tokio::test]
async fn ab_wi_021_awaiting_release_hold_and_resume_keep_durable_worker_capacity() {
    use actionqueue_core::task::constraints::ConcurrencyKeyWaitPolicy;
    use worldinterface_worker::mapping::Operation;
    for hold in [false, true] {
        let h = support::worker_fixture::Harness::new(
            worldinterface_runtime::worker_process::Task::Sleep { millis: 5000 },
            "AB-WI-021",
            1500,
            65536,
        );
        h.operation(Operation::Start).await;
        let aq_path = h.temp.path().join("bundle/aq");
        let mut a =
            StorageMutationAuthority::new(log(aq_path.clone(), false), replay_log(&aq_path))
                .with_host(host());
        let mut constraints = TaskConstraints::new(3, None, Some("worker-key".into())).unwrap();
        if hold {
            constraints
                .set_concurrency_key_wait_policy(ConcurrencyKeyWaitPolicy::HoldWhileAwaiting);
        } else {
            assert_eq!(
                constraints.concurrency_key_wait_policy(),
                ConcurrencyKeyWaitPolicy::ReleaseWhileAwaiting
            );
        }
        let first = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"first".to_vec()),
            RunPolicy::Once,
            constraints.clone(),
            TaskMetadata::default(),
        )
        .unwrap();
        let second = TaskSpec::new(
            TaskId::new(),
            TaskPayload::new(b"second".to_vec()),
            RunPolicy::Once,
            constraints,
            TaskMetadata::default(),
        )
        .unwrap();
        let run = admit(&mut a, request(first, vec![]));
        let other = admit(&mut a, request(second, vec![]));
        let expectation = begin(&mut a, run, "local");
        disposition::commit(&mut a, expectation, AttemptDisposition::awaiting(wait(), None), 101)
            .unwrap();
        assert_eq!(a.projection().key_reservation(run).is_some(), hold);
        assert_eq!(claim::eligible(a.projection(), other, None, 101), !hold);
        assert!(h.state().capacity_reserved);
        let mut replacement = h.binding();
        replacement.run = "replacement".to_string().try_into().unwrap();
        replacement.request = "replacement-request".to_string().try_into().unwrap();
        assert!(h.bundle.wi().reserve_start(replacement.clone(), 1).is_err());
        a.into_parts().0.close().unwrap();
        let mut a =
            StorageMutationAuthority::new(log(aq_path.clone(), false), replay_log(&aq_path))
                .with_host(host());
        assert_eq!(a.projection().key_reservation(run).is_some(), hold);
        let reopened =
            worldinterface_storage::StoreRoot::open_existing(h.temp.path().join("bundle/wi"))
                .unwrap();
        assert!(reopened.reserve_start(replacement.clone(), 1).is_err());
        // Resolving a wait makes the original run claimable; reacquisition fences its peer.
        waits::reconcile(&mut a, 200).unwrap();
        assert_eq!(a.projection().get_run_state(&run), Some(&RunState::Ready));
        claim::accept(&mut a, run, AttemptId::new(), "resumed", 200, 1000).unwrap();
        assert!(a.projection().key_reservation(run).is_some());
        assert!(!claim::eligible(a.projection(), other, None, 200));
        assert!(reopened.reserve_start(replacement.clone(), 1).is_err());
        a.into_parts().0.close().unwrap();
        drop(reopened);
        h.operation(Operation::Terminate).await;
        let cleanup = h.finish().await;
        assert!(cleanup.exit_confirmed && cleanup.cleanup_confirmed);
        assert!(h.state().stop_requested && h.state().exited);
        assert!(!h.state().cleanup_confirmed && h.state().capacity_reserved);
        assert!(h.bundle.wi().reserve_start(replacement.clone(), 1).is_err());
        assert!(!h.state().completed);
        assert!(matches!(
            h.state().collection,
            worldinterface_domain::worker::WorkerCollection::Absent
        ));
        h.operation(Operation::Release).await;
        assert!(h.state().cleanup_confirmed && !h.state().capacity_reserved);
        assert!(!h.state().completed);
        assert!(matches!(
            h.state().collection,
            worldinterface_domain::worker::WorkerCollection::Absent
        ));
        assert!(h.bundle.wi().reserve_start(replacement, 1).unwrap());
    }
}
