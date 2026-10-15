//! Process death around AQ's native framed WAL, using only public 0.2.0 APIs.
#[path = "support/child.rs"]
mod child_guard;
mod support {
    pub mod connectors;
    pub mod spi_harness;
}
use actionqueue_core::{
    admission::EnsureTaskRequest,
    causal::{CausalContext, CausalOverride},
    continuation::*,
    disposition::*,
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
use actionqueue_storage::{
    mutation::StorageMutationAuthority,
    store::{open_store, OpenOptions},
    wal::{
        event::{WalEvent, WalEventType},
        fs_writer::WalFsWriter,
        writer::{WalWriter, WalWriterError},
    },
};
use std::{
    io::Write,
    path::PathBuf,
    time::{Duration, Instant},
};
use support::spi_harness::host;
fn barrier(path: &std::path::Path) {
    std::fs::write(path, b"ready").unwrap();
    loop {
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
struct CrashWriter {
    inner: WalFsWriter,
    mode: String,
    ready: PathBuf,
    publishing: bool,
}
impl WalWriter for CrashWriter {
    fn store_session(&self) -> Option<&actionqueue_storage::store::StoreSession> {
        self.inner.store_session()
    }
    fn append(&mut self, e: &WalEvent) -> Result<(), WalWriterError> {
        if matches!(e.event(), WalEventType::AttemptDispositionCommitted { .. }) {
            self.publishing = true;
            if self.mode == "before-commit" {
                barrier(&self.ready);
            }
            if self.mode == "during-publication" {
                let session = self.inner.store_session().unwrap();
                let bytes = actionqueue_storage::wal::codec::encode_for_store(
                    e,
                    session.manifest().store_id,
                )
                .unwrap();
                let mut f =
                    std::fs::OpenOptions::new().append(true).open(session.wal_path()).unwrap();
                f.write_all(&bytes[..bytes.len() / 2]).unwrap();
                f.sync_all().unwrap();
                barrier(&self.ready);
            }
        }
        self.inner.append(e)
    }
    fn flush(&mut self) -> Result<(), WalWriterError> {
        self.inner.flush()?;
        if self.publishing && self.mode == "after-durable-before-ack" {
            barrier(&self.ready);
        }
        Ok(())
    }
    fn close(self) -> Result<(), WalWriterError> {
        self.inner.close()
    }
}
struct Clock;
impl actionqueue_core::time::clock::Clock for Clock {
    fn now(&self) -> u64 {
        100
    }
}
fn spec() -> TaskSpec {
    TaskSpec::new(
        TaskId::new(),
        TaskPayload::new(vec![]),
        RunPolicy::Once,
        TaskConstraints::default(),
        TaskMetadata::default(),
    )
    .unwrap()
}
#[test]
fn native_compound_crash_child() {
    let Ok(dir) = std::env::var("WI_FLOW_CRASH_DIR") else { return };
    let dir = PathBuf::from(dir);
    let mode = std::env::var("WI_FLOW_CRASH_MODE").unwrap();
    let session =
        open_store(&dir.join("aq"), OpenOptions::Initialize { features: vec![] }).unwrap();
    let writer = WalFsWriter::new(session).unwrap();
    let p = actionqueue_storage::recovery::bootstrap::recover_read_only(
        writer.store_session().unwrap(),
        actionqueue_storage::wal::repair::RepairPolicy::Strict,
    )
    .unwrap()
    .projection;
    let mut a = StorageMutationAuthority::new(
        CrashWriter {
            inner: writer,
            mode: mode.clone(),
            ready: dir.join("ready"),
            publishing: false,
        },
        p,
    )
    .with_host(host());
    let parent = spec();
    let task = parent.id();
    let req = EnsureTaskRequest::new(
        AdmissionKey::new("parent").unwrap(),
        parent,
        vec![],
        CausalContext::new(TraceId::new("crash").unwrap(), CorrelationId::new("crash").unwrap()),
        None,
    )
    .unwrap();
    actionqueue_runtime::admission::ensure_task(&mut a, req, &Clock).unwrap();
    let run = a.projection().run_ids_for_task(task)[0];
    let _ = a
        .submit_command(
            MutationCommand::RunStateTransition(RunStateTransitionCommand::new(
                a.projection().latest_sequence() + 1,
                run,
                RunState::Scheduled,
                RunState::Ready,
                100,
            )),
            DurabilityPolicy::Immediate,
        )
        .unwrap();
    let attempt = AttemptId::new();
    let fence =
        actionqueue_runtime::claim::accept(&mut a, run, attempt, "local", 100, 1000).unwrap();
    let expected = AttemptCommitExpectation::new(
        a.projection().latest_sequence() + 1,
        run,
        attempt,
        RunState::Running,
        fence,
    );
    let ids: Vec<_> = (0..3).map(|_| TaskId::new()).collect();
    let children = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            ChildAdmission::new(
                AdmissionKey::new(format!("node:{i}")).unwrap(),
                TaskSpec::new(
                    *id,
                    TaskPayload::new(b"node".to_vec()),
                    RunPolicy::Once,
                    TaskConstraints::default(),
                    TaskMetadata::default(),
                )
                .unwrap()
                .with_parent(task),
                if i == 0 { vec![] } else { vec![ids[i - 1]] },
                CausalOverride::default(),
            )
            .unwrap()
        })
        .collect();
    let wait =
        WaitSpec::children(WaitId::new(), ids.clone(), ChildWaitPolicy::AllTerminal, None).unwrap();
    std::fs::write(
        dir.join("identities"),
        serde_json::to_vec(&(task, run, ids, wait.wait_id())).unwrap(),
    )
    .unwrap();
    let d = AttemptDisposition::new(
        DispositionOutcome::Awaiting,
        DispositionParts {
            child_admissions: children,
            wait: Some(wait),
            checkpoint: Some(CheckpointRef {
                checkpoint_id: CheckpointId::new(),
                created_by_attempt: attempt,
                data: actionqueue_core::data_ref::DataRef::External(
                    actionqueue_core::data_ref::ExternalDataRef {
                        scheme: actionqueue_core::bounded::DataScheme::new("fixture").unwrap(),
                        locator: actionqueue_core::bounded::OpaqueRef::new("prepared").unwrap(),
                        hash: actionqueue_core::bounded::ContentHash::new(
                            actionqueue_core::bounded::HashAlgorithm::Sha256,
                            vec![0; 32],
                        )
                        .unwrap(),
                        size_bytes: None,
                        content_type: None,
                    },
                ),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    actionqueue_runtime::disposition::commit(&mut a, expected, d, 101).unwrap();
    panic!("crash barrier not reached");
}
#[test]
fn child_transaction_native_wal_process_death_cuts_replay_all_or_none() {
    for mode in ["before-commit", "during-publication", "after-durable-before-ack"] {
        let t = tempfile::tempdir().unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "native_compound_crash_child", "--nocapture"])
            .env("WI_FLOW_CRASH_DIR", t.path())
            .env("WI_FLOW_CRASH_MODE", mode)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut child = child_guard::KillOnDrop(child);
        let deadline = Instant::now() + Duration::from_secs(20);
        while !t.path().join("ready").exists() {
            assert!(child.try_wait().unwrap().is_none(), "helper exited at {mode}");
            assert!(Instant::now() < deadline, "barrier {mode}");
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let mut session = open_store(&t.path().join("aq"), OpenOptions::ReadOnly).unwrap();
        if mode == "during-publication" {
            assert!(actionqueue_storage::recovery::bootstrap::recover_read_only(
                &session,
                actionqueue_storage::wal::repair::RepairPolicy::Strict
            )
            .is_err());
            drop(session);
            let result = actionqueue_runtime::engine::ActionQueueEngine::new(
                actionqueue_runtime::config::RuntimeConfig {
                    data_dir: t.path().join("aq"),
                    ..Default::default()
                },
                Noop,
            )
            .bootstrap();
            let Err(actionqueue_runtime::engine::BootstrapError::Recovery(error)) = result else {
                panic!("Strict bootstrap must reject torn tail during recovery");
            };
            assert!(
                matches!(
                    error,
                    actionqueue_storage::recovery::bootstrap::RecoveryBootstrapError::WalReplay(_)
                ),
                "{error}"
            );
            session = open_store(&t.path().join("aq"), OpenOptions::ReadOnly).unwrap();
        }
        let recovered = actionqueue_storage::recovery::bootstrap::recover_read_only(
            &session,
            actionqueue_storage::wal::repair::RepairPolicy::TruncatePartial,
        )
        .unwrap();
        let (parent, run, children, wait): (TaskId, RunId, Vec<TaskId>, WaitId) =
            serde_json::from_slice(&std::fs::read(t.path().join("identities")).unwrap()).unwrap();
        let committed = mode == "after-durable-before-ack";
        assert_eq!(recovered.projection.task_count(), if committed { 4 } else { 1 });
        assert_eq!(
            recovered.projection.parent_child_mappings().count(),
            if committed { 3 } else { 0 }
        );
        assert_eq!(recovered.projection.waits().active_count(), usize::from(committed));
        assert_eq!(
            recovered.projection.get_run_state(&run),
            Some(&if committed { RunState::Awaiting } else { RunState::Running })
        );
        for child in children {
            assert_eq!(recovered.projection.task_admission(child).is_some(), committed);
            if committed {
                assert_eq!(
                    recovered.projection.get_task(&child).unwrap().parent_task_id(),
                    Some(parent)
                );
            }
        }
        assert_eq!(recovered.projection.waits().get(wait).is_some(), committed);
        assert_eq!(
            recovered
                .projection
                .dependency_declarations()
                .filter(|(_, deps)| !deps.is_empty())
                .count(),
            if committed { 2 } else { 0 }
        );
        assert_eq!(recovered.incomplete_tail.is_some(), mode == "during-publication");
    }
}

struct Noop;
impl actionqueue_executor_local::handler::ExecutorHandler for Noop {
    fn execute(
        &self,
        _: actionqueue_executor_local::handler::ExecutorContext,
    ) -> AttemptDisposition {
        AttemptDisposition::complete(None)
    }
}
