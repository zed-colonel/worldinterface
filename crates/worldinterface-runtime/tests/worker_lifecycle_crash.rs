#![cfg(target_os = "linux")]
mod support {
    pub mod child;
    pub mod connectors;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use support::{child::KillOnDrop, worker_fixture::*};
use worldinterface_runtime::{
    storage::StoreBundle,
    worker::recovery::recover_quarantines,
    worker_process::{HostScope, Task},
};
use worldinterface_storage::StoreKind;
use worldinterface_worker::mapping::Operation;
#[tokio::test]
async fn worker_crash_child() {
    let Some(path) = std::env::var_os("WI_WORKER_CRASH_ROOT") else {
        return;
    };
    let temp = tempfile::tempdir_in(Path::new(&path)).unwrap();
    std::fs::write(Path::new(&path).join("location"), temp.path().to_str().unwrap()).unwrap();
    let h = Harness::at(temp, Task::Sleep { millis: 5000 }, "WP-019", 3000, 65536);
    h.operation(Operation::Start).await;
    h.operation(Operation::Terminate).await;
    h.finish().await;
    h.operation(Operation::CollectArtifacts).await;
    h.operation(Operation::Release).await;
    panic!("worker crash boundary was not reached");
}
fn journal_count(path: &Path) -> usize {
    std::fs::read_dir(path.join("process"))
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(".wi-owner-"))
        .count()
}
#[test]
fn wp_019_and_wp_024_killed_lifecycle_boundaries_quarantine_without_replacement() {
    prebuild();
    for point in [
        "worker-operation-before-ack",
        "worker-operation-acknowledged",
        "worker-operation-intent-before-commit",
        "worker-operation-intent-committed",
        "worker-intent-before-commit",
        "worker-intent-committed",
        "worker-helper-before-spawn",
        "worker-helper-spawned",
        "worker-ownership-before-persist",
        "worker-ownership-persisted",
        "worker-fixture-before-spawn",
        "worker-fixture-spawned",
        "worker-fixture-ownership-persisted",
        "worker-before-acknowledgement",
        "worker-acknowledged",
        "worker-event-before-commit",
        "worker-event-committed",
        "worker-wake-before-publish",
        "worker-wake-published",
        "worker-wake-before-ack",
        "worker-wake-acknowledged",
        "worker-cancel-before-commit",
        "worker-cancel-committed",
        "worker-cancel-before-delivery",
        "worker-cancel-delivered",
        "worker-cleanup-before-commit",
        "worker-cleanup-committed",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("host");
        std::fs::create_dir(&path).unwrap();
        let ready = t.path().join("ready");
        let log = std::fs::File::create(t.path().join("child.log")).unwrap();
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "worker_crash_child", "--nocapture"])
                .env("WI_WORKER_CRASH_ROOT", &path)
                .env("WI_CRASH_POINT", point)
                .env("WI_CRASH_READY", &ready)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(40);
        while !ready.is_file() {
            if let Some(status) = child.try_wait().unwrap() {
                panic!(
                    "{point} child exited {status}: {}",
                    std::fs::read_to_string(t.path().join("child.log")).unwrap()
                );
            }
            assert!(Instant::now() < deadline, "{point} boundary deadline");
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let path =
            std::path::PathBuf::from(std::fs::read_to_string(path.join("location")).unwrap());
        let count = journal_count(&path);
        assert!(count <= 1, "blind duplicate launch at {point}");
        // Helpers keep bounded stop-on-disconnect ownership. Give any launched tree time to clean up.
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let live = std::fs::read_dir(path.join("process"))
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with(".wi-owner-"))
                .flat_map(|e| {
                    std::fs::read_to_string(e.path())
                        .unwrap_or_default()
                        .lines()
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(&line).ok())
                .any(|v| {
                    let Some(identity) = v["entry"]["identity"].as_object() else {
                        return false;
                    };
                    let Some(pid) = identity["pid"].as_u64() else {
                        return false;
                    };
                    let birth = identity["birth"].as_u64();
                    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                        return false;
                    };
                    let fields: Vec<_> =
                        stat.rsplit_once(')').unwrap().1.split_whitespace().collect();
                    fields[0] != "Z" && fields.get(19).and_then(|s| s.parse::<u64>().ok()) == birth
                });
            if !live {
                break;
            }
            assert!(Instant::now() < until, "owned helper or fixture remained live after {point}");
            std::thread::sleep(Duration::from_millis(10));
        }
        let bundle = StoreBundle::open(&path.join("bundle")).unwrap();
        let root = bundle.wi();
        let scope = HostScope {
            tenant: "tenant:fixture".into(),
            principal: "12345678-1234-4234-8234-123456789abc".into(),
        };
        recover_quarantines(root, &scope).unwrap();
        recover_quarantines(root, &scope).unwrap();
        let tenant = "tenant:fixture".to_string().try_into().unwrap();
        let provider = "local-proof".to_string().try_into().unwrap();
        let run = "run-proof".to_string().try_into().unwrap();
        if let Some(state) = root.worker_snapshot(&tenant, &provider, &run).unwrap() {
            if state.capacity_reserved {
                assert!(state.quarantined && state.stop_requested);
                let mut replacement = state.binding.clone();
                replacement.run = "replacement".to_string().try_into().unwrap();
                replacement.request = "replacement".to_string().try_into().unwrap();
                assert!(root.reserve_start(replacement, 1).is_err());
            } else {
                assert!(state.cleanup_confirmed);
            }
            assert!(!root.reserve_start(state.binding.clone(), 1).unwrap());
        } else {
            assert_eq!(point, "worker-intent-before-commit");
            assert_eq!(count, 0);
        }
        for obligation in root.pending_signals(StoreKind::Fabric, None, 100).unwrap() {
            assert_eq!(
                root.authoritative_event(StoreKind::Fabric, &obligation.event.id).unwrap(),
                obligation.event
            );
        }
        let mut queue = worldinterface_runtime::aq_adapter::QueueEngine::bootstrap(
            &bundle,
            support::spi_harness::Noop,
            support::spi_harness::host(),
        )
        .unwrap();
        queue.drain_signals().unwrap();
        assert_eq!(queue.drain_signals().unwrap().acknowledged, 0);
        queue.shutdown().unwrap();
        assert_eq!(journal_count(&path), count);
        eprintln!("verified crash cut {point}: {count} launch journal(s), recovery and wake replay converged");
    }
}
