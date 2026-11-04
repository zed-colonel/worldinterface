//! Positive evaluation: requires acceptance of this exact packet before execution.
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
use worldinterface_domain::worker::*;
use worldinterface_protocol::*;
use worldinterface_runtime::{
    storage::StoreBundle,
    worker::{artifacts, recovery},
    worker_process::*,
};
use worldinterface_storage::{flow::FlowOutcome, StoreKind};
use worldinterface_worker::mapping::Operation;
#[tokio::test]
async fn artifact_live_crash_child() {
    let Some(root) = std::env::var_os("WI_ARTIFACT_LIVE_ROOT") else { return };
    let temp = tempfile::tempdir_in(Path::new(&root)).unwrap();
    std::fs::write(Path::new(&root).join("location"), temp.path().as_os_str().as_encoded_bytes())
        .unwrap();
    let mut h = Harness::at(temp, Task::Probe, "WP-028", 1500, 65536);
    h.exports = vec![Export {
        reference: "probe".into(),
        name: "probe.json".into(),
        digest: ContentHash::of(b"{\"clean\":true}"),
        size: 14,
    }];
    h.operation(Operation::Start).await;
    h.finish().await;
    std::fs::write(Path::new(&root).join("binding"), canonical_data(&h.binding()).unwrap())
        .unwrap();
    std::env::set_var("WI_CRASH_POINT", std::env::var("WI_ARTIFACT_LIVE_POINT").unwrap());
    h.operation(Operation::CollectArtifacts).await;
    worldinterface_storage::worker_boundary("worker-artifact-aq-completed");
    panic!("collection/effect/AQ barrier not reached");
}
#[tokio::test]
async fn wp_028_live_collection_ack_receipt_and_aq_crashes_preserve_publication_order() {
    prebuild();
    for point in [
        "worker-collection-before-prepare",
        "worker-collection-prepared",
        "worker-collection-before-publish",
        "worker-collection-published",
        "worker-operation-before-ack",
        "worker-operation-acknowledged",
        "fabric-receipt-history",
        "fabric-receipt-outbox",
        "fabric-receipt-committed",
        "worker-artifact-before-aq-disposition",
        "worker-artifact-aq-completed",
    ] {
        let t = tempfile::tempdir().unwrap();
        let ready = t.path().join("ready");
        let log = std::fs::File::create(t.path().join("child.log")).unwrap();
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "artifact_live_crash_child", "--nocapture"])
                .env("WI_ARTIFACT_LIVE_ROOT", t.path())
                .env("WI_ARTIFACT_LIVE_POINT", point)
                .env("WI_CRASH_READY", &ready)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        while !ready.is_file() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "{point}: {}",
                std::fs::read_to_string(t.path().join("child.log")).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "{point}: {}",
                std::fs::read_to_string(t.path().join("child.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let path =
            std::path::PathBuf::from(std::fs::read_to_string(t.path().join("location")).unwrap());
        let b: WorkerBinding =
            serde_json::from_slice(&std::fs::read(t.path().join("binding")).unwrap()).unwrap();
        let bundle = StoreBundle::open(&path.join("bundle")).unwrap();
        recovery::recover_quarantines(
            bundle.wi(),
            &HostScope { tenant: b.tenant.as_str().into(), principal: b.principal.as_str().into() },
        )
        .unwrap();
        let state = bundle.wi().worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
        if state.completed {
            assert!(matches!(state.collection, WorkerCollection::Published(_)));
        }
        if matches!(state.collection, WorkerCollection::Published(_)) {
            assert_eq!(artifacts::read(bundle.wi(), &b, "probe").unwrap(), b"{\"clean\":true}");
        } else {
            assert!(state.quarantined);
            assert!(artifacts::read(bundle.wi(), &b, "probe").is_err());
        }
        // F-001: recovery must survive the next admitted operations, not just reopen.
        let published = matches!(state.collection, WorkerCollection::Published(_));
        drop(bundle);
        let mut h = Harness::open_at(t, &path, Task::Probe, "WP-028", 1500, 65536);
        h.exports = vec![Export {
            reference: "probe".into(),
            name: "probe.json".into(),
            digest: ContentHash::of(b"{\"clean\":true}"),
            size: 14,
        }];
        assert_eq!(h.binding(), b);
        if published {
            h.operation(Operation::CollectArtifacts).await;
            assert!(h.state().completed, "{point}: recovered collection must complete");
        }
        let decided = h.state().collection;
        h.operation_expect(Operation::Release, FlowOutcome::Uncertain).await;
        let after = h.state();
        assert_eq!(after.collection, decided, "{point}: Release changed export disposition");
        assert_eq!(after.completed, published, "{point}: Release revoked completion");
        assert!(after.quarantined && after.capacity_reserved && !after.cleanup_confirmed);
        h.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        let h = h.reopen();
        assert_eq!(h.state(), after, "{point}: replay/reopen changed durable outcome");
        if published {
            assert_eq!(h.runtime.read_artifact(&b, "probe").unwrap(), b"{\"clean\":true}");
        } else {
            assert!(h.runtime.read_artifact(&b, "probe").is_err());
        }
        let journals = std::fs::read_dir(path.join("process"))
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().starts_with(".wi-owner-"))
            .count();
        assert_eq!(journals, 1, "no replacement work");
    }
}
