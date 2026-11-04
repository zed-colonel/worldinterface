//! Crash recovery of private snapshots and staged WI publication; no worker is launched.
#[path = "support/child.rs"]
mod child;
#[path = "support/worker_collection.rs"]
mod fixture;
use fixture::Collection;
use std::{
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use worldinterface_domain::worker::*;
use worldinterface_runtime::worker::{artifacts, recovery::recover_quarantines};
use worldinterface_storage::{StoreKind, StoreRoot};
#[test]
fn artifact_crash_child() {
    let Some(path) = std::env::var_os("WI_ARTIFACT_CRASH_ROOT") else { return };
    let c = Collection::new(&Path::new(&path).join("store"));
    std::fs::write(
        Path::new(&path).join("binding"),
        worldinterface_protocol::canonical_data(&c.binding).unwrap(),
    )
    .unwrap();
    c.prepare().unwrap();
    artifacts::resume(&c.root, &c.binding).unwrap();
    panic!("collection barrier not reached");
}
#[test]
fn wp_028_snapshot_and_publication_crash_cuts_recover_exactly_or_quarantine() {
    for point in [
        "worker-snapshot-retained",
        "worker-collection-before-prepare",
        "worker-collection-prepared",
        "publication-identified",
        "publication-durable",
        "publication-availability",
        "worker-manifest-published",
        "worker-collection-before-publish",
        "worker-collection-published",
    ] {
        let t = tempfile::tempdir().unwrap();
        let ready = t.path().join("ready");
        let log = std::fs::File::create(t.path().join("child.log")).unwrap();
        let mut process = child::KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "artifact_crash_child", "--nocapture"])
                .env("WI_ARTIFACT_CRASH_ROOT", t.path())
                .env("WI_CRASH_POINT", point)
                .env("WI_CRASH_READY", &ready)
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready.is_file() {
            assert!(
                process.try_wait().unwrap().is_none(),
                "{point}: {}",
                std::fs::read_to_string(t.path().join("child.log")).unwrap()
            );
            assert!(Instant::now() < deadline, "{point} deadline");
            std::thread::sleep(Duration::from_millis(5));
        }
        process.kill().unwrap();
        process.wait().unwrap();
        let b: WorkerBinding =
            serde_json::from_slice(&std::fs::read(t.path().join("binding")).unwrap()).unwrap();
        let root = StoreRoot::open_existing(t.path().join("store")).unwrap();
        recover_quarantines(
            &root,
            &worldinterface_runtime::worker_process::HostScope {
                tenant: b.tenant.as_str().into(),
                principal: b.principal.as_str().into(),
            },
        )
        .unwrap();
        let state = root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
        assert!(!state.completed);
        if matches!(state.collection, WorkerCollection::Published(_)) {
            assert_eq!(artifacts::read(&root, &b, "blob").unwrap(), b"safe");
            let before = state.clone();
            artifacts::resume(&root, &b).unwrap();
            assert_eq!(
                root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap(),
                before
            );
        } else {
            assert!(state.quarantined);
            assert!(artifacts::read(&root, &b, "blob").is_err());
        }
        root.repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    }
}
#[test]
fn wp_028_missing_or_corrupt_prepared_snapshot_quarantines_without_recollection() {
    for corrupt in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("store");
        let c = Collection::new(&path);
        c.prepare().unwrap();
        let b = c.binding.clone();
        drop(c);
        let sql = rusqlite::Connection::open(path.join("fabric.sqlite")).unwrap();
        sql.execute_batch(
            "DROP TRIGGER worker_snapshot_no_delete; DROP TRIGGER worker_snapshot_no_update;",
        )
        .unwrap();
        if corrupt {
            sql.execute("UPDATE worker_snapshots SET bytes=x'00'", []).unwrap();
        } else {
            sql.execute("DELETE FROM worker_snapshots", []).unwrap();
        }
        // Restore the schema's immutable guard; only private bytes were fault-injected.
        sql.execute_batch("CREATE TRIGGER worker_snapshot_no_update BEFORE UPDATE ON worker_snapshots BEGIN SELECT RAISE(ABORT,'immutable'); END; CREATE TRIGGER worker_snapshot_no_delete BEFORE DELETE ON worker_snapshots BEGIN SELECT RAISE(ABORT,'immutable'); END;").unwrap();
        drop(sql);
        let root = StoreRoot::open_existing(&path).unwrap();
        assert!(artifacts::resume(&root, &b).is_err());
        let state = root.worker_snapshot(&b.tenant, &b.provider, &b.run).unwrap().unwrap();
        assert!(matches!(state.collection, WorkerCollection::Quarantined(_)));
        assert!(!state.completed);
        assert!(artifacts::read(&root, &b, "blob").is_err());
    }
}
