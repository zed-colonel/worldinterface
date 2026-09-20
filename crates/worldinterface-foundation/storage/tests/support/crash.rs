//! Test-only subprocess barriers: production builds contain no environment fault switches.
use crate::*;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use worldinterface_domain::{Action, Event, Machine};
use worldinterface_protocol::*;
pub(crate) fn checkpoint(name: &str) {
    if std::env::var("WI_CRASH_POINT").as_deref() == Ok(name) {
        fs::write(std::env::var_os("WI_CRASH_READY").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
pub(crate) fn prepare(c: &rusqlite::Connection) {
    if std::env::var_os("WI_CRASH_ROOT").is_some() {
        c.execute_batch("PRAGMA cache_size=1; PRAGMA cache_spill=ON;").unwrap();
    }
}
fn profile() -> StoreProfile {
    StoreProfile {
        instance: InstanceId::generate(),
        created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
        platform: false,
    }
}
fn submission() -> Envelope {
    let v: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../conformance/wi-fabric-2/domain/canonical-vectors.json"
    ))
    .unwrap();
    Envelope::parse(&serde_json::to_vec(&v[0]["envelope"]).unwrap()).unwrap()
}
fn submit(root: &StoreRoot) -> EntityId {
    let id: EntityId = "12345678-1234-4234-8234-123456789abc".to_string().try_into().unwrap();
    root.repository(StoreKind::Fabric)
        .unwrap()
        .admit(
            AdmissionScope {
                deployment_tenant: "tenant".to_string().try_into().unwrap(),
                authenticated_principal: PrincipalId::generate(),
            },
            submission(),
            id.clone(),
            Event {
                entity: id.clone(),
                machine: Machine::Effect,
                action: Action::Submit,
                at: profile().created_at,
            },
            OutboxEntry {
                key: "stable".to_string().try_into().unwrap(),
                entity: id,
                payload: "input".to_string().try_into().unwrap(),
                delivered: false,
            },
        )
        .unwrap()
}
#[test]
fn child() {
    let Some(path) = std::env::var_os("WI_CRASH_ROOT") else {
        return;
    };
    let path = Path::new(&path);
    let point = std::env::var("WI_CRASH_POINT").unwrap();
    if point.starts_with("initialization") {
        let _ = StoreRoot::create_empty(path, profile());
    } else if point == "backup-publish" {
        StoreRoot::backup_offline(path, path.with_extension("backup")).unwrap();
    } else {
        let root = StoreRoot::open_existing(path).unwrap();
        if point == "artifact-durable" {
            root.publish("tenant".to_string().try_into().unwrap(), b"bytes").unwrap();
        } else {
            submit(&root);
        }
    }
    panic!("child did not reach requested barrier");
}
fn kill_at(path: &Path, point: &str) {
    let ready = path.with_extension("ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash::child", "--nocapture"])
        .env("WI_CRASH_ROOT", path)
        .env("WI_CRASH_POINT", point)
        .env("WI_CRASH_READY", &ready)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.exists() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("child exited before {point}: {status}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let reached = ready.exists();
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(reached, "barrier {point} timed out");
}
#[test]
fn killed_transactions_artifacts_and_publication_recover_atomically() {
    for point in [
        "initialization-store",
        "initialization-publish",
        "after-records",
        "after-outbox",
        "after-commit",
        "artifact-durable",
        "backup-publish",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("root");
        if !point.starts_with("initialization") {
            drop(StoreRoot::create_empty(&path, profile()).unwrap());
        }
        kill_at(&path, point);
        if point.starts_with("initialization") {
            assert!(StoreRoot::open_existing(&path).is_err());
            continue;
        }
        if point == "backup-publish" {
            assert!(!path.with_extension("backup").exists());
        }
        if point == "after-outbox" {
            let journal = fs::read(path.join("fabric.sqlite-journal")).unwrap();
            assert_eq!(
                &journal[..8],
                &[0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7],
                "must exercise a hot journal"
            );
            let manifest_path = path.join("manifest.json");
            let original = fs::read(&manifest_path).unwrap();
            let mut bad: serde_json::Value = serde_json::from_slice(&original).unwrap();
            bad["schema"] = 999.into();
            fs::write(&manifest_path, serde_json::to_vec(&bad).unwrap()).unwrap();
            let db = fs::read(path.join("fabric.sqlite")).unwrap();
            assert!(StoreRoot::open_existing(&path).is_err());
            assert_eq!(fs::read(path.join("fabric.sqlite-journal")).unwrap(), journal);
            assert_eq!(fs::read(path.join("fabric.sqlite")).unwrap(), db);
            fs::write(&manifest_path, original).unwrap();
        }
        let root = StoreRoot::open_existing(&path).unwrap();
        if point.starts_with("after-") {
            let c = root.connect(StoreKind::Fabric).unwrap();
            let count = |table: &str| {
                c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get::<_, i64>(0))
                    .unwrap()
            };
            let expected = if point == "after-commit" { 1 } else { 0 };
            for table in ["records", "outbox", "history", "admissions", "projections"] {
                assert_eq!(count(table), expected, "{point}/{table}");
            }
        }
        if point == "artifact-durable" {
            let a = ArtifactRef {
                scope: "tenant".to_string().try_into().unwrap(),
                content: ContentHash::of(b"bytes"),
                size: 5,
            };
            assert!(root.read_artifact(&a).is_err());
            assert_eq!(root.publish(a.scope.clone(), b"bytes").unwrap(), a);
        }
    }
}
