#[path = "child.rs"]
mod child_guard;
use crate::storage::StoreBundle;
use child_guard::KillOnDrop;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use worldinterface_protocol::InstanceId;
use worldinterface_storage::StoreProfile;
pub(crate) fn checkpoint(point: &str) {
    if std::env::var("WI_BUNDLE_POINT").as_deref() == Ok(point) {
        let ready = std::env::var_os("WI_BUNDLE_READY").unwrap();
        fs::write(ready, b"ready").unwrap();
        loop {
            if std::env::var_os("WI_BUNDLE_CONTINUE").is_some_and(|p| Path::new(&p).exists()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
fn profile() -> StoreProfile {
    StoreProfile {
        instance: InstanceId::generate(),
        created_at: "2026-09-19T00:00:00.000000000Z".to_string().try_into().unwrap(),
        platform: false,
    }
}
#[test]
fn child() {
    let Some(path) = std::env::var_os("WI_BUNDLE_SOURCE") else {
        return;
    };
    let path = Path::new(&path);
    let dest = path.with_extension("destination");
    match std::env::var("WI_BUNDLE_OPERATION").unwrap().as_str() {
        "initialize" => {
            StoreBundle::initialize(&dest, profile()).unwrap();
        }
        "backup" => StoreBundle::backup(path, &dest).unwrap(),
        "restore" => StoreBundle::restore(path, &dest).unwrap(),
        _ => unreachable!(),
    }
    panic!("publication barrier not reached");
}
#[test]
fn killed_bundle_publication_never_exposes_incomplete_destination() {
    for (operation, point) in ["initialize", "backup", "restore"]
        .into_iter()
        .flat_map(|op| ["before-rename", "after-rename"].map(|point| (op, point)))
    {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("source");
        if operation == "backup" {
            drop(StoreBundle::initialize(&path, profile()).unwrap());
        }
        if operation == "restore" {
            let origin = t.path().join("origin");
            drop(StoreBundle::initialize(&origin, profile()).unwrap());
            StoreBundle::backup(&origin, &path).unwrap();
        }
        let ready = t.path().join("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash::child", "--nocapture"])
            .env("WI_BUNDLE_SOURCE", &path)
            .env("WI_BUNDLE_OPERATION", operation)
            .env("WI_BUNDLE_POINT", point)
            .env("WI_BUNDLE_READY", &ready)
            .stdout(Stdio::null())
            .spawn()
            .map(KillOnDrop)
            .unwrap();
        // CI may start the helper while another crash case is doing durable
        // bundle I/O. Give the child time to reach the explicit fault point.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready.exists() && Instant::now() < deadline {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("early exit {status}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let reached = ready.exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(reached, "publication barrier not reached for {operation} at {point}");
        let dest = path.with_extension("destination");
        if point == "before-rename" {
            assert!(!dest.exists());
            match operation {
                "initialize" => drop(StoreBundle::initialize(&dest, profile()).unwrap()),
                "backup" => StoreBundle::backup(&path, &dest).unwrap(),
                "restore" => StoreBundle::restore(&path, &dest).unwrap(),
                _ => unreachable!(),
            }
        }
        // After rename, publication is already complete even before parent sync.
        drop(StoreBundle::open(&dest).unwrap());
        if operation != "initialize" {
            drop(StoreBundle::open(&path).unwrap());
        }
    }
}

#[test]
fn destination_racing_publication_is_never_replaced() {
    for operation in ["initialize", "backup", "restore"] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("source");
        if operation == "restore" {
            let origin = t.path().join("origin");
            drop(StoreBundle::initialize(&origin, profile()).unwrap());
            StoreBundle::backup(&origin, &path).unwrap();
        } else {
            drop(StoreBundle::initialize(&path, profile()).unwrap());
        }
        let dest = path.with_extension("destination");
        let ready = t.path().join("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash::child", "--nocapture"])
            .env("WI_BUNDLE_SOURCE", &path)
            .env("WI_BUNDLE_OPERATION", operation)
            .env("WI_BUNDLE_POINT", "before-rename")
            .env("WI_BUNDLE_READY", &ready)
            .env("WI_BUNDLE_CONTINUE", t.path().join("continue"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map(KillOnDrop)
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ready.exists() && Instant::now() < deadline {
            assert!(child.try_wait().unwrap().is_none());
            std::thread::sleep(Duration::from_millis(5));
        }
        if !ready.exists() {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("publication barrier timed out for {operation}");
        }
        // An empty directory is the critical case: ordinary rename would replace it.
        fs::create_dir(&dest).unwrap();
        use std::os::unix::fs::MetadataExt;
        let inode = fs::metadata(&dest).unwrap().ino();
        fs::write(t.path().join("continue"), b"continue").unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("publication did not finish for {operation}");
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(!status.success());
        assert_eq!(fs::metadata(&dest).unwrap().ino(), inode);
        assert_eq!(fs::read_dir(&dest).unwrap().count(), 0);
    }
}
