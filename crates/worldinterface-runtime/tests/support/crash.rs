use crate::storage::StoreBundle;
use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use worldinterface_protocol::InstanceId;
use worldinterface_storage::StoreProfile;
pub(crate) fn checkpoint() {
    if let Some(ready) = std::env::var_os("WI_BUNDLE_READY") {
        fs::write(ready, b"ready").unwrap();
        loop {
            std::thread::park();
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
    for operation in ["initialize", "backup", "restore"] {
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
            .env("WI_BUNDLE_READY", &ready)
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("early exit {status}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let reached = ready.exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(reached);
        assert!(!path.with_extension("destination").exists());
        if operation != "initialize" {
            drop(StoreBundle::open(&path).unwrap());
        }
    }
}
