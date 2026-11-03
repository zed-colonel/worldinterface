//! Trusted test-host builder. Build provenance comes from verified packet sources
//! and a fixed compiler recipe, independently of any path later supplied for launch.
#![allow(dead_code)]
use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};
use worldinterface_protocol::ContentHash;
use worldinterface_runtime::worker::evaluation::EvaluationManifest;

pub struct WorkerBuild {
    _private: tempfile::TempDir,
    pub packet: ContentHash,
    pub fixture: PathBuf,
    pub helper: PathBuf,
    pub fixture_bytes: Vec<u8>,
    pub helper_bytes: Vec<u8>,
    pub fixture_digest: ContentHash,
    pub helper_digest: ContentHash,
}

fn compiler(program: &str, root: &Path) -> Command {
    let mut command = Command::new(program);
    command.current_dir(root).env_clear();
    // Do not inherit compiler wrappers, flags, profile overrides or target selection.
    // Preserve only tool/cache lookup and the supplied external scratch location.
    for name in ["PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME", "TMPDIR", "TMP", "TEMP"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command.env("RUSTUP_TOOLCHAIN", "1.89.0").env("CARGO_NET_OFFLINE", "true");
    command
}

fn compile() -> WorkerBuild {
    let runtime = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = runtime.join("../..");
    let manifest: EvaluationManifest = serde_json::from_slice(
        &fs::read(root.join("conformance/wi-fabric-2/worker/evaluation-manifest.json")).unwrap(),
    )
    .unwrap();
    manifest.verify_sources(&root).unwrap();
    let environment: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join("conformance/wi-fabric-2/worker/evaluation-environment.json")).unwrap(),
    )
    .unwrap();
    for (path, expected) in environment["support_binary_sha256"].as_object().unwrap() {
        assert_eq!(ContentHash::of(&fs::read(path).unwrap()).as_str(), expected.as_str().unwrap());
    }
    let rustc = compiler("rustc", runtime).arg("--version").output().unwrap();
    assert!(rustc.status.success());
    let rustc = String::from_utf8(rustc.stdout).unwrap();
    assert!(rustc.starts_with("rustc 1.89.0 "), "unapproved compiler identity");
    // Cargo supplies this location even before the binary exists. Use it only
    // to locate this test's external cache, never as executable build evidence.
    let target = Path::new(env!("CARGO_BIN_EXE_wi-process-fixture"))
        .parent()
        .unwrap()
        .join("wi-worker-reviewed-build");
    let output = compiler("/usr/bin/timeout", runtime)
        .args([
            "900",
            env!("CARGO"),
            "build",
            "--locked",
            "--offline",
            "--bins",
            "--jobs",
            "1",
            "--message-format=json",
            "--manifest-path",
        ])
        .arg(runtime.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(&target)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "reviewed worker build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut produced = BTreeMap::new();
    for line in String::from_utf8(output.stdout).unwrap().lines() {
        let event: serde_json::Value = serde_json::from_str(line).unwrap();
        if event["reason"] == "compiler-artifact"
            && event["target"]["kind"] == serde_json::json!(["bin"])
            && ["wi-process-fixture", "wi-process-reaper"]
                .contains(&event["target"]["name"].as_str().unwrap_or(""))
        {
            let name = event["target"]["name"].as_str().unwrap();
            assert_eq!(
                Path::new(event["target"]["src_path"].as_str().unwrap()).canonicalize().unwrap(),
                runtime.join(format!("src/bin/{name}.rs")).canonicalize().unwrap()
            );
            assert_eq!(event["profile"]["debug_assertions"], true);
            assert_eq!(event["profile"]["overflow_checks"], true);
            assert_eq!(event["profile"]["debuginfo"], 0);
            produced.insert(name.to_owned(), PathBuf::from(event["executable"].as_str().unwrap()));
        }
    }
    assert_eq!(produced.len(), 2, "missing compiler-attested worker outputs");
    manifest.verify_sources(&root).unwrap();
    let private = tempfile::tempdir().unwrap();
    let mut artifacts = BTreeMap::new();
    let mut report = BTreeMap::new();
    for (name, path) in produced {
        assert!(path.canonicalize().unwrap().starts_with(target.canonicalize().unwrap()));
        assert_eq!(path.file_name().unwrap(), name.as_str());
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.starts_with(b"\x7fELF"));
        assert!(bytes.len() as u64 <= worldinterface_protocol::plan::MAX_BOUNDARY_ARTIFACT_BYTES);
        let digest = ContentHash::of(&bytes);
        let copy = private.path().join(&name);
        fs::write(&copy, &bytes).unwrap();
        fs::set_permissions(&copy, fs::Permissions::from_mode(0o500)).unwrap();
        report.insert(name.clone(), serde_json::json!({"sha256":digest,"bytes":bytes.len()}));
        artifacts.insert(name, (copy, bytes, digest));
    }
    let packet = manifest.digest().unwrap();
    let evidence = PathBuf::from(std::env::var_os("TMPDIR").expect("external run scratch"))
        .join("verification/revised-packet");
    fs::create_dir_all(&evidence).unwrap();
    fs::write(
        evidence.join(format!("trusted-build-{}.json", std::process::id())),
        serde_json::to_vec_pretty(&serde_json::json!({
            "version":3,"manifest":packet,"rustc":rustc.trim(),
            "recipe":"tests/support/worker_build.rs","executables":report
        }))
        .unwrap(),
    )
    .unwrap();
    let (fixture, fixture_bytes, fixture_digest) = artifacts.remove("wi-process-fixture").unwrap();
    let (helper, helper_bytes, helper_digest) = artifacts.remove("wi-process-reaper").unwrap();
    WorkerBuild {
        _private: private,
        packet,
        fixture,
        helper,
        fixture_bytes,
        helper_bytes,
        fixture_digest,
        helper_digest,
    }
}

pub fn current() -> &'static WorkerBuild {
    static BUILD: OnceLock<WorkerBuild> = OnceLock::new();
    BUILD.get_or_init(compile)
}
