#![cfg(target_os = "linux")]
use std::{
    fs,
    os::fd::AsRawFd,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use worldinterface_protocol::ContentHash;
use worldinterface_runtime::worker_process::*;
use worldinterface_worker::{
    controls::{Control, ControlEvidenceV1, ControlKind, ControlState},
    credential::CredentialBindingV1,
    mapping::{Operation, ResolvedWorkerV1, WorkerRequestV1},
    ports::{Acknowledgment, ObservationKind, WorkerPort},
    profile::ProfileBinding,
};
const OPS: [Operation; 6] = [
    Operation::Provision,
    Operation::Start,
    Operation::Inspect,
    Operation::Terminate,
    Operation::CollectArtifacts,
    Operation::Release,
];
struct Harness {
    port: ProcessWorkerPort,
    root: tempfile::TempDir,
    requests: Vec<WorkerRequestV1>,
}
fn private_dir() -> tempfile::TempDir {
    use std::os::unix::fs::PermissionsExt;
    tempfile::Builder::new().permissions(fs::Permissions::from_mode(0o700)).tempdir().unwrap()
}
fn registry() -> ExecutableRegistry {
    let mut r = ExecutableRegistry::default();
    let path = Path::new(env!("CARGO_BIN_EXE_wi-process-fixture"));
    r.register("fixture-v1", path, &executable_digest(path).unwrap()).unwrap();
    r
}
fn port(root: &Path, scope: HostScope) -> ProcessWorkerPort {
    let path = Path::new(env!("CARGO_BIN_EXE_wi-process-reaper"));
    ProcessWorkerPort::new(
        scope,
        root,
        path,
        &executable_digest(path).unwrap(),
        registry(),
        Arc::new(SystemClock),
    )
    .unwrap()
}
fn scope() -> HostScope {
    HostScope { tenant: "tenant-a".into(), principal: "principal-a".into() }
}
fn authorization(
    task: Task,
    wall: u64,
    output: u64,
    exports: Vec<Export>,
    name: &str,
) -> AuthorizedRun {
    let controls = ControlEvidenceV1 {
        schema: 1,
        provider: "local-proof".into(),
        run: name.into(),
        requested: vec![
            Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Supported(wall),
            },
            Control {
                kind: ControlKind::OutputBytes,
                mandatory: true,
                state: ControlState::Supported(output),
            },
        ],
        advertised: vec![],
        applied: vec![
            Control {
                kind: ControlKind::WallTimeMs,
                mandatory: true,
                state: ControlState::Supported(wall),
            },
            Control {
                kind: ControlKind::OutputBytes,
                mandatory: true,
                state: ControlState::Supported(output),
            },
        ],
        observed: vec![],
    };
    let inputs = vec![Input {
        reference: format!("input-{name}"),
        digest: ContentHash::of(b"hello"),
        bytes: b"hello".to_vec(),
    }];
    let input_digest = AuthorizedRun::input_digest(&inputs).unwrap();
    let controls_digest = AuthorizedRun::controls_digest(&controls).unwrap();
    let configuration_digest = AuthorizedRun::configuration_digest(&task, &exports).unwrap();
    let auth = ContentHash::of(format!("authorized-tenant-a-{name}").as_bytes());
    let exe = executable_digest(Path::new(env!("CARGO_BIN_EXE_wi-process-fixture"))).unwrap();
    let operations = OPS
        .iter()
        .map(|op| {
            let r = WorkerRequestV1 {
                schema: 1,
                profile: ProfileBinding::process_proof(),
                operation: *op,
                provider: "local-proof".into(),
                run: name.into(),
                request: format!("{name}-{}", op.name()),
                input_digest: input_digest.clone(),
                controls_digest: controls_digest.clone(),
            };
            let resolved = ResolvedWorkerV1 {
                schema: 1,
                request_digest: r.digest().unwrap(),
                provider: r.provider.clone(),
                run: r.run.clone(),
                adapter_digest: adapter_digest(),
                executable_digest: exe.clone(),
                dependencies_digest: dependencies_digest(),
                configuration_digest: configuration_digest.clone(),
                authorization_digest: auth.clone(),
                controls_digest: controls_digest.clone(),
            };
            (r, resolved)
        })
        .collect();
    AuthorizedRun {
        scope: scope(),
        executable: "fixture-v1".into(),
        operations,
        task,
        inputs,
        exports: exports.clone(),
        controls,
        credential: CredentialBindingV1 {
            schema: 1,
            reference: format!("credential-{name}"),
            issuer: "worldinterface".into(),
            audience: "process-managed-proof-1".into(),
            principal: "principal-a".into(),
            provider: "local-proof".into(),
            run: name.into(),
            operations: OPS.to_vec(),
            artifact_scope: exports.iter().map(|e| e.reference.clone()).collect(),
            authorization_digest: auth,
            expires_unix_ms: SystemClock.unix_ms() + 60_000,
            revocation_generation: 0,
        },
    }
}
fn harness(task: Task, wall: u64, output: u64, exports: Vec<Export>) -> Harness {
    let root = private_dir();
    let port = port(root.path(), scope());
    let a = authorization(task, wall, output, exports, "run-a");
    let requests = a.operations.iter().map(|(r, _)| r.clone()).collect();
    port.admit(a).unwrap();
    Harness { port, root, requests }
}
impl Harness {
    async fn submit(&self, op: Operation) -> Result<Acknowledgment, String> {
        self.port.submit(&self.requests[OPS.iter().position(|x| *x == op).unwrap()]).await
    }
    async fn start(&self) {
        self.submit(Operation::Provision).await.unwrap();
        self.submit(Operation::Start).await.unwrap();
    }
    fn finished(&self) -> CleanupReport {
        let deadline = Instant::now() + Duration::from_secs(8);
        while Instant::now() < deadline {
            if let Some(report) = self.port.evidence("local-proof", "run-a").unwrap() {
                return report;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("bounded adapter completion deadline exhausted");
    }
}
fn export(name: &str, bytes: &[u8]) -> Export {
    Export {
        reference: format!("artifact-{name}"),
        name: name.into(),
        digest: ContentHash::of(bytes),
        size: bytes.len() as u64,
    }
}
#[tokio::test]
async fn transform_artifacts_are_scoped_integrity_checked_and_release_is_replay_safe() {
    let bytes = b"hello".iter().map(|b| b ^ 7).collect::<Vec<_>>();
    let h = harness(Task::Transform { xor: 7 }, 1500, 65536, vec![export("output.bin", &bytes)]);
    h.start().await;
    let report = h.finished();
    assert!(report.exit_confirmed && report.cleanup_confirmed, "{report:?}");
    h.submit(Operation::CollectArtifacts).await.unwrap();
    assert_eq!(h.port.read_artifact("artifact-output.bin").await.unwrap(), bytes);
    assert!(h.port.read_artifact("other-run-artifact").await.is_err());
    let other = private_dir();
    let foreign = port(
        other.path(),
        HostScope { tenant: "tenant-b".into(), principal: "principal-b".into() },
    );
    assert!(foreign.read_artifact("artifact-output.bin").await.is_err());
    assert!(foreign.observe("local-proof", "run-a").await.is_err());
    let realization = h.port.realization(&h.requests[1]).unwrap();
    assert_eq!(
        realization.executable_digest,
        executable_digest(Path::new(env!("CARGO_BIN_EXE_wi-process-fixture"))).unwrap()
    );
    let records = serde_json::to_string(&(
        h.port.observe("local-proof", "run-a").await.unwrap(),
        realization,
        report,
    ))
    .unwrap();
    for private in
        [h.root.path().to_str().unwrap(), "/proc/", "\"pid\"", "ownership_nonce", "token"]
    {
        assert!(!records.contains(private));
    }
    h.submit(Operation::Release).await.unwrap();
    h.submit(Operation::Release).await.unwrap();
    assert_eq!(fs::read_dir(h.root.path()).unwrap().count(), 0);
    assert!(h.submit(Operation::Start).await.is_err());
}
#[tokio::test]
async fn coding_fixture_edits_the_copied_sample_and_exports_patch_and_report() {
    let patch=b"--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -10,10 +10,10 @@\n     Some((key.trim(), value.trim()))\n }\n \n-/// Calculate the averge of a slice of numbers.\n+/// Calculate the average of a slice of numbers.\n ///\n /// Returns 0.0 for an empty slice.\n-pub fn calculate_averge(numbers: &[f64]) -> f64 {\n+pub fn calculate_average(numbers: &[f64]) -> f64 {\n     if numbers.is_empty() {\n         return 0.0;\n     }\n";
    let report = b"{\"changed\":[\"src/lib.rs\"],\"task\":\"rename-average\",\"schema\":1}";
    let h = harness(
        Task::Coding,
        1500,
        65536,
        vec![export("change.patch", patch), export("report.json", report)],
    );
    h.start().await;
    assert!(h.finished().cleanup_confirmed);
    let allocation = fs::read_dir(h.root.path()).unwrap().next().unwrap().unwrap().path();
    let edited = fs::read_to_string(allocation.join("work/sample-rust/src/lib.rs")).unwrap();
    assert!(edited.contains("fn calculate_average"));
    assert!(!edited.contains("averge"));
    let original =
        include_str!("../../../fixtures/pre-wi-fabric-2/exo/repos/sample-rust/src/lib.rs");
    assert_eq!(edited, original.replace("averge", "average"));
    assert_eq!(
        fs::read(allocation.join("work/sample-rust/Cargo.toml")).unwrap(),
        include_bytes!("../../../fixtures/pre-wi-fabric-2/exo/repos/sample-rust/Cargo.toml")
    );
    let verification = private_dir();
    fs::create_dir(verification.path().join("src")).unwrap();
    fs::write(verification.path().join("src/lib.rs"), original).unwrap();
    fs::write(verification.path().join("change.patch"), patch).unwrap();
    let status = std::process::Command::new("timeout")
        .args(["10", "git", "apply", "change.patch"])
        .current_dir(verification.path())
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(fs::read_to_string(verification.path().join("src/lib.rs")).unwrap(), edited);

    h.submit(Operation::CollectArtifacts).await.unwrap();
    assert_eq!(h.port.read_artifact("artifact-change.patch").await.unwrap(), patch);
    assert_eq!(h.port.read_artifact("artifact-report.json").await.unwrap(), report);
    h.submit(Operation::Release).await.unwrap();
}
#[tokio::test]
async fn wall_and_simultaneous_output_bounds_run_without_observation_polling() {
    for task in [Task::Sleep { millis: 30_000 }, Task::Flood { bytes: 1024 * 1024 }] {
        let flood = matches!(task, Task::Flood { .. });
        let h = harness(task, 400, 8192, vec![]);
        h.start().await;
        // No observations, futures, or evidence polls drive the background supervisor.
        std::thread::sleep(Duration::from_millis(2200));
        let report = h.finished();
        assert!(report.exit_confirmed && report.cleanup_confirmed, "{report:?}");
        assert!(report.retained_output_bytes <= 8192);
        assert_eq!(report.reason, if flood { "output bound" } else { "wall-time bound" });
        h.submit(Operation::Release).await.unwrap();
    }
}
#[tokio::test]
async fn descendants_grandchildren_sessions_and_early_leader_exit_are_cleaned_with_escalation() {
    for (early_exit, change_session, resist_term) in
        [(false, false, false), (false, true, true), (true, true, true)]
    {
        let h = harness(Task::Tree { early_exit, change_session, resist_term }, 400, 65536, vec![]);
        h.start().await;
        let report = h.finished();
        assert!(report.exit_confirmed && report.cleanup_confirmed, "{report:?}");
        assert!(report.descendants_observed >= 1);
        if resist_term {
            assert!(report.kill_signals >= 1, "{report:?}");
        }
        h.submit(Operation::Release).await.unwrap();
    }
}
#[tokio::test]
async fn environment_and_deliberately_non_cloexec_descriptor_are_excluded() {
    let sentinel = tempfile::NamedTempFile::new().unwrap();
    let fd = sentinel.as_file().as_raw_fd();
    assert!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } >= 0);
    std::env::set_var("WI_SENTINEL_SECRET", "do-not-inherit-this-test-sentinel");
    let h = harness(Task::Probe, 1500, 65536, vec![export("probe.json", b"{\"clean\":true}")]);
    h.start().await;
    assert!(h.finished().cleanup_confirmed);
    h.submit(Operation::CollectArtifacts).await.unwrap();
    assert_eq!(h.port.read_artifact("artifact-probe.json").await.unwrap(), b"{\"clean\":true}");
    h.submit(Operation::Release).await.unwrap();
    std::env::remove_var("WI_SENTINEL_SECRET");
}
#[tokio::test]
async fn unavailable_operations_have_no_process_filesystem_credential_or_observation_side_effect() {
    let h = harness(Task::Sleep { millis: 30_000 }, 2000, 65536, vec![]);
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        let mut r = h.requests[1].clone();
        r.operation = op;
        r.schema = 999;
        r.provider = "unadmitted".into();
        assert_eq!(h.port.submit(&r).await.unwrap(), Acknowledgment::Unavailable);
    }
    assert_eq!(fs::read_dir(h.root.path()).unwrap().count(), 0);
    assert!(h.port.observe("local-proof", "run-a").await.unwrap().is_empty());
    assert!(h.port.evidence("local-proof", "run-a").unwrap().is_none());
    h.start().await;
    let until = Instant::now() + Duration::from_secs(4);
    while Instant::now() < until {
        if h.port
            .observe("local-proof", "run-a")
            .await
            .unwrap()
            .iter()
            .any(|o| o.kind == ObservationKind::Running)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let before = h.port.observe("local-proof", "run-a").await.unwrap();
    assert!(before.iter().any(|o| o.kind == ObservationKind::Running));
    for op in [Operation::Command, Operation::Checkpoint, Operation::Suspend, Operation::Resume] {
        let mut r = h.requests[1].clone();
        r.operation = op;
        assert_eq!(h.port.submit(&r).await.unwrap(), Acknowledgment::Unavailable);
    }
    assert_eq!(h.port.observe("local-proof", "run-a").await.unwrap(), before);
    assert!(h.port.evidence("local-proof", "run-a").unwrap().is_none());
    h.submit(Operation::Terminate).await.unwrap();
    assert!(h.finished().cleanup_confirmed);
    h.submit(Operation::Release).await.unwrap();
}
#[tokio::test]
async fn exact_admission_rejects_executable_configuration_input_control_and_request_drift() {
    let root = private_dir();
    let port = port(root.path(), scope());
    for field in [
        "executable",
        "configuration",
        "input",
        "controls",
        "authorization",
        "adapter",
        "dependencies",
        "scope",
        "credential",
        "request",
        "arguments",
    ] {
        let mut a = authorization(Task::Sleep { millis: 1 }, 1000, 65536, vec![], field);
        match field {
            "executable" => a.operations[0].1.executable_digest = ContentHash::of(b"drift"),
            "configuration" => a.operations[0].1.configuration_digest = ContentHash::of(b"drift"),
            "input" => a.inputs[0].bytes.push(0),
            "controls" => a.controls.applied[0].state = ControlState::Supported(999),
            "authorization" => a.operations[0].1.authorization_digest = ContentHash::of(b"drift"),
            "adapter" => a.operations[0].1.adapter_digest = ContentHash::of(b"drift"),
            "dependencies" => a.operations[0].1.dependencies_digest = ContentHash::of(b"drift"),
            "scope" => a.scope.tenant = "tenant-b".into(),
            "credential" => a.credential.audience = "other".into(),
            "request" => {
                a.operations[1].0.request = a.operations[0].0.request.clone();
                a.operations[1].1.request_digest = a.operations[1].0.digest().unwrap();
            }
            _ => a.task = Task::Sleep { millis: 60_001 },
        }
        assert!(port.admit(a).is_err(), "{field}");
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    let a = authorization(Task::Sleep { millis: 1 }, 1000, 65536, vec![], "accepted");
    let mut req = a.operations[1].0.clone();
    port.admit(a).unwrap();
    req.input_digest = ContentHash::of(b"drift");
    assert!(port.submit(&req).await.is_err());
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}
#[tokio::test]
async fn artifact_integrity_symlink_hardlink_and_special_files_are_quarantined() {
    for attack in ["digest", "symlink", "hardlink", "fifo", "directory", "size"] {
        let h =
            harness(Task::Transform { xor: 0 }, 1500, 65536, vec![export("output.bin", b"hello")]);
        h.start().await;
        assert!(h.finished().cleanup_confirmed);
        let allocation = fs::read_dir(h.root.path()).unwrap().next().unwrap().unwrap().path();
        let file = allocation.join("export/output.bin");
        match attack {
            "digest" => fs::write(&file, b"other").unwrap(),
            "symlink" => {
                fs::remove_file(&file).unwrap();
                std::os::unix::fs::symlink("../input/bytes", &file).unwrap();
            }
            "hardlink" => {
                fs::remove_file(&file).unwrap();
                fs::hard_link(allocation.join("input/bytes"), &file).unwrap();
            }
            "fifo" => {
                fs::remove_file(&file).unwrap();
                let name = std::ffi::CString::new(file.to_str().unwrap()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            }
            "directory" => {
                fs::remove_file(&file).unwrap();
                fs::create_dir(&file).unwrap();
            }
            _ => fs::write(&file, b"larger-than-approved").unwrap(),
        }
        assert!(h.submit(Operation::CollectArtifacts).await.is_err(), "{attack}");
        assert!(h.port.read_artifact("artifact-output.bin").await.is_err());
        assert!(h.submit(Operation::Release).await.is_err());
    }
}
#[tokio::test]
async fn duplicate_start_is_single_launch_and_revocation_stops_run_and_denies_reads() {
    let h = harness(Task::Sleep { millis: 30_000 }, 3000, 65536, vec![]);
    h.start().await;
    for _ in 0..8 {
        h.submit(Operation::Start).await.unwrap();
    }
    assert_eq!(fs::read_dir(h.root.path()).unwrap().count(), 1);
    h.port.revoke(1).unwrap();
    assert!(h.port.observe("local-proof", "run-a").await.is_err());
    assert!(h.submit(Operation::Start).await.is_err());
    // Drop joins the independent supervisor and cleans the owned process even when credentials are revoked.
}
#[test]
fn executable_path_replacement_cannot_change_reviewed_bytes() {
    let root = private_dir();
    let copy = root.path().join("fixture");
    fs::copy(env!("CARGO_BIN_EXE_wi-process-fixture"), &copy).unwrap();
    let expected = executable_digest(&copy).unwrap();
    let mut registry = ExecutableRegistry::default();
    registry.register("fixture-v1", &copy, &expected).unwrap();
    fs::write(&copy, b"unreviewed replacement").unwrap();
    assert!(registry.register("replacement", &copy, &expected).is_err());
    // The registered capability is a sealed copy. Execute it after replacing its original pathname.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let workspace = private_dir();
        let helper = Path::new(env!("CARGO_BIN_EXE_wi-process-reaper"));
        let p = ProcessWorkerPort::new(
            scope(),
            workspace.path(),
            helper,
            &executable_digest(helper).unwrap(),
            registry,
            Arc::new(SystemClock),
        )
        .unwrap();
        let a = authorization(Task::Sleep { millis: 1 }, 1000, 65536, vec![], "replace");
        let requests = a.operations.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
        p.admit(a).unwrap();
        p.submit(&requests[0]).await.unwrap();
        p.submit(&requests[1]).await.unwrap();
        let until = Instant::now() + Duration::from_secs(6);
        loop {
            if let Some(report) = p.evidence("local-proof", "replace").unwrap() {
                assert!(report.cleanup_confirmed, "{report:?}");
                break;
            }
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(10));
        }
    });
}

#[tokio::test]
async fn expired_and_cross_run_credentials_are_rejected_before_allocation() {
    let root = private_dir();
    let port = port(root.path(), scope());
    for kind in [
        "expired",
        "revoked",
        "run",
        "provider",
        "principal",
        "issuer",
        "audience",
        "operation",
        "artifact",
    ] {
        let mut a = authorization(
            Task::Sleep { millis: 1 },
            1000,
            65536,
            vec![export("output.bin", b"hello")],
            kind,
        );
        match kind {
            "expired" => a.credential.expires_unix_ms = SystemClock.unix_ms(),
            "revoked" => a.credential.revocation_generation = 1,
            "run" => a.credential.run = "different-run".into(),
            "provider" => a.credential.provider = "different-provider".into(),
            "principal" => a.credential.principal = "different-principal".into(),
            "issuer" => a.credential.issuer = "different-issuer".into(),
            "audience" => a.credential.audience = "different-audience".into(),
            "operation" => a.credential.operations.retain(|op| *op != Operation::Start),
            _ => a.credential.artifact_scope.clear(),
        }
        assert!(port.admit(a).is_err(), "{kind}");
    }
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}
#[tokio::test]
async fn expired_or_revoked_artifact_and_observation_access_is_denied() {
    use std::sync::atomic::{AtomicU64, Ordering};
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn unix_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    for revoke in [false, true] {
        let root = private_dir();
        let clock = Arc::new(TestClock(AtomicU64::new(SystemClock.unix_ms())));
        let helper = Path::new(env!("CARGO_BIN_EXE_wi-process-reaper"));
        let p = ProcessWorkerPort::new(
            scope(),
            root.path(),
            helper,
            &executable_digest(helper).unwrap(),
            registry(),
            clock.clone(),
        )
        .unwrap();
        let a = authorization(
            Task::Transform { xor: 0 },
            1500,
            65536,
            vec![export("output.bin", b"hello")],
            "run-a",
        );
        let requests = a.operations.iter().map(|(r, _)| r.clone()).collect();
        p.admit(a).unwrap();
        let h = Harness { port: p, root, requests };
        h.start().await;
        assert!(h.finished().cleanup_confirmed);
        h.submit(Operation::CollectArtifacts).await.unwrap();
        assert!(h.port.read_artifact("artifact-output.bin").await.is_ok());
        if revoke {
            h.port.revoke(1).unwrap();
        } else {
            clock.0.fetch_add(120_000, Ordering::SeqCst);
        }
        assert!(h.port.read_artifact("artifact-output.bin").await.is_err());
        assert!(h.port.observe("local-proof", "run-a").await.is_err());
    }
}
#[tokio::test]
async fn port_drop_joins_supervision_and_reaps_owned_descendants() {
    let h = harness(
        Task::Tree { early_exit: false, change_session: true, resist_term: true },
        3000,
        65536,
        vec![],
    );
    h.start().await;
    let until = Instant::now() + Duration::from_secs(4);
    loop {
        if h.port
            .observe("local-proof", "run-a")
            .await
            .unwrap()
            .iter()
            .any(|o| o.kind == ObservationKind::Running)
        {
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(10));
    }
    // Give the fixture time to fork, then drop without submitting terminate or polling again.
    std::thread::sleep(Duration::from_millis(50));
    let Harness { port, root, .. } = h;
    let started = Instant::now();
    drop(port);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn stop_before_start_prevents_consequential_launch() {
    let h = harness(Task::Sleep { millis: 1 }, 1000, 65536, vec![]);
    h.submit(Operation::Provision).await.unwrap();
    h.submit(Operation::Terminate).await.unwrap();
    assert!(h.submit(Operation::Start).await.is_err());
    assert!(h.port.evidence("local-proof", "run-a").unwrap().is_none());
    h.submit(Operation::Release).await.unwrap();
    assert_eq!(fs::read_dir(h.root.path()).unwrap().count(), 0);
}
