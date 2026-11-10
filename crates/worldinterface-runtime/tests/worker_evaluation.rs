//! Packet boundary tests deliberately launch no worker processes.
#[path = "support/worker_packet.rs"]
mod packet;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, Ordering},
};
use worldinterface_protocol::{canonical_data, ContentHash};
use worldinterface_runtime::{worker::evaluation::*, worker_process::Task, Result};
struct Trust {
    packet: ContentHash,
    revoked: AtomicBool,
}
impl TrustedApprovals for Trust {
    fn accepted(&self, m: &EvaluationManifest, _: &std::path::Path) -> Result<bool> {
        Ok(m.digest()? == self.packet && !self.revoked.load(Ordering::SeqCst))
    }
}
fn fixture() -> (tempfile::TempDir, EvaluationVerifier<Trust>) {
    let t = tempfile::tempdir().unwrap();
    std::fs::write(t.path().join("fixture.rs"), b"reviewed source").unwrap();
    let m = EvaluationManifest {
        version: 1,
        profile: "process-managed-proof-1".into(),
        environment: ContentHash::of(b"linux-private-nonsensitive"),
        files: BTreeMap::from([("fixture.rs".into(), ContentHash::of(b"reviewed source"))]),
        tasks: BTreeSet::from([ContentHash::of(&canonical_data(&Task::Probe).unwrap())]),
        cases: BTreeSet::from(["WP-017".into()]),
        operations: BTreeSet::from(["start".into()]),
        max_wall_ms: 15000,
        max_output_bytes: 65536,
        nonsensitive_only: true,
        production_credentials: false,
        ordinary_use: false,
    };
    let packet = m.digest().unwrap();
    let v = EvaluationVerifier {
        manifest: m,
        trusted: Trust { packet, revoked: AtomicBool::new(false) },
        source_root: t.path().into(),
    };
    (t, v)
}
#[test]
fn packet_acceptance_is_independent_of_calendar_and_rejects_revocation() {
    let (_t, v) = fixture();
    for now in [0, 100, 200, u64::MAX] {
        v.verify(now).unwrap();
    }
    v.trusted.revoked.store(true, Ordering::SeqCst);
    assert!(v.verify(100).is_err());
}
#[test]
fn committed_packet_cannot_widen_tasks_operations_cases_environment_or_source_bindings() {
    let (t, mut v) = fixture();
    let env = v.manifest.environment.clone();
    v.authorize(100, "WP-017", "start", &Task::Probe, &env).unwrap();
    for (case, op, task, environment) in [
        ("WP-018", "start", Task::Probe, env.clone()),
        ("WP-017", "command", Task::Probe, env.clone()),
        ("WP-017", "start", Task::Transform { xor: 7 }, env.clone()),
        ("WP-017", "start", Task::Probe, ContentHash::of(b"other-environment")),
    ] {
        assert!(v.authorize(100, case, op, &task, &environment).is_err());
    }
    let original = v.manifest.clone();
    for mode in ["ordinary", "credential", "input", "profile", "bound", "new-task"] {
        v.manifest = original.clone();
        match mode {
            "ordinary" => v.manifest.ordinary_use = true,
            "credential" => v.manifest.production_credentials = true,
            "input" => v.manifest.nonsensitive_only = false,
            "profile" => v.manifest.profile = "other".into(),
            "bound" => v.manifest.max_wall_ms = 60_001,
            _ => {
                v.manifest.tasks.insert(ContentHash::of(b"other"));
            }
        };
        // A changed request cannot update the independently bound host packet.
        assert!(v.verify(100).is_err(), "{mode}");
    }
    v.manifest = original;
    std::fs::write(t.path().join("fixture.rs"), b"changed source").unwrap();
    assert!(v.verify(100).is_err());
}

#[test]
fn live_setup_rejects_stale_and_uncommitted_packets_before_fixture_execution() {
    use std::{path::Path, process::Command, sync::atomic::AtomicUsize};
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = tempfile::tempdir().unwrap();
    let packet_path = "conformance/wi-fabric-2/worker/evaluation-manifest.json";
    let raw = std::fs::read(source.join(packet_path)).unwrap();
    let manifest: EvaluationManifest = serde_json::from_slice(&raw).unwrap();
    for name in manifest.files.keys().map(String::as_str).chain([packet_path]) {
        let dest = root.path().join(name);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::copy(source.join(name), dest).unwrap();
    }
    let git = |args: &[&str]| {
        let output = Command::new("git").args(args).current_dir(root.path()).output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    };
    git(&["init", "-q"]);
    git(&["add", "-f", "."]);
    git(&[
        "-c",
        "user.name=Packet Test",
        "-c",
        "user.email=packet@example.invalid",
        "commit",
        "-qm",
        "private fixture",
    ]);
    let launches = AtomicUsize::new(0);
    let setup = |_| -> Result<()> {
        launches.fetch_add(1, Ordering::SeqCst);
        Ok(())
    };
    // This is the same setup boundary used by worker_fixture::evaluation().
    packet::live_setup(root.path(), setup).unwrap();
    assert_eq!(launches.swap(0, Ordering::SeqCst), 1);
    std::fs::write(root.path().join("crates/worldinterface-runtime/src/lib.rs"), b"changed")
        .unwrap();
    assert!(packet::live_setup(root.path(), setup).is_err());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
    let refresh = Command::new("python3")
        .args(["-B", "scripts/refresh-wi-worker-evaluation.py"])
        .current_dir(root.path())
        .output()
        .unwrap();
    assert!(refresh.status.success(), "{}", String::from_utf8_lossy(&refresh.stderr));
    packet::current(root.path(), false).unwrap();
    assert!(packet::live_setup(root.path(), setup).is_err());
    git(&["add", "-f", "."]);
    assert!(packet::live_setup(root.path(), setup).is_err());
    assert_eq!(launches.load(Ordering::SeqCst), 0);
}
