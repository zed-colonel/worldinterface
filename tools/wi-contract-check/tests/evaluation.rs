//! Exercise the validator used by the adopted contract-check, without worker execution.
use serde_json::json;
use std::{fs, path::Path, process::Command};
use wi_contract_check::{evaluation, read, worker_campaign};

#[test]
fn complete_packet_validator_regressions() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new("timeout")
        .args(["120s", "python3", "-I", "-B"])
        .arg(root.join("scripts/test_wi_worker_evaluation.py"))
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

#[test]
fn adopted_evaluation_check_rejects_stale_packets_and_uncommitted_refresh() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = tempfile::tempdir().unwrap();
    let manifest = read(&source.join(evaluation::PACKET)).unwrap();
    for name in manifest["files"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .chain([evaluation::PACKET])
    {
        let dest = root.path().join(name);
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        fs::copy(source.join(name), dest).unwrap();
    }
    let git = |args: &[&str]| {
        assert!(Command::new("git")
            .args(args)
            .current_dir(root.path())
            .output()
            .unwrap()
            .status
            .success());
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
    assert!(evaluation::check(root.path()).unwrap().is_empty());
    fs::write(root.path().join("crates/worldinterface-runtime/src/lib.rs"), b"changed").unwrap();
    assert!(evaluation::check(root.path()).is_err());
    assert!(Command::new("python3")
        .args(["-B", "scripts/refresh-wi-worker-evaluation.py"])
        .current_dir(root.path())
        .output()
        .unwrap()
        .status
        .success());
    assert!(evaluation::check(root.path()).is_err());
}

#[test]
fn campaign_packet_and_build_identity_cannot_be_substituted() {
    let manifest = json!({"fixture":"reviewed"});
    let record = evaluation::packet_record(&manifest).unwrap();
    let observation = json!({"build":{"packet":record["manifest"],"profile_definition":"profile"}});
    evaluation::validate_record(&record, &manifest).unwrap();
    worker_campaign::validate_build_packet(&observation, &record, "profile").unwrap();
    for field in ["manifest", "acceptance_policy"] {
        let mut changed = record.clone();
        changed[field] = json!("substituted");
        assert!(evaluation::validate_record(&changed, &manifest).is_err());
        assert!(worker_campaign::validate_build_packet(&observation, &changed, "profile").is_err());
    }
    for field in ["packet", "profile_definition"] {
        let mut changed = observation.clone();
        changed["build"][field] = json!("substituted");
        assert!(worker_campaign::validate_build_packet(&changed, &record, "profile").is_err());
    }
}
