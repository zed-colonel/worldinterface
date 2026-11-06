//! Adopted runtime-test entry point. Tooling is a subprocess, never a runtime dependency.
#[test]
fn wi_owned_worker_acceptance_campaign() {
    assert!(std::env::var_os("WI_WORKER_CAMPAIGN_CHILD").is_none(), "recursive campaign");
    std::fs::create_dir_all(
        std::path::PathBuf::from(std::env::var_os("TMPDIR").unwrap()).join("verification"),
    )
    .unwrap();
    let directory = tempfile::Builder::new()
        .prefix("wi-worker-campaign-")
        .tempdir_in(
            std::path::PathBuf::from(
                std::env::var_os("TMPDIR").expect("external scratch required"),
            )
            .join("verification"),
        )
        .expect("verification directory");
    // Persist evidence for review even on failure.
    let path = directory.keep();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = std::process::Command::new("timeout")
        .args([
            "--kill-after=15s",
            "7200s",
            "cargo",
            "run",
            "--locked",
            "--offline",
            "--manifest-path",
            "tools/wi-contract-check/Cargo.toml",
            "--",
            "campaign",
            "run",
        ])
        .arg(&path)
        .current_dir(root)
        .status()
        .unwrap();
    assert!(status.success(), "worker campaign failed; evidence {}", path.display());
}
