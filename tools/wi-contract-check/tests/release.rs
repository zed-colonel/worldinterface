//! Release tooling stays in the adopted contract-test surface.
#[test]
fn release_preparation_regressions() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("timeout")
        .args(["300s", "python3", "-I", "-B"])
        .arg(root.join("scripts/test_wi_release.py"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
