//! Repository handling stays in the test host, outside production execution.
#![allow(dead_code)]
use std::{path::Path, process::Command};
use worldinterface_runtime::{worker::evaluation::EvaluationManifest, Result};

pub fn validate(root: &Path, manifest: &EvaluationManifest, committed: bool) -> Result<()> {
    let mut command = Command::new("python3");
    command
        .args(["-I", "-B"])
        .arg(root.join("scripts/wi_worker_evaluation.py"))
        .arg("--root")
        .arg(root);
    if committed {
        command.arg("--require-committed");
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "evaluation packet rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let result: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    if result["manifest_digest"] != serde_json::to_value(manifest.digest()?)? {
        return Err("evaluation packet canonical digest mismatch".into());
    }
    Ok(())
}

pub fn current(root: &Path, committed: bool) -> Result<EvaluationManifest> {
    let manifest: EvaluationManifest = serde_json::from_slice(&std::fs::read(
        root.join("conformance/wi-fabric-2/worker/evaluation-manifest.json"),
    )?)?;
    validate(root, &manifest, committed)?;
    Ok(manifest)
}

/// The live setup boundary must run before even a cached builder or fixture launcher.
pub fn live_setup<T>(
    root: &Path,
    setup: impl FnOnce(EvaluationManifest) -> Result<T>,
) -> Result<T> {
    setup(current(root, true)?)
}
