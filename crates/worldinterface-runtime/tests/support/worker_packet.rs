//! Repository handling stays in the test host, outside production execution.
#![allow(dead_code)]
use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::Mutex,
};
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
    verify_response(&output.stdout, manifest)
}

fn verify_response(bytes: &[u8], manifest: &EvaluationManifest) -> Result<()> {
    let result: serde_json::Value = serde_json::from_slice(bytes)?;
    if result
        != serde_json::json!({
            "manifest_digest": manifest.digest()?,
            "acceptance_policy": "reviewed-merged-packet-2026-11-09",
        })
    {
        return Err(format!("evaluation packet rejected: {result}").into());
    }
    Ok(())
}

/// Reuse the interpreter, never a validation result. Each request reconstructs
/// the complete current packet and verifies committed bytes independently.
pub struct Validator {
    root: PathBuf,
    process: Mutex<ValidatorProcess>,
}
struct ValidatorProcess {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}
impl Drop for ValidatorProcess {
    fn drop(&mut self) {
        self.input.take();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Validator {
    pub fn new(root: &Path) -> Result<Self> {
        let root = root.canonicalize()?;
        let mut child = Command::new("python3")
            .args(["-I", "-B"])
            .arg(root.join("scripts/wi_worker_evaluation.py"))
            .arg("--root")
            .arg(&root)
            .args(["--require-committed", "--serve"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child.stdin.take();
        let output = BufReader::new(child.stdout.take().unwrap());
        Ok(Self { root, process: Mutex::new(ValidatorProcess { child, input, output }) })
    }
    pub fn validate(&self, root: &Path, manifest: &EvaluationManifest) -> Result<()> {
        if root.canonicalize()? != self.root {
            return Err("evaluation validator source root mismatch".into());
        }
        let mut process = self.process.lock().map_err(|_| "evaluation validator unavailable")?;
        let input = process.input.as_mut().ok_or("evaluation validator closed")?;
        input.write_all(b"check\n")?;
        input.flush()?;
        let mut response = String::new();
        if process.output.read_line(&mut response)? == 0 {
            return Err("evaluation validator closed".into());
        }
        verify_response(response.as_bytes(), manifest)
    }
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
