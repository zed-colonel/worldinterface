//! The repository validator reconstructs the entire packet; checks never refresh it.
use crate::{hash, read, Finding, Result};
use serde_json::{json, Value};
use std::{path::Path, process::Command};

pub const PACKET: &str = "conformance/wi-fabric-2/worker/evaluation-manifest.json";
pub const POLICY: &str = "reviewed-merged-packet-2026-11-09";

pub fn packet(root: &Path) -> Result<Value> {
    let output = Command::new("timeout")
        .args(["60s", "python3", "-I", "-B"])
        .arg(root.join("scripts/wi_worker_evaluation.py"))
        .arg("--root")
        .arg(root)
        .arg("--require-committed")
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "evaluation packet rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let result: Value = serde_json::from_slice(&output.stdout)?;
    let manifest = read(&root.join(PACKET))?;
    let record = packet_record(&manifest)?;
    if result != json!({"manifest_digest":record["manifest"], "acceptance_policy":POLICY}) {
        return Err("evaluation validator canonical digest or policy mismatch".into());
    }
    Ok(record)
}

pub fn packet_record(manifest: &Value) -> Result<Value> {
    Ok(json!({"manifest":hash(serde_json::to_vec(manifest)?), "acceptance_policy":POLICY}))
}

pub fn validate_record(record: &Value, manifest: &Value) -> Result<()> {
    if *record != packet_record(manifest)? {
        return Err("substituted evaluation packet or acceptance policy".into());
    }
    Ok(())
}

pub fn check(root: &Path) -> Result<Vec<Finding>> {
    packet(root)?;
    Ok(vec![])
}
