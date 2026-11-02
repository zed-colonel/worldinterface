//! Approval boundary tests deliberately launch no processes.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicBool, Ordering},
};
use worldinterface_protocol::{canonical_data, ContentHash};
use worldinterface_runtime::{worker::evaluation::*, worker_process::Task, Result};
struct Trust {
    receipt: OperatorApprovalReceipt,
    revoked: AtomicBool,
}
impl TrustedApprovals for Trust {
    fn accepted(&self, r: &OperatorApprovalReceipt, now: u64) -> Result<bool> {
        Ok(r == &self.receipt && now < r.expires_unix_ms && !self.revoked.load(Ordering::SeqCst))
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
        max_wall_ms: 1000,
        max_output_bytes: 65536,
        nonsensitive_only: true,
        production_credentials: false,
        ordinary_use: false,
    };
    let receipt = OperatorApprovalReceipt {
        decision: "isolated-negative-test-decision".into(),
        operator: "test-authority".into(),
        manifest: m.digest().unwrap(),
        expires_unix_ms: 200,
    };
    let v = EvaluationVerifier {
        manifest: m,
        receipt: receipt.clone(),
        trusted: Trust { receipt, revoked: AtomicBool::new(false) },
        source_root: t.path().into(),
    };
    (t, v)
}
#[test]
fn approval_rejects_drafts_self_assertions_unknown_decisions_expiry_and_revocation() {
    let (_t, mut v) = fixture();
    v.verify(100).unwrap();
    let original = v.receipt.clone();
    for change in ["draft", "unknown", "operator", "digest", "expired"] {
        v.receipt = original.clone();
        match change {
            "draft" => v.receipt.decision.clear(),
            "unknown" => v.receipt.decision = "invented-acceptance".into(),
            "operator" => v.receipt.operator = "self-asserted".into(),
            "digest" => v.receipt.manifest = ContentHash::of(b"other"),
            _ => v.receipt.expires_unix_ms = 99,
        };
        assert!(v.verify(100).is_err(), "{change}");
    }
    v.receipt = original;
    assert!(v.verify(200).is_err());
    v.trusted.revoked.store(true, Ordering::SeqCst);
    assert!(v.verify(100).is_err());
    assert!(serde_json::from_str::<OperatorApprovalReceipt>(
        r#"{"status":"accepted","decision":"invented"}"#
    )
    .is_err());
}
#[test]
fn approved_packet_cannot_widen_tasks_operations_cases_environment_or_source_bindings() {
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
        // Updating a repository receipt cannot update the independently accepted host record.
        v.receipt.manifest = v.manifest.digest().unwrap();
        assert!(v.verify(100).is_err(), "{mode}");
    }
    v.manifest = original;
    v.receipt = v.trusted.receipt.clone();
    std::fs::write(t.path().join("fixture.rs"), b"changed source").unwrap();
    assert!(v.verify(100).is_err());
}
