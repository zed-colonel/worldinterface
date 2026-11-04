#![cfg(target_os = "linux")]
mod support {
    pub mod connectors;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use support::worker_fixture::*;
use worldinterface_domain::worker::WorkerCollection;
use worldinterface_protocol::*;
use worldinterface_runtime::worker_process::*;
use worldinterface_storage::flow::FlowOutcome;
use worldinterface_worker::mapping::Operation;
#[tokio::test]
async fn wp_027_scoped_transform_import_and_durable_export_through_real_wi_aq() {
    let mut h = Harness::new(Task::Transform { xor: 7 }, "WP-027", 1500, 65536);
    h.inputs = vec![h.bundle.wi().publish(h.auth.0.clone(), b"hello").unwrap()];
    let bytes: Vec<_> = b"hello".iter().map(|b| b ^ 7).collect();
    h.exports = vec![Export {
        reference: "result".into(),
        name: "output.bin".into(),
        digest: ContentHash::of(&bytes),
        size: bytes.len() as u64,
    }];
    h.operation(Operation::Start).await;
    h.finish().await;
    assert!(h.runtime.read_artifact(&h.binding(), "result").is_err());
    assert!(!h.state().completed);
    h.operation(Operation::CollectArtifacts).await;
    assert!(h.state().completed && matches!(h.state().collection, WorkerCollection::Published(_)));
    assert_eq!(h.runtime.read_artifact(&h.binding(), "result").unwrap(), bytes);
    for scope in 0..4 {
        let mut b = h.binding();
        match scope {
            0 => b.tenant = "other".to_string().try_into().unwrap(),
            1 => b.principal = PrincipalId::generate(),
            2 => b.run = "other".to_string().try_into().unwrap(),
            _ => b.realization = ContentHash::of(b"other"),
        };
        assert!(h.runtime.read_artifact(&b, "result").is_err());
    }
    // F-001: a normal restart before Release retains published work even when
    // the new host cannot establish ownership of the old provider allocation.
    let published = h.state().collection;
    let h = h.reopen();
    h.operation_expect(Operation::Release, FlowOutcome::Uncertain).await;
    let state = h.state();
    assert!(state.completed && state.quarantined && state.capacity_reserved);
    assert!(!state.cleanup_confirmed);
    assert_eq!(state.collection, published);
    assert_eq!(h.runtime.read_artifact(&h.binding(), "result").unwrap(), bytes);
    // Retrying the already acknowledged collection cannot revoke AQ success.
    h.operation(Operation::CollectArtifacts).await;
    let h = h.reopen();
    assert!(h.state().completed);
    assert_eq!(h.state().collection, published);
    assert_eq!(h.runtime.read_artifact(&h.binding(), "result").unwrap(), bytes);
}
#[tokio::test]
async fn wp_027_coding_patch_and_schema_report_publish_before_completion() {
    let mut h = Harness::new(Task::Coding, "WP-027", 1500, 65536);
    let patch=b"--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -10,10 +10,10 @@\n     Some((key.trim(), value.trim()))\n }\n \n-/// Calculate the averge of a slice of numbers.\n+/// Calculate the average of a slice of numbers.\n ///\n /// Returns 0.0 for an empty slice.\n-pub fn calculate_averge(numbers: &[f64]) -> f64 {\n+pub fn calculate_average(numbers: &[f64]) -> f64 {\n     if numbers.is_empty() {\n         return 0.0;\n     }\n";
    let report = b"{\"changed\":[\"src/lib.rs\"],\"task\":\"rename-average\",\"schema\":1}";
    h.exports =
        [("patch", "change.patch", patch.as_slice()), ("report", "report.json", report.as_slice())]
            .into_iter()
            .map(|(r, n, b)| Export {
                reference: r.into(),
                name: n.into(),
                digest: ContentHash::of(b),
                size: b.len() as u64,
            })
            .collect();
    h.operation(Operation::Start).await;
    h.finish().await;
    assert!(!h.state().completed);
    h.operation(Operation::CollectArtifacts).await;
    let WorkerCollection::Published(p) = h.state().collection else { panic!("publication absent") };
    assert_eq!(p.exports.len(), 2);
    assert!(p.prepared.findings.iter().all(|f| f.passed));
    assert_eq!(h.runtime.read_artifact(&h.binding(), "patch").unwrap(), patch);
    assert_eq!(h.runtime.read_artifact(&h.binding(), "report").unwrap(), report);
    h.operation(Operation::Release).await;
}
