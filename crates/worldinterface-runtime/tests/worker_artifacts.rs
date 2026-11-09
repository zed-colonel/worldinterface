#![cfg(target_os = "linux")]
mod support {
    pub mod connectors;
    pub mod flow_fixture;
    pub mod spi_harness;
    pub mod worker_fixture;
}
use support::worker_fixture::*;
use worldinterface_domain::worker::{WorkerBinding, WorkerCollection};
use worldinterface_protocol::*;
use worldinterface_runtime::worker_process::*;
use worldinterface_storage::{flow::FlowOutcome, StoreKind, StoreRoot};
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

fn assert_collection_quarantined(
    root: &StoreRoot,
    binding: &WorkerBinding,
    code: &str,
) -> WorkerCollection {
    let state =
        root.worker_snapshot(&binding.tenant, &binding.provider, &binding.run).unwrap().unwrap();
    assert!(state.quarantined && state.stop_requested && state.capacity_reserved);
    assert!(!state.completed && !state.cleanup_confirmed);
    let WorkerCollection::Quarantined(report) = &state.collection else {
        panic!("collection was not quarantined: {:?}", state.collection);
    };
    let operation =
        state.operations.values().find(|o| o.operation.as_str() == "collect_artifacts").unwrap();
    assert!(!operation.acknowledged);
    assert_eq!(report.binding, *binding);
    assert_eq!(report.operation, operation.key);
    assert_eq!(report.codes, vec![code.to_string().try_into().unwrap()]);
    assert_eq!(report.policy, ContentHash::of(&canonical_data(&state.artifact_policy[0]).unwrap()));
    for export in &state.artifact_policy[0].exports {
        assert!(worldinterface_runtime::worker::artifacts::read(
            root,
            binding,
            export.reference.as_str()
        )
        .is_err());
        assert!(
            root.read_artifact(&ArtifactRef {
                scope: binding.tenant.clone(),
                content: export.digest.clone(),
                size: export.size,
            })
            .is_err(),
            "rejected export reached the public artifact store"
        );
    }
    let signals = root.signal_obligations(StoreKind::Fabric, None, 100).unwrap();
    assert!(signals.iter().any(|s| s.event.route.kind
        == worldinterface_domain::continuation::EventKind::WorkerObservation
        && s.event.route.correlation == binding.event_correlation().unwrap()));
    for signal in signals {
        assert_eq!(
            root.authoritative_event(StoreKind::Fabric, &signal.event.id).unwrap(),
            signal.event
        );
    }
    state.collection
}

// F-015: exercise the connector's rejection path through admission, the real
// executor/AQ and its outbox, rather than calling the adapter or collector alone.
#[tokio::test]
async fn wp_028_unsafe_live_exports_quarantine_and_report_through_real_wi_aq() {
    for attack in ["symlink", "hardlink", "fifo"] {
        let mut h = Harness::new(Task::Transform { xor: 7 }, "WP-028", 1500, 65536);
        h.inputs = vec![h.bundle.wi().publish(h.auth.0.clone(), b"hello").unwrap()];
        let bytes: Vec<_> = b"hello".iter().map(|b| b ^ 7).collect();
        h.exports = vec![Export {
            reference: "result".into(),
            name: "output.bin".into(),
            digest: ContentHash::of(&bytes),
            size: bytes.len() as u64,
        }];
        h.operation(Operation::Start).await;
        let cleanup = h.finish().await;
        assert!(cleanup.exit_confirmed && cleanup.cleanup_confirmed);
        assert_eq!(cleanup.exit_success, Some(true));
        assert_eq!(h.runtime.spawn_attempts(), Some(1));
        assert!(matches!(h.state().collection, WorkerCollection::Absent));
        let allocations: Vec<_> = std::fs::read_dir(h.root.join("process"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_dir())
            .collect();
        assert_eq!(allocations.len(), 1);
        let file = allocations[0].join("export/output.bin");
        assert_eq!(std::fs::read(&file).unwrap(), bytes);
        let source = h.root.join("export-source");
        std::fs::write(&source, &bytes).unwrap();
        std::fs::remove_file(&file).unwrap();
        match attack {
            "symlink" => std::os::unix::fs::symlink(&source, &file).unwrap(),
            "hardlink" => std::fs::hard_link(&source, &file).unwrap(),
            "fifo" => {
                let name = std::ffi::CString::new(file.as_os_str().as_encoded_bytes()).unwrap();
                assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            }
            _ => unreachable!(),
        }
        h.operation_expect(Operation::CollectArtifacts, FlowOutcome::Uncertain).await;
        let binding = h.binding();
        let decided =
            assert_collection_quarantined(h.bundle.wi(), &binding, "provider-collection-rejected");
        assert_eq!(h.runtime.spawn_attempts(), Some(1));
        h.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
        assert_eq!(
            assert_collection_quarantined(h.bundle.wi(), &binding, "provider-collection-rejected"),
            decided
        );
        let h = h.reopen();
        assert_eq!(
            assert_collection_quarantined(h.bundle.wi(), &binding, "provider-collection-rejected"),
            decided
        );
        assert_eq!(h.runtime.spawn_attempts(), Some(0), "reopen must not launch a replacement");
    }
}

#[tokio::test]
async fn wp_028_unavailable_seam_export_quarantines_after_provider_ack_through_real_wi_aq() {
    let mut h = Harness::new(Task::Transform { xor: 7 }, "WP-028", 1500, 65536);
    h.inputs = vec![h.bundle.wi().publish(h.auth.0.clone(), b"hello").unwrap()];
    let bytes: Vec<_> = b"hello".iter().map(|b| b ^ 7).collect();
    h.exports = vec![Export {
        reference: "result".into(),
        name: "output.bin".into(),
        digest: ContentHash::of(&bytes),
        size: bytes.len() as u64,
    }];
    let (h, provider) = h.with_seam();
    assert_eq!(seam::LABEL, "seam simulation");
    h.operation(Operation::Start).await;
    h.reconcile_start().await;
    assert!(h.finish().await.cleanup_confirmed);
    assert_eq!(provider.launches(), 1);
    // A real asynchronous seam implementation acknowledges collection but has
    // lost its export bytes. WI must retain uncertainty and a quarantine report.
    provider.discard_artifacts();
    h.operation_expect(Operation::CollectArtifacts, FlowOutcome::Uncertain).await;
    let binding = h.binding();
    let decided =
        assert_collection_quarantined(h.bundle.wi(), &binding, "provider-collection-unavailable");
    assert_eq!(provider.launches(), 1);
    h.bundle.wi().repository(StoreKind::Fabric).unwrap().rebuild().unwrap();
    assert_eq!(
        assert_collection_quarantined(h.bundle.wi(), &binding, "provider-collection-unavailable"),
        decided
    );
    let path = h.root.join("bundle");
    let Harness { temp: _retained_temp, bundle, runtime, .. } = h;
    drop(runtime);
    drop(bundle);
    drop(provider);
    let bundle = worldinterface_runtime::storage::StoreBundle::open(&path).unwrap();
    assert_eq!(
        assert_collection_quarantined(bundle.wi(), &binding, "provider-collection-unavailable"),
        decided
    );
}
