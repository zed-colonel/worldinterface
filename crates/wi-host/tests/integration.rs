//! Integration tests for EmbeddedHost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use wi_connector::connectors::{DelayConnector, FsReadConnector, FsWriteConnector};
use wi_connector::ConnectorRegistry;
use wi_core::flowspec::*;
use wi_core::id::{FlowRunId, NodeId};
use wi_host::{EmbeddedHost, FlowPhase, FlowRunStatus, HostConfig, HostError, StepPhase};

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build a registry WITHOUT the HTTP connector (reqwest::blocking::Client
/// creates an internal tokio runtime, incompatible with #[tokio::test]).
fn test_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry
}

fn test_config(dir: &std::path::Path) -> HostConfig {
    HostConfig {
        aq_data_dir: dir.join("aq"),
        context_store_path: dir.join("context.db"),
        tick_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

fn connector_node(id: NodeId, name: &str, params: serde_json::Value) -> Node {
    Node {
        id,
        label: Some(name.into()),
        node_type: NodeType::Connector(ConnectorNode {
            connector: name.into(),
            params,
            idempotency_config: None,
        }),
    }
}

fn delay_node(id: NodeId, ms: u64) -> Node {
    connector_node(id, "delay", json!({"duration_ms": ms}))
}

fn identity_node(id: NodeId) -> Node {
    Node {
        id,
        label: Some("identity".into()),
        node_type: NodeType::Transform(TransformNode {
            transform: TransformType::Identity,
            input: json!({}),
        }),
    }
}

fn edge(from: NodeId, to: NodeId) -> Edge {
    Edge { from, to, condition: None }
}

fn branch_edge(from: NodeId, to: NodeId, condition: EdgeCondition) -> Edge {
    Edge { from, to, condition: Some(condition) }
}

fn make_spec(nodes: Vec<Node>, edges: Vec<Edge>) -> FlowSpec {
    FlowSpec { id: None, name: None, nodes, edges, params: None }
}

async fn poll_until_terminal(host: &EmbeddedHost, flow_run_id: FlowRunId) -> FlowRunStatus {
    let timeout = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status = host.run_status(flow_run_id).await.unwrap();
        match status.phase {
            FlowPhase::Completed | FlowPhase::Failed | FlowPhase::Canceled => return status,
            _ => {
                if tokio::time::Instant::now() > timeout {
                    panic!(
                        "flow {flow_run_id} did not reach terminal state within 10s, phase: {:?}",
                        status.phase
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

// ── T-2: Host Lifecycle ─────────────────────────────────────────────────

#[tokio::test]
async fn host_starts_with_default_config() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_starts_with_empty_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    assert!(!dir.path().join("aq").exists());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();
    assert!(dir.path().join("aq").exists());
    assert!(dir.path().join("context.db").exists());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_starts_with_existing_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());

    // First start — creates directories
    {
        let host = EmbeddedHost::start(config.clone(), test_registry()).await.unwrap();
        host.shutdown().await.unwrap();
    }

    // Second start — replays from existing data
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_shutdown_stops_tick_loop() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();
    // Shutdown should complete without hanging
    tokio::time::timeout(Duration::from_secs(5), host.shutdown())
        .await
        .expect("shutdown should complete within 5s")
        .unwrap();
}

// ── T-3: Flow Submission ────────────────────────────────────────────────

#[tokio::test]
async fn submit_valid_flow_returns_flow_run_id() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 10)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();
    assert!(!frid.as_ref().is_nil());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn submit_invalid_flow_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new()];
    let spec = make_spec(
        vec![delay_node(ids[0], 10), delay_node(ids[1], 10)],
        vec![edge(ids[0], ids[1]), edge(ids[1], ids[0])], // cycle
    );
    let err = host.submit_flow(spec).await.unwrap_err();
    assert!(matches!(err, HostError::Compilation(_)));

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn submit_multiple_flows() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let mut frids = Vec::new();
    for _ in 0..3 {
        let id = NodeId::new();
        let spec = make_spec(vec![delay_node(id, 10)], vec![]);
        frids.push(host.submit_flow(spec).await.unwrap());
    }

    // All distinct
    frids.sort_by_key(|f| *AsRef::<uuid::Uuid>::as_ref(f));
    frids.dedup_by_key(|f| *AsRef::<uuid::Uuid>::as_ref(f));
    assert_eq!(frids.len(), 3);

    host.shutdown().await.unwrap();
}

// ── T-4: Flow Execution (End-to-End) ───────────────────────────────────

#[tokio::test]
async fn single_delay_flow_completes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 10)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn linear_three_step_flow_completes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new(), NodeId::new()];
    let spec = make_spec(
        vec![delay_node(ids[0], 5), delay_node(ids[1], 5), delay_node(ids[2], 5)],
        vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
    );
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);
    assert!(status.outputs.is_some());
    let outputs = status.outputs.unwrap();
    assert_eq!(outputs.len(), 3);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn linear_flow_with_identity_transform() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new(), NodeId::new()];
    let spec = make_spec(
        vec![delay_node(ids[0], 5), identity_node(ids[1]), delay_node(ids[2], 5)],
        vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
    );
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn branch_flow_executes_taken_path() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new(), NodeId::new(), NodeId::new()];
    // A(delay) → Branch → B(delay, then) | C(delay, else)
    let spec = FlowSpec {
        id: None,
        name: Some("branch-test".into()),
        nodes: vec![
            delay_node(ids[0], 5),
            Node {
                id: ids[1],
                label: None,
                node_type: NodeType::Branch(BranchNode {
                    condition: BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                    then_edge: ids[2],
                    else_edge: Some(ids[3]),
                }),
            },
            delay_node(ids[2], 5),
            delay_node(ids[3], 5),
        ],
        edges: vec![
            edge(ids[0], ids[1]),
            branch_edge(ids[1], ids[2], EdgeCondition::BranchTrue),
            branch_edge(ids[1], ids[3], EdgeCondition::BranchFalse),
        ],
        params: Some(json!({"flag": true})),
    };
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    // The "then" path (ids[2]) should have executed, the "else" path (ids[3]) should be excluded
    let step_map: HashMap<NodeId, &wi_host::StepStatus> =
        status.steps.iter().map(|s| (s.node_id, s)).collect();

    assert_eq!(step_map[&ids[2]].phase, StepPhase::Completed);
    // The else branch should be excluded
    assert_eq!(step_map[&ids[3]].phase, StepPhase::Excluded);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn flow_failure_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    // fs.read on a nonexistent file → terminal connector failure
    let id = NodeId::new();
    let spec = make_spec(
        vec![connector_node(id, "fs.read", json!({"path": "/nonexistent/path/file.txt"}))],
        vec![],
    );
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Failed);
    assert!(status.error.is_some(), "failed flow should have an error message");
    assert!(status.outputs.is_none(), "failed flow should have no outputs");

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_step_shows_in_status() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new()];
    // delay(ok) → fs.read(fail)
    let spec = make_spec(
        vec![
            delay_node(ids[0], 5),
            connector_node(ids[1], "fs.read", json!({"path": "/nonexistent/path/file.txt"})),
        ],
        vec![edge(ids[0], ids[1])],
    );
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Failed);

    let step_map: HashMap<NodeId, &wi_host::StepStatus> =
        status.steps.iter().map(|s| (s.node_id, s)).collect();
    // First step should have completed
    assert_eq!(step_map[&ids[0]].phase, StepPhase::Completed);
    // Second step should have failed
    assert_eq!(step_map[&ids[1]].phase, StepPhase::Failed);

    host.shutdown().await.unwrap();
}

// ── T-5: FlowRunStatus Queries ──────────────────────────────────────────

#[tokio::test]
async fn status_completed_after_flow_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 5)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);
    assert!(status.outputs.is_some());
    assert!(status.error.is_none());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_includes_step_details() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let ids = [NodeId::new(), NodeId::new()];
    let spec =
        make_spec(vec![delay_node(ids[0], 5), delay_node(ids[1], 5)], vec![edge(ids[0], ids[1])]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.steps.len(), 2);

    let step_ids: Vec<NodeId> = status.steps.iter().map(|s| s.node_id).collect();
    assert!(step_ids.contains(&ids[0]));
    assert!(step_ids.contains(&ids[1]));

    for step in &status.steps {
        assert_eq!(step.phase, StepPhase::Completed);
    }

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_unknown_flow_run_id() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let err = host.run_status(FlowRunId::new()).await.unwrap_err();
    assert!(matches!(err, HostError::FlowRunNotFound(_)));

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_has_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 5)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);
    assert!(status.submitted_at > 0, "submitted_at should be set");
    assert!(
        status.last_updated_at >= status.submitted_at,
        "last_updated_at should be >= submitted_at"
    );
}

// ── T-6: Capability Discovery ───────────────────────────────────────────

#[tokio::test]
async fn list_capabilities_returns_all_connectors() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let caps = host.list_capabilities();
    let names: Vec<&str> = caps.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"delay"));
    assert!(names.contains(&"fs.read"));
    assert!(names.contains(&"fs.write"));

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_existing_connector() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let desc = host.describe("delay");
    assert!(desc.is_some());
    assert_eq!(desc.unwrap().name, "delay");

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_nonexistent_connector() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    assert!(host.describe("nonexistent").is_none());

    host.shutdown().await.unwrap();
}

// ── T-7: Single-Op Invocation ───────────────────────────────────────────

#[tokio::test]
async fn invoke_single_delay() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let output = host.invoke_single("delay", json!({"duration_ms": 10})).await.unwrap();
    assert!(output.is_object());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn invoke_single_unknown_connector() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let err = host.invoke_single("nonexistent", json!({})).await.unwrap_err();
    assert!(matches!(err, HostError::ConnectorNotFound(_)));

    host.shutdown().await.unwrap();
}

// ── T-8: Crash-Resume ───────────────────────────────────────────────────

#[tokio::test]
async fn crash_resume_restores_coordinator_map() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Start host, submit flow, crash_drop
    let flow_run_id = {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        let id = NodeId::new();
        let spec = make_spec(vec![delay_node(id, 5)], vec![]);
        let frid = host.submit_flow(spec).await.unwrap();

        // Wait for flow to complete
        let status = poll_until_terminal(&host, frid).await;
        assert_eq!(status.phase, FlowPhase::Completed);

        // Simulate crash
        host.crash_drop().await;
        frid
    };

    // Phase 2: Restart from same data
    {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        // The coordinator map should be restored — we can query the old flow
        let status = host.run_status(flow_run_id).await.unwrap();
        assert_eq!(status.phase, FlowPhase::Completed);

        host.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn crash_resume_linear_flow() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Start, submit multi-step flow, let it run briefly, then crash
    let flow_run_id = {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        let ids = [NodeId::new(), NodeId::new(), NodeId::new()];
        let spec = make_spec(
            vec![delay_node(ids[0], 5), delay_node(ids[1], 5), delay_node(ids[2], 5)],
            vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        );
        let frid = host.submit_flow(spec).await.unwrap();

        // Give some time for at least one step to start
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Simulate crash
        host.crash_drop().await;
        frid
    };

    // Phase 2: Restart — flow should complete
    {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        let status = poll_until_terminal(&host, flow_run_id).await;
        assert_eq!(status.phase, FlowPhase::Completed);

        host.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn crash_resume_no_duplicate_fs_write() {
    let dir = tempfile::tempdir().unwrap();
    let work_dir = dir.path().join("work");
    std::fs::create_dir_all(&work_dir).unwrap();
    let file_path = work_dir.join("output.txt");

    // Phase 1: Submit flow with fs.write, let it complete, then crash
    let flow_run_id = {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        let ids = [NodeId::new(), NodeId::new()];
        let spec = make_spec(
            vec![
                delay_node(ids[0], 5),
                connector_node(
                    ids[1],
                    "fs.write",
                    json!({
                        "path": file_path.to_str().unwrap(),
                        "content": "written-once"
                    }),
                ),
            ],
            vec![edge(ids[0], ids[1])],
        );
        let frid = host.submit_flow(spec).await.unwrap();

        let status = poll_until_terminal(&host, frid).await;
        assert_eq!(status.phase, FlowPhase::Completed);
        assert!(file_path.exists());

        // Simulate crash
        host.crash_drop().await;
        frid
    };

    // Phase 2: Restart — should NOT re-run the fs.write
    {
        let config = test_config(dir.path());
        let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

        let status = host.run_status(flow_run_id).await.unwrap();
        assert_eq!(status.phase, FlowPhase::Completed);

        // Verify file was written exactly once
        let content = std::fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "written-once");

        host.shutdown().await.unwrap();
    }
}

// ── T-10: Tick Loop Behavior ────────────────────────────────────────────

#[tokio::test]
async fn tick_loop_drives_execution() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 5)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();

    // The tick loop should drive this to completion — we just poll status
    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn tick_loop_resumes_suspended_coordinators() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    // Multi-step flow requires coordinator resume between steps
    let ids = [NodeId::new(), NodeId::new()];
    let spec =
        make_spec(vec![delay_node(ids[0], 5), delay_node(ids[1], 5)], vec![edge(ids[0], ids[1])]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);
    assert_eq!(status.steps.len(), 2);

    host.shutdown().await.unwrap();
}

// ── T-11: Concurrent Flow Execution ─────────────────────────────────────

#[tokio::test]
async fn multiple_flows_execute_concurrently() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    let mut frids = Vec::new();
    for _ in 0..3 {
        let id = NodeId::new();
        let spec = make_spec(vec![delay_node(id, 10)], vec![]);
        frids.push(host.submit_flow(spec).await.unwrap());
    }

    for frid in &frids {
        let status = poll_until_terminal(&host, *frid).await;
        assert_eq!(status.phase, FlowPhase::Completed);
    }

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn coordinator_map_prunes_terminal_flows() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config.clone(), test_registry()).await.unwrap();

    // Submit a flow and let it complete
    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 5)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();

    let status = poll_until_terminal(&host, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    // Wait long enough for the GC tick to fire (100 ticks * 10ms = 1s, give extra margin)
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Shut down and restart — the coordinator map should have been pruned
    host.shutdown().await.unwrap();

    // Restart and verify the pruned map was persisted
    let host2 = EmbeddedHost::start(config, test_registry()).await.unwrap();

    // The terminal flow should no longer be in the coordinator map,
    // so run_status should return FlowRunNotFound
    let err = host2.run_status(frid).await.unwrap_err();
    assert!(
        matches!(err, HostError::FlowRunNotFound(_)),
        "terminal flow should have been pruned from coordinator map"
    );

    host2.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_submit_and_status() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry()).await.unwrap();

    // Submit first flow
    let id1 = NodeId::new();
    let spec1 = make_spec(vec![delay_node(id1, 50)], vec![]);
    let frid1 = host.submit_flow(spec1).await.unwrap();

    // While first is running, submit second and query status of first
    let id2 = NodeId::new();
    let spec2 = make_spec(vec![delay_node(id2, 5)], vec![]);
    let frid2 = host.submit_flow(spec2).await.unwrap();
    let _status = host.run_status(frid1).await.unwrap();

    // Both should complete
    let s1 = poll_until_terminal(&host, frid1).await;
    let s2 = poll_until_terminal(&host, frid2).await;
    assert_eq!(s1.phase, FlowPhase::Completed);
    assert_eq!(s2.phase, FlowPhase::Completed);

    host.shutdown().await.unwrap();
}
