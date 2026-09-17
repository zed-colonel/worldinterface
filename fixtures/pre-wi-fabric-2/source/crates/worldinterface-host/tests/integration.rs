//! Integration tests for EmbeddedHost.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use worldinterface_connector::connectors::{
    DelayConnector, FsReadConnector, FsWriteConnector, HttpRequestConnector, SandboxExecConnector,
    ShellExecConnector,
};
use worldinterface_connector::ConnectorRegistry;
use worldinterface_core::flowspec::*;
use worldinterface_core::id::{FlowRunId, NodeId};
use worldinterface_host::{
    EmbeddedHost, FlowPhase, FlowRunStatus, HostConfig, HostError, StepPhase,
};

// ── Helpers ─────────────────────────────────────────────────────────────

/// Build a registry with all built-in connectors including HTTP.
fn test_registry() -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry.register(Arc::new(HttpRequestConnector::new()));
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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_starts_with_empty_data_dir() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    assert!(!dir.path().join("aq").exists());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();
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
        let host = EmbeddedHost::start(config.clone(), test_registry(), None).await.unwrap();
        host.shutdown().await.unwrap();
    }

    // Second start — replays from existing data
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_shutdown_stops_tick_loop() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();
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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(vec![delay_node(id, 10)], vec![]);
    let frid = host.submit_flow(spec).await.unwrap();
    assert!(!frid.as_ref().is_nil());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_connector_returns_descriptor() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    let desc = host.describe_connector("delay").unwrap();
    assert_eq!(desc.name, "delay");
    assert!(desc.is_read_only);

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_connector_unknown_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    assert!(host.describe_connector("unknown.connector").is_none());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn submit_invalid_flow_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let step_map: HashMap<NodeId, &worldinterface_host::StepStatus> =
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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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

    let step_map: HashMap<NodeId, &worldinterface_host::StepStatus> =
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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    let err = host.run_status(FlowRunId::new()).await.unwrap_err();
    assert!(matches!(err, HostError::FlowRunNotFound(_)));

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_has_timestamps() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    let desc = host.describe("delay");
    assert!(desc.is_some());
    assert_eq!(desc.unwrap().name, "delay");

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_nonexistent_connector() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    assert!(host.describe("nonexistent").is_none());

    host.shutdown().await.unwrap();
}

// ── T-7: Single-Op Invocation ───────────────────────────────────────────

#[tokio::test]
async fn invoke_single_delay() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

    let output = host.invoke_single("delay", json!({"duration_ms": 10})).await.unwrap();
    assert!(output.is_object());

    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn invoke_single_unknown_connector() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
        let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config.clone(), test_registry(), None).await.unwrap();

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
    let host2 = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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
    let host = EmbeddedHost::start(config, test_registry(), None).await.unwrap();

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

// ── E2S1-T47: shell.exec invocable via EmbeddedHost ──

#[tokio::test]
async fn shell_exec_invocable_via_host() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());

    // Use a registry that includes ShellExecConnector
    let registry = test_registry();
    registry.register(Arc::new(ShellExecConnector::new()));

    let host = EmbeddedHost::start(config, registry, None).await.unwrap();

    let output = host
        .invoke_single("shell.exec", json!({"command": "echo", "args": ["-n", "from_host"]}))
        .await
        .unwrap();

    assert_eq!(output["stdout"], "from_host");
    assert_eq!(output["exit_code"], 0);

    host.shutdown().await.unwrap();
}

// ── E2S1-T48: FlowSpec with shell.exec node compiles and runs ──

#[tokio::test]
async fn flowspec_with_shell_exec_compiles_and_runs() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());

    let registry = test_registry();
    registry.register(Arc::new(ShellExecConnector::new()));

    let host = EmbeddedHost::start(config, registry, None).await.unwrap();

    let id = NodeId::new();
    let spec = make_spec(
        vec![connector_node(
            id,
            "shell.exec",
            json!({"command": "echo", "args": ["flowspec_test"]}),
        )],
        vec![],
    );

    let frid = host.submit_flow(spec).await.unwrap();
    let status = poll_until_terminal(&host, frid).await;

    assert_eq!(status.phase, FlowPhase::Completed);
    assert_eq!(status.steps.len(), 1);
    assert_eq!(status.steps[0].phase, StepPhase::Completed);

    host.shutdown().await.unwrap();
}

// ── E2S2-T43: sandbox.exec invocable via EmbeddedHost ──

#[tokio::test]
async fn sandbox_exec_invocable_via_host() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());

    // Use a sandbox dir that exists on the host (tempdir, not /sandbox)
    let sandbox_dir = tempfile::tempdir().unwrap();

    // Use a registry that includes both ShellExecConnector and SandboxExecConnector
    let registry = test_registry();
    registry.register(Arc::new(ShellExecConnector::new()));
    registry.register(Arc::new(SandboxExecConnector::with_sandbox_dir(
        sandbox_dir.path().to_str().unwrap(),
    )));

    let host = EmbeddedHost::start(config, registry, None).await.unwrap();

    let output = host
        .invoke_single(
            "sandbox.exec",
            json!({"command": "echo", "args": ["-n", "sandbox_host_test"]}),
        )
        .await
        .unwrap();

    assert_eq!(output["stdout"], "sandbox_host_test");
    assert_eq!(output["exit_code"], 0);

    host.shutdown().await.unwrap();
}

// ══════════════════════════════════════════════════════════════════════════════
// Sprint E2-S4: WASM ConnectorRegistry Integration Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(feature = "wasm-tests")]
mod wasm_host_tests {
    use std::path::PathBuf;

    use super::*;

    fn ref_compiled_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/wasm-connectors/compiled")
    }

    fn wasm_config(dir: &std::path::Path) -> HostConfig {
        HostConfig {
            aq_data_dir: dir.join("aq"),
            context_store_path: dir.join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(ref_compiled_dir()),
            ..Default::default()
        }
    }

    // ── E2S4-T8: EmbeddedHost::start with connectors_dir loads WASM connectors ──

    #[tokio::test]
    async fn host_loads_wasm_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // Should have native connectors + WASM connectors
        let caps = host.list_capabilities();
        let names: Vec<&str> = caps.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"json.validate"), "missing json.validate in {names:?}");
        assert!(names.contains(&"demo.host-functions"), "missing demo.host-functions in {names:?}");

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T9: WASM + native connectors coexist in registry ──

    #[tokio::test]
    async fn wasm_and_native_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        let caps = host.list_capabilities();

        // Natives: delay, fs.read, fs.write, http.request
        assert!(caps.iter().any(|d| d.name == "delay"));
        assert!(caps.iter().any(|d| d.name == "http.request"));
        // WASM:
        assert!(caps.iter().any(|d| d.name == "json.validate"));
        assert!(caps.iter().any(|d| d.name == "demo.host-functions"));

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T10: list_capabilities returns both native and WASM descriptors ──

    #[tokio::test]
    async fn list_capabilities_includes_wasm() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        let caps = host.list_capabilities();

        // At least 4 native + 2 WASM = 6
        assert!(caps.len() >= 6, "expected ≥6 capabilities, got {}", caps.len());

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T11: describe("json.validate") returns WASM connector descriptor ──

    #[tokio::test]
    async fn describe_wasm_connector() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        let desc = host.describe("json.validate");

        assert!(desc.is_some());
        let desc = desc.unwrap();
        assert_eq!(desc.name, "json.validate");

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T12: WASM connector invocable via invoke_single ──

    #[tokio::test]
    async fn wasm_invoke_single() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        let result = host
            .invoke_single(
                "json.validate",
                json!({
                    "document": "hello",
                    "schema": {"type": "string"}
                }),
            )
            .await
            .unwrap();

        assert_eq!(result["valid"], true);

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T13: Empty connectors_dir → 0 WASM connectors, no error ──

    #[tokio::test]
    async fn empty_connectors_dir() {
        let dir = tempfile::tempdir().unwrap();
        let empty_dir = tempfile::tempdir().unwrap();
        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(empty_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        let caps = host.list_capabilities();

        // Only native connectors (4 from test_registry)
        assert_eq!(caps.len(), 4, "expected only native connectors, got {}", caps.len());

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T14: Duplicate name (WASM matches native) → DuplicateConnector ──

    #[tokio::test]
    async fn duplicate_connector_name_error() {
        let dir = tempfile::tempdir().unwrap();
        let connectors_dir = tempfile::tempdir().unwrap();

        // Create a WASM module that has the same name as a native connector
        let manifest = r#"
[connector]
name = "delay"
description = "Fake connector with duplicate name"

[capabilities]
"#;
        // Copy a real WASM module and give it a conflicting manifest
        let ref_dir = ref_compiled_dir();
        std::fs::copy(
            ref_dir.join("json-validate.wasm"),
            connectors_dir.path().join("conflict.wasm"),
        )
        .unwrap();
        std::fs::write(connectors_dir.path().join("conflict.connector.toml"), manifest).unwrap();

        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();

        let result = EmbeddedHost::start(config, registry, None).await;
        match result {
            Err(HostError::DuplicateConnector(ref name)) if name == "delay" => {
                // Expected
            }
            Err(e) => panic!("expected DuplicateConnector(\"delay\"), got: {e:?}"),
            Ok(host) => {
                host.shutdown().await.unwrap();
                panic!("expected DuplicateConnector error, but start succeeded");
            }
        }
    }

    // ── E2S4-T15: Invalid module in connectors_dir → skipped, valid still loads ──

    #[tokio::test]
    async fn invalid_module_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let connectors_dir = tempfile::tempdir().unwrap();

        // Copy valid modules
        let ref_dir = ref_compiled_dir();
        for entry in std::fs::read_dir(&ref_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let name_str = name.to_str().unwrap();
            if name_str.ends_with(".wasm") || name_str.ends_with(".connector.toml") {
                std::fs::copy(entry.path(), connectors_dir.path().join(&name)).unwrap();
            }
        }

        // Add an invalid WASM module (just garbage bytes)
        std::fs::write(connectors_dir.path().join("broken.wasm"), b"not a wasm module").unwrap();
        std::fs::write(
            connectors_dir.path().join("broken.connector.toml"),
            "[connector]\nname = \"test.broken\"\n[capabilities]\n",
        )
        .unwrap();

        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        let caps = host.list_capabilities();

        // json.validate and demo.host-functions should still load
        let names: Vec<&str> = caps.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"json.validate"), "valid module should still load");
        assert!(names.contains(&"demo.host-functions"), "valid module should still load");

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T26: FlowSpec with WASM connector node compiles and executes ──

    #[tokio::test]
    async fn flowspec_with_wasm_node() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        let id = NodeId::new();
        let spec = make_spec(
            vec![connector_node(
                id,
                "json.validate",
                json!({
                    "document": 42,
                    "schema": {"type": "number"}
                }),
            )],
            vec![],
        );

        let frid = host.submit_flow(spec).await.unwrap();
        let status = poll_until_terminal(&host, frid).await;

        assert_eq!(status.phase, FlowPhase::Completed);
        assert_eq!(status.steps.len(), 1);

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T27: FlowSpec with WASM + native nodes → both execute ──

    #[tokio::test]
    async fn flowspec_mixed_wasm_and_native() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        let delay_id = NodeId::new();
        let wasm_id = NodeId::new();
        let spec = make_spec(
            vec![
                delay_node(delay_id, 10),
                connector_node(
                    wasm_id,
                    "json.validate",
                    json!({
                        "document": "test",
                        "schema": {"type": "string"}
                    }),
                ),
            ],
            vec![edge(delay_id, wasm_id)],
        );

        let frid = host.submit_flow(spec).await.unwrap();
        let status = poll_until_terminal(&host, frid).await;

        assert_eq!(status.phase, FlowPhase::Completed);
        assert_eq!(status.steps.len(), 2);

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T28: WASM connector via invoke_single → receipt stored ──

    #[tokio::test]
    async fn wasm_invoke_single_stores_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // invoke_single wraps in an ephemeral FlowSpec, so receipts should
        // be generated through the normal Connector::invoke path
        let result = host
            .invoke_single("demo.host-functions", json!({"message": "receipt-test"}))
            .await
            .unwrap();

        assert_eq!(result["message"], "receipt-test");
        assert!(result["sha256"].is_string());
        assert_eq!(result["stored"], true);

        host.shutdown().await.unwrap();
    }

    // ── E2S4-T30: Host shutdown drops WasmRuntime cleanly (no panic) ──

    #[tokio::test]
    async fn host_shutdown_drops_wasm_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        // Just verify shutdown completes without panic
        host.shutdown().await.unwrap();
    }

    // ── E4S3-T21: EmbeddedHost accepts stream handler ──

    #[tokio::test]
    async fn embedded_host_accepts_stream_handler() {
        use worldinterface_core::streaming::{StreamMessage, StreamMessageHandler};

        struct NoopHandler;
        impl StreamMessageHandler for NoopHandler {
            fn handle_messages(
                &self,
                _connector_name: &str,
                _messages: Vec<StreamMessage>,
            ) -> Result<(), String> {
                Ok(())
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();
        let handler: Arc<dyn StreamMessageHandler> = Arc::new(NoopHandler);

        // start() with Some(handler) should succeed even when no streaming
        // modules are loaded (the handler is simply not used)
        let host = EmbeddedHost::start(config, registry, Some(handler)).await.unwrap();
        host.shutdown().await.unwrap();
    }

    // ── E4S3-T22: EmbeddedHost None handler works (standalone WI mode) ──

    #[tokio::test]
    async fn embedded_host_none_handler_ok() {
        let dir = tempfile::tempdir().unwrap();
        let config = wasm_config(dir.path());
        let registry = test_registry();

        let host = EmbeddedHost::start(config, registry, None).await.unwrap();
        host.shutdown().await.unwrap();
    }

    // ── E5S3-T6: host_load_connector ──

    fn test_compiled_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../worldinterface-wasm/test-modules/compiled")
    }

    #[tokio::test]
    async fn host_load_connector() {
        let dir = tempfile::tempdir().unwrap();
        let empty_connectors_dir = tempfile::tempdir().unwrap();
        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(empty_connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();
        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // test.echo should not be present yet
        assert!(host.describe("test.echo").is_none());

        let compiled = test_compiled_dir();
        let manifest_path = compiled.join("echo.connector.toml");
        let wasm_path = compiled.join("echo.wasm");

        let descriptor = host.load_connector(&manifest_path, &wasm_path).await.unwrap();
        assert_eq!(descriptor.name, "test.echo");

        // Now it should appear in list_capabilities
        let caps = host.list_capabilities();
        let names: Vec<&str> = caps.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"test.echo"), "test.echo should be in capabilities: {names:?}");

        host.shutdown().await.unwrap();
    }

    // ── E5S3-T7: host_load_connector_invalid_manifest ──

    #[tokio::test]
    async fn host_load_connector_invalid_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let empty_connectors_dir = tempfile::tempdir().unwrap();
        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(empty_connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();
        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // Nonexistent manifest path should fail
        let nonexistent = dir.path().join("nonexistent.connector.toml");
        let wasm_path = dir.path().join("nonexistent.wasm");

        let result = host.load_connector(&nonexistent, &wasm_path).await;
        assert!(result.is_err(), "load_connector with nonexistent manifest should fail");

        host.shutdown().await.unwrap();
    }

    // ── E5S3-T8: host_unload_connector ──

    #[tokio::test]
    async fn host_unload_connector() {
        let dir = tempfile::tempdir().unwrap();
        let empty_connectors_dir = tempfile::tempdir().unwrap();
        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(empty_connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();
        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // Load the echo connector
        let compiled = test_compiled_dir();
        let manifest_path = compiled.join("echo.connector.toml");
        let wasm_path = compiled.join("echo.wasm");
        host.load_connector(&manifest_path, &wasm_path).await.unwrap();
        assert!(host.describe("test.echo").is_some());

        // Unload it
        host.unload_connector("test.echo").unwrap();
        assert!(host.describe("test.echo").is_none());

        // Unloading again should return ConnectorNotFound
        let err = host.unload_connector("test.echo").unwrap_err();
        assert!(
            matches!(err, HostError::ConnectorNotFound(_)),
            "expected ConnectorNotFound, got: {err:?}"
        );

        host.shutdown().await.unwrap();
    }

    // ── E5S3-T9: host_rescan_connectors ──

    #[tokio::test]
    async fn host_rescan_connectors() {
        let dir = tempfile::tempdir().unwrap();
        let connectors_dir = tempfile::tempdir().unwrap();
        let config = HostConfig {
            aq_data_dir: dir.path().join("aq"),
            context_store_path: dir.path().join("context.db"),
            tick_interval: Duration::from_millis(10),
            connectors_dir: Some(connectors_dir.path().to_path_buf()),
            ..Default::default()
        };
        let registry = test_registry();
        let host = EmbeddedHost::start(config, registry, None).await.unwrap();

        // Initially no WASM connectors
        let caps_before = host.list_capabilities();
        assert!(
            !caps_before.iter().any(|d| d.name == "test.echo"),
            "test.echo should not be loaded yet"
        );

        // Copy echo module into connectors_dir
        let compiled = test_compiled_dir();
        std::fs::copy(
            compiled.join("echo.connector.toml"),
            connectors_dir.path().join("echo.connector.toml"),
        )
        .unwrap();
        std::fs::copy(compiled.join("echo.wasm"), connectors_dir.path().join("echo.wasm")).unwrap();

        // Rescan should pick up the new module
        let newly_loaded = host.rescan_connectors().await.unwrap();
        assert_eq!(newly_loaded.len(), 1);
        assert_eq!(newly_loaded[0].name, "test.echo");

        // Verify it's now in capabilities
        let caps_after = host.list_capabilities();
        assert!(
            caps_after.iter().any(|d| d.name == "test.echo"),
            "test.echo should be loaded after rescan"
        );

        // Rescan again should find nothing new
        let second_rescan = host.rescan_connectors().await.unwrap();
        assert!(second_rescan.is_empty(), "second rescan should find nothing new");

        host.shutdown().await.unwrap();
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// Sprint E5-S3: Directory Watcher Tests
// ══════════════════════════════════════════════════════════════════════════════

#[cfg(feature = "watcher")]
mod watcher_tests {
    use std::path::PathBuf;
    use std::time::Duration;

    // ── E5S3-T21: directory_watcher_detects_new_module ──
    //
    // Tests the watcher using notify directly (same approach as the production
    // code) but with a synchronous channel to avoid spawn_blocking timing issues
    // in CI environments.
    //
    // Accepts both Create and Modify events — on Linux (inotify),
    // std::fs::write() to a new file may emit Modify rather than Create
    // depending on filesystem and timing.

    #[test]
    fn directory_watcher_detects_new_module() {
        use notify::{Event, EventKind, RecursiveMode, Watcher};

        let watch_dir = tempfile::tempdir().unwrap();
        let watch_path = watch_dir.path().to_path_buf();

        let (tx, rx) = std::sync::mpsc::channel::<(PathBuf, PathBuf)>();
        let dir_clone = watch_path.clone();

        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let _watcher = {
            let mut w = notify::recommended_watcher(move |res: Result<Event, _>| {
                if let Ok(event) = res {
                    let _ = notify_tx.send(event);
                }
            })
            .expect("failed to create filesystem watcher");
            w.watch(&watch_path, RecursiveMode::NonRecursive).expect("failed to watch directory");
            w
        };

        // Spawn a thread to process notify events (mirrors production watcher logic)
        let processor = std::thread::spawn(move || {
            while let Ok(event) = notify_rx.recv_timeout(Duration::from_secs(10)) {
                if matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_)) {
                    for path in &event.paths {
                        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        if name.ends_with(".connector.toml") {
                            if let Some(stem) = name.strip_suffix(".connector.toml") {
                                let wasm_path = dir_clone.join(format!("{stem}.wasm"));
                                if wasm_path.exists() {
                                    let _ = tx.send((path.clone(), wasm_path));
                                    return; // Done — exit after first detection
                                }
                            }
                        }
                    }
                }
            }
        });

        // Give the watcher time to register with the OS
        std::thread::sleep(Duration::from_millis(200));

        // Write .wasm file first
        let test_compiled = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../worldinterface-wasm/test-modules/compiled");
        let wasm_bytes = std::fs::read(test_compiled.join("echo.wasm")).unwrap();
        std::fs::write(watch_dir.path().join("echo.wasm"), &wasm_bytes).unwrap();

        // Small delay
        std::thread::sleep(Duration::from_millis(100));

        // Write .connector.toml — this should trigger detection
        let manifest_bytes = std::fs::read(test_compiled.join("echo.connector.toml")).unwrap();
        std::fs::write(watch_dir.path().join("echo.connector.toml"), &manifest_bytes).unwrap();

        // Wait for the processor to detect and send
        let result = rx.recv_timeout(Duration::from_secs(5));
        match result {
            Ok((manifest_path, wasm_path)) => {
                assert!(
                    manifest_path.to_str().unwrap().ends_with("echo.connector.toml"),
                    "manifest path should end with echo.connector.toml, got: {manifest_path:?}"
                );
                assert!(
                    wasm_path.to_str().unwrap().ends_with("echo.wasm"),
                    "wasm path should end with echo.wasm, got: {wasm_path:?}"
                );
            }
            Err(_) => panic!("timed out waiting for watcher event"),
        }

        processor.join().ok();
    }
}
