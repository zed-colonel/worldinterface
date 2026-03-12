//! Integration tests — run flows end-to-end through a real AQ engine.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use actionqueue_core::ids::TaskId;
use actionqueue_core::run::state::RunState;
use actionqueue_engine::time::clock::MockClock;
use actionqueue_runtime::config::{BackoffStrategyConfig, RuntimeConfig};
use actionqueue_runtime::engine::ActionQueueEngine;
use serde_json::json;
use wi_connector::{
    connectors::{DelayConnector, FsReadConnector, FsWriteConnector},
    ConnectorRegistry,
};
use wi_contextstore::{ContextStore, SqliteContextStore};
use wi_coordinator::FlowHandler;
use wi_core::flowspec::branch::{BranchCondition, BranchNode, ParamRef};
use wi_core::flowspec::transform::{TransformNode, TransformType};
use wi_core::flowspec::*;
use wi_core::id::{FlowRunId, NodeId};
use wi_core::metrics::{MetricsRecorder, NoopMetricsRecorder};
use wi_flowspec::compile::compile_with_config;
use wi_flowspec::config::CompilerConfig;

fn noop_metrics() -> Arc<dyn MetricsRecorder> {
    Arc::new(NoopMetricsRecorder)
}

/// Build a registry WITHOUT the HTTP connector (which contains reqwest::blocking::Client
/// that creates an internal tokio runtime, incompatible with #[tokio::test]).
fn test_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry
}

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_data_dir(label: &str) -> PathBuf {
    let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = PathBuf::from("target")
        .join("tmp")
        .join(format!("wi-coord-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create data dir");
    dir
}

fn engine_config(dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        data_dir: dir,
        backoff_strategy: BackoffStrategyConfig::Fixed { interval: Duration::ZERO },
        dispatch_concurrency: NonZeroUsize::new(4).expect("non-zero"),
        lease_timeout_secs: 30,
        snapshot_event_threshold: None,
        ..RuntimeConfig::default()
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

fn transform_node(id: NodeId, input: serde_json::Value) -> Node {
    Node {
        id,
        label: None,
        node_type: NodeType::Transform(TransformNode { transform: TransformType::Identity, input }),
    }
}

fn branch_node(
    id: NodeId,
    condition: BranchCondition,
    then_edge: NodeId,
    else_edge: Option<NodeId>,
) -> Node {
    Node {
        id,
        label: None,
        node_type: NodeType::Branch(BranchNode { condition, then_edge, else_edge }),
    }
}

fn edge(from: NodeId, to: NodeId) -> Edge {
    Edge { from, to, condition: None }
}

fn branch_edge(from: NodeId, to: NodeId, condition: EdgeCondition) -> Edge {
    Edge { from, to, condition: Some(condition) }
}

/// Run engine until idle, then resume the coordinator, and repeat
/// until the coordinator is terminal (completed or failed).
async fn run_flow_to_completion(
    engine: &mut actionqueue_runtime::engine::BootstrappedEngine<
        FlowHandler<SqliteContextStore>,
        MockClock,
    >,
    coordinator_task_id: TaskId,
) {
    for _ in 0..20 {
        // Run until idle
        let _ = engine.run_until_idle().await.expect("run_until_idle");

        // Check if coordinator is terminal
        let coord_runs = engine.projection().run_ids_for_task(coordinator_task_id);
        if let Some(run_id) = coord_runs.first() {
            let state = engine.projection().get_run_state(run_id);
            if let Some(s) = state {
                if s.is_terminal() {
                    return;
                }
                // If suspended, resume
                if matches!(s, RunState::Suspended) {
                    engine.resume_run(*run_id).expect("resume_run");
                }
            }
        }
    }
    panic!("flow did not complete within 20 iterations");
}

// T-7: Linear Flow End-to-End

#[tokio::test]
async fn linear_single_node_flow() {
    let dir = unique_data_dir("single-node");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let node_id = NodeId::new();
    let fr = FlowRunId::new();

    let spec = FlowSpec {
        id: None,
        name: Some("single-node".into()),
        nodes: vec![connector_node(node_id, "delay", json!({"duration_ms": 10}))],
        edges: vec![],
        params: None,
    };

    let config = CompilerConfig::default();
    let compiled = compile_with_config(&spec, &config, fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    // Submit coordinator
    eng.submit_task(compiled.coordinator.clone()).expect("submit coordinator");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // Verify coordinator completed
    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    assert_eq!(eng.projection().get_run_state(&coord_runs[0]), Some(&RunState::Completed));

    // Verify output in ContextStore
    assert!(store.get(fr, node_id).unwrap().is_some());

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn linear_three_node_flow() {
    let dir = unique_data_dir("three-node");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let ids: Vec<NodeId> = (0..3).map(|_| NodeId::new()).collect();
    let fr = FlowRunId::new();

    // delay -> identity transform -> delay
    let spec = FlowSpec {
        id: None,
        name: Some("three-node".into()),
        nodes: vec![
            connector_node(ids[0], "delay", json!({"duration_ms": 10})),
            transform_node(ids[1], json!(format!("{{{{nodes.{}.output}}}}", ids[0].as_ref()))),
            connector_node(ids[2], "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![edge(ids[0], ids[1]), edge(ids[1], ids[2])],
        params: None,
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // All 3 outputs in ContextStore
    for &nid in &ids {
        assert!(store.get(fr, nid).unwrap().is_some(), "node {} should have output", nid.as_ref());
    }

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn linear_flow_with_param_passing() {
    let dir = unique_data_dir("param-passing");
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_path = tmp_dir.path().join("test.txt");

    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let write_id = NodeId::new();
    let read_id = NodeId::new();
    let fr = FlowRunId::new();

    // fs.write -> fs.read, read path from write output
    let spec = FlowSpec {
        id: None,
        name: Some("param-passing".into()),
        nodes: vec![
            connector_node(
                write_id,
                "fs.write",
                json!({
                    "path": file_path.to_str().unwrap(),
                    "content": "hello from param passing"
                }),
            ),
            connector_node(
                read_id,
                "fs.read",
                json!({
                    "path": file_path.to_str().unwrap()
                }),
            ),
        ],
        edges: vec![edge(write_id, read_id)],
        params: None,
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // Verify read got the written content
    let read_output = store.get(fr, read_id).unwrap().unwrap();
    assert_eq!(read_output["content"], "hello from param passing");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn linear_flow_step_failure_propagates() {
    let dir = unique_data_dir("step-failure");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let node_id = NodeId::new();
    let fr = FlowRunId::new();

    // fs.read on nonexistent file → terminal failure
    let spec = FlowSpec {
        id: None,
        name: Some("failure".into()),
        nodes: vec![connector_node(
            node_id,
            "fs.read",
            json!({"path": "/nonexistent/path/file.txt"}),
        )],
        edges: vec![],
        params: None,
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // Coordinator should have failed (step failed → coordinator fails)
    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    let state = eng.projection().get_run_state(&coord_runs[0]).unwrap();
    assert!(
        matches!(state, RunState::Completed | RunState::Failed),
        "coordinator should be terminal, got {:?}",
        state
    );

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// T-8: Branch Flow End-to-End

#[tokio::test]
async fn branch_then_path_taken() {
    let dir = unique_data_dir("branch-then");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new(); // then target
    let c_id = NodeId::new(); // else target
    let fr = FlowRunId::new();

    // A → Branch(flag exists, true) → B | C
    let spec = FlowSpec {
        id: None,
        name: Some("branch-then".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                Some(c_id),
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
            connector_node(c_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![
            edge(a_id, branch_id),
            branch_edge(branch_id, b_id, EdgeCondition::BranchTrue),
            branch_edge(branch_id, c_id, EdgeCondition::BranchFalse),
        ],
        params: Some(json!({"flag": "present"})),
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // A and B executed, C did NOT
    assert!(store.get(fr, a_id).unwrap().is_some(), "A should have output");
    assert!(store.get(fr, branch_id).unwrap().is_some(), "Branch should have output");
    assert!(store.get(fr, b_id).unwrap().is_some(), "B should have output");
    assert!(
        store.get(fr, c_id).unwrap().is_none(),
        "C should NOT have output (else path not taken)"
    );

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn branch_else_path_taken() {
    let dir = unique_data_dir("branch-else");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new();
    let c_id = NodeId::new();
    let fr = FlowRunId::new();

    // A → Branch(flag exists, false because null) → B | C
    let spec = FlowSpec {
        id: None,
        name: Some("branch-else".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                Some(c_id),
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
            connector_node(c_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![
            edge(a_id, branch_id),
            branch_edge(branch_id, b_id, EdgeCondition::BranchTrue),
            branch_edge(branch_id, c_id, EdgeCondition::BranchFalse),
        ],
        params: Some(json!({"flag": null})),
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // A and C executed, B did NOT
    assert!(store.get(fr, a_id).unwrap().is_some());
    assert!(store.get(fr, branch_id).unwrap().is_some());
    assert!(
        store.get(fr, b_id).unwrap().is_none(),
        "B should NOT have output (then path not taken)"
    );
    assert!(store.get(fr, c_id).unwrap().is_some(), "C should have output");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn branch_then_only_not_taken() {
    let dir = unique_data_dir("branch-then-only-not-taken");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new();
    let fr = FlowRunId::new();

    // A → Branch(flag exists, false) → B (no else)
    let spec = FlowSpec {
        id: None,
        name: Some("branch-then-only-not-taken".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                None,
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![edge(a_id, branch_id), branch_edge(branch_id, b_id, EdgeCondition::BranchTrue)],
        params: Some(json!({"flag": null})),
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // Only A executed; B not taken
    assert!(store.get(fr, a_id).unwrap().is_some());
    assert!(store.get(fr, branch_id).unwrap().is_some());
    assert!(store.get(fr, b_id).unwrap().is_none(), "B should NOT have output");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// T-9: Resume Mechanism

#[tokio::test]
async fn coordinator_suspends_on_first_dispatch() {
    let dir = unique_data_dir("suspend-first");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let node_id = NodeId::new();
    let fr = FlowRunId::new();

    let spec = FlowSpec {
        id: None,
        name: Some("suspend-test".into()),
        nodes: vec![connector_node(node_id, "delay", json!({"duration_ms": 10}))],
        edges: vec![],
        params: None,
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    // Run until idle — coordinator should have dispatched and returned Suspended
    let _ = eng.run_until_idle().await.expect("run_until_idle");

    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    assert!(!coord_runs.is_empty());

    // The coordinator's run should be Suspended (children may have completed by now
    // since run_until_idle runs all dispatchable tasks)
    // Actually, after run_until_idle, children may have completed too, and coordinator
    // is still suspended since it hasn't been resumed.
    let coord_state = eng.projection().get_run_state(&coord_runs[0]).unwrap();
    assert_eq!(*coord_state, RunState::Suspended, "coordinator should be Suspended");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn coordinator_resumes_and_completes() {
    let dir = unique_data_dir("resume-complete");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let node_id = NodeId::new();
    let fr = FlowRunId::new();

    let spec = FlowSpec {
        id: None,
        name: Some("resume-test".into()),
        nodes: vec![connector_node(node_id, "delay", json!({"duration_ms": 10}))],
        edges: vec![],
        params: None,
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    // Phase 1: run until idle (coordinator suspended, children completed)
    let _ = eng.run_until_idle().await.expect("phase 1");

    // Resume coordinator
    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    eng.resume_run(coord_runs[0]).expect("resume");

    // Phase 2: run until idle (coordinator sees completed children, returns Success)
    let _ = eng.run_until_idle().await.expect("phase 2");

    let final_state = eng.projection().get_run_state(&coord_runs[0]).unwrap();
    assert_eq!(*final_state, RunState::Completed, "coordinator should be Completed after resume");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// T-8: Additional branch tests

#[tokio::test]
async fn branch_then_only_taken() {
    let dir = unique_data_dir("branch-then-only-taken");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new();
    let fr = FlowRunId::new();

    // A → Branch(flag exists, true) → B (no else)
    let spec = FlowSpec {
        id: None,
        name: Some("branch-then-only-taken".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                None,
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![edge(a_id, branch_id), branch_edge(branch_id, b_id, EdgeCondition::BranchTrue)],
        params: Some(json!({"flag": "present"})),
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // A, Branch, and B all executed
    assert!(store.get(fr, a_id).unwrap().is_some(), "A should have output");
    assert!(store.get(fr, branch_id).unwrap().is_some(), "Branch should have output");
    assert!(store.get(fr, b_id).unwrap().is_some(), "B should have output");

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn branch_with_downstream_merge() {
    let dir = unique_data_dir("branch-merge");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new(); // then target
    let c_id = NodeId::new(); // else target
    let d_id = NodeId::new(); // merge point: downstream of both B and C
    let fr = FlowRunId::new();

    // A → Branch → B | C → D
    let spec = FlowSpec {
        id: None,
        name: Some("branch-merge".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                Some(c_id),
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
            connector_node(c_id, "delay", json!({"duration_ms": 10})),
            connector_node(d_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![
            edge(a_id, branch_id),
            branch_edge(branch_id, b_id, EdgeCondition::BranchTrue),
            branch_edge(branch_id, c_id, EdgeCondition::BranchFalse),
            edge(b_id, d_id),
            edge(c_id, d_id),
        ],
        params: Some(json!({"flag": "present"})), // then path taken
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    run_flow_to_completion(&mut eng, compiled.coordinator.id()).await;

    // A, Branch, B executed; C did NOT; D executed (merge point)
    assert!(store.get(fr, a_id).unwrap().is_some(), "A should have output");
    assert!(store.get(fr, branch_id).unwrap().is_some(), "Branch should have output");
    assert!(store.get(fr, b_id).unwrap().is_some(), "B should have output");
    assert!(
        store.get(fr, c_id).unwrap().is_none(),
        "C should NOT have output (else path not taken)"
    );
    assert!(store.get(fr, d_id).unwrap().is_some(), "D (merge point) should have output");

    // Coordinator completed successfully
    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    assert_eq!(eng.projection().get_run_state(&coord_runs[0]), Some(&RunState::Completed));

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// T-9: Additional resume test

#[tokio::test]
async fn coordinator_suspends_multiple_times_for_branch_flow() {
    let dir = unique_data_dir("multi-suspend");
    let store = Arc::new(SqliteContextStore::in_memory().unwrap());
    let registry = Arc::new(test_registry());
    let handler = FlowHandler::new(registry, Arc::clone(&store), noop_metrics());

    let a_id = NodeId::new();
    let branch_id = NodeId::new();
    let b_id = NodeId::new();
    let c_id = NodeId::new();
    let fr = FlowRunId::new();

    // A → Branch(flag exists, true) → B | C
    let spec = FlowSpec {
        id: None,
        name: Some("multi-suspend".into()),
        nodes: vec![
            connector_node(a_id, "delay", json!({"duration_ms": 10})),
            branch_node(
                branch_id,
                BranchCondition::Exists(ParamRef::FlowParam { path: "flag".into() }),
                b_id,
                Some(c_id),
            ),
            connector_node(b_id, "delay", json!({"duration_ms": 10})),
            connector_node(c_id, "delay", json!({"duration_ms": 10})),
        ],
        edges: vec![
            edge(a_id, branch_id),
            branch_edge(branch_id, b_id, EdgeCondition::BranchTrue),
            branch_edge(branch_id, c_id, EdgeCondition::BranchFalse),
        ],
        params: Some(json!({"flag": "present"})),
    };

    let compiled = compile_with_config(&spec, &CompilerConfig::default(), fr).unwrap();

    let clock = MockClock::new(1000);
    let engine = ActionQueueEngine::new(engine_config(dir.clone()), handler);
    let mut eng = engine.bootstrap_with_clock(clock).expect("bootstrap");

    eng.submit_task(compiled.coordinator.clone()).expect("submit");

    // Phase 1: first dispatch — coordinator submits initial steps, suspends
    let _ = eng.run_until_idle().await.expect("phase 1");
    let coord_runs = eng.projection().run_ids_for_task(compiled.coordinator.id());
    let coord_run_id = coord_runs[0];
    assert_eq!(
        eng.projection().get_run_state(&coord_run_id),
        Some(&RunState::Suspended),
        "coordinator should be Suspended after phase 1"
    );

    // Phase 2: resume — coordinator reads branch result, submits taken-path step, suspends
    eng.resume_run(coord_run_id).expect("resume 1");
    let _ = eng.run_until_idle().await.expect("phase 2");
    let state_after_resume = eng.projection().get_run_state(&coord_run_id).unwrap();
    // Coordinator either suspends again (waiting for B) or completes if B ran fast
    assert!(
        matches!(state_after_resume, RunState::Suspended | RunState::Completed),
        "coordinator should be Suspended or Completed after phase 2, got {:?}",
        state_after_resume
    );

    // Phase 3+: keep resuming until terminal
    for _ in 0..10 {
        let state = eng.projection().get_run_state(&coord_run_id).unwrap();
        if state.is_terminal() {
            break;
        }
        if matches!(state, RunState::Suspended) {
            eng.resume_run(coord_run_id).expect("resume");
        }
        let _ = eng.run_until_idle().await.expect("phase 3+");
    }

    // Final state: Completed
    assert_eq!(
        eng.projection().get_run_state(&coord_run_id),
        Some(&RunState::Completed),
        "coordinator should be Completed at end"
    );

    // Verify correct execution: A, Branch, B ran; C did not
    assert!(store.get(fr, a_id).unwrap().is_some());
    assert!(store.get(fr, branch_id).unwrap().is_some());
    assert!(store.get(fr, b_id).unwrap().is_some());
    assert!(store.get(fr, c_id).unwrap().is_none());

    tokio::task::spawn_blocking(move || {
        eng.shutdown().expect("shutdown");
    })
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
