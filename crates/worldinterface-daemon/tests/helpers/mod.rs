//! Shared test helpers for daemon integration and acceptance tests.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde_json::{json, Value};
use worldinterface_connector::connectors::{DelayConnector, FsReadConnector, FsWriteConnector};
use worldinterface_connector::ConnectorRegistry;
use worldinterface_core::flowspec::{
    BranchCondition, BranchNode, ConnectorNode, Edge, EdgeCondition, FlowSpec, Node, NodeType,
    TransformNode, TransformType,
};
use worldinterface_core::id::{FlowRunId, NodeId};
use worldinterface_daemon::metrics::PrometheusMetricsRecorder;
use worldinterface_daemon::{AppState, DaemonConfig, SharedState, WiMetricsRegistry};
use worldinterface_host::{EmbeddedHost, FlowPhase, FlowRunStatus};

/// Build a registry WITHOUT the HTTP connector (reqwest::blocking::Client
/// creates an internal tokio runtime, incompatible with #[tokio::test]).
pub fn test_registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));
    registry
}

/// Handle to a running test daemon.
pub struct TestDaemon {
    pub addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    /// Owns the temp dir to keep it alive until the daemon shuts down.
    _dir: Option<tempfile::TempDir>,
}

impl TestDaemon {
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Attach a TempDir to this daemon so it stays alive until shutdown.
    pub fn with_dir(mut self, dir: tempfile::TempDir) -> Self {
        self._dir = Some(dir);
        self
    }

    /// Take ownership of the TempDir (for reuse across restarts).
    pub fn take_dir(&mut self) -> tempfile::TempDir {
        self._dir.take().expect("no TempDir to take")
    }

    /// Shut down the test daemon and wait for cleanup to complete.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
    }

    /// Simulate a crash: abort the server task immediately without
    /// graceful shutdown. This is more aggressive than `shutdown()` and
    /// simulates an unexpected process termination.
    #[allow(dead_code)]
    pub async fn crash(mut self) {
        // Drop the shutdown sender (won't trigger graceful shutdown)
        self.shutdown_tx.take();
        // Abort the server task
        self.handle.abort();
        let _ = self.handle.await;
        // Brief pause for OS to release resources
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Start a test daemon on a random port.
pub async fn start_test_daemon() -> TestDaemon {
    let dir = tempfile::tempdir().unwrap();
    start_test_daemon_in(dir).await
}

/// Start a test daemon using the given data directory.
///
/// The caller owns the `TempDir` lifetime, which allows starting multiple
/// daemons against the same directory for restart tests.
pub async fn start_test_daemon_in(dir: tempfile::TempDir) -> TestDaemon {
    start_test_daemon_at_path(dir.path()).await.with_dir(dir)
}

/// Start a test daemon at the given path (no TempDir ownership).
pub async fn start_test_daemon_at_path(path: &std::path::Path) -> TestDaemon {
    let config = DaemonConfig {
        bind_address: "127.0.0.1:0".to_string(),
        aq_data_dir: path.join("aq"),
        context_store_path: path.join("context.db"),
        tick_interval_ms: 20,
        ..Default::default()
    };

    let metrics = Arc::new(WiMetricsRegistry::new().unwrap());
    let mut host_config = config.to_host_config();
    host_config.metrics = Arc::new(PrometheusMetricsRecorder::new(Arc::clone(&metrics)));
    let registry = test_registry();
    let host = EmbeddedHost::start(host_config, registry).await.unwrap();
    let webhook_registry =
        worldinterface_http_trigger::WebhookRegistry::load_from_store(host.context_store())
            .unwrap();
    let state: SharedState =
        Arc::new(AppState { host, webhook_registry: RwLock::new(webhook_registry), metrics });
    let router = worldinterface_daemon::router::build_router(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                shutdown_rx.await.ok();
            })
            .await
            .unwrap();
        if let Ok(app_state) = Arc::try_unwrap(state) {
            let _ = app_state.host.shutdown().await;
        }
    });

    // Give the server a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(50)).await;

    TestDaemon { addr, shutdown_tx: Some(shutdown_tx), handle, _dir: None }
}

/// Helper to poll until a flow reaches a terminal state.
pub async fn poll_until_terminal(
    client: &reqwest::Client,
    base: &str,
    flow_run_id: FlowRunId,
) -> FlowRunStatus {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(15) {
            panic!("timed out waiting for flow {flow_run_id} to complete");
        }
        let resp =
            client.get(format!("{}/api/v1/runs/{}", base, flow_run_id)).send().await.unwrap();
        let status: FlowRunStatus = resp.json().await.unwrap();
        match status.phase {
            FlowPhase::Completed | FlowPhase::Failed | FlowPhase::Canceled => return status,
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

// ---------- Flow Builder Helpers ----------

pub fn single_delay_spec() -> FlowSpec {
    let node_id = NodeId::new();
    FlowSpec {
        id: None,
        name: Some("test-delay".to_string()),
        nodes: vec![Node {
            id: node_id,
            label: Some("delay 10ms".to_string()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "delay".to_string(),
                params: json!({ "duration_ms": 10 }),
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    }
}

#[allow(dead_code)]
pub fn two_step_delay_spec() -> FlowSpec {
    let a = NodeId::new();
    let b = NodeId::new();
    FlowSpec {
        id: None,
        name: Some("test-two-step".to_string()),
        nodes: vec![
            Node {
                id: a,
                label: Some("delay a".to_string()),
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".to_string(),
                    params: json!({ "duration_ms": 10 }),
                    idempotency_config: None,
                }),
            },
            Node {
                id: b,
                label: Some("delay b".to_string()),
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".to_string(),
                    params: json!({ "duration_ms": 10 }),
                    idempotency_config: None,
                }),
            },
        ],
        edges: vec![Edge { from: a, to: b, condition: None }],
        params: None,
    }
}

pub fn delay_node(id: NodeId, label: &str, ms: u64) -> Node {
    Node {
        id,
        label: Some(label.to_string()),
        node_type: NodeType::Connector(ConnectorNode {
            connector: "delay".to_string(),
            params: json!({ "duration_ms": ms }),
            idempotency_config: None,
        }),
    }
}

pub fn connector_node(id: NodeId, label: &str, connector: &str, params: Value) -> Node {
    Node {
        id,
        label: Some(label.to_string()),
        node_type: NodeType::Connector(ConnectorNode {
            connector: connector.to_string(),
            params,
            idempotency_config: None,
        }),
    }
}

pub fn identity_node(id: NodeId, label: &str, input: Value) -> Node {
    Node {
        id,
        label: Some(label.to_string()),
        node_type: NodeType::Transform(TransformNode { transform: TransformType::Identity, input }),
    }
}

pub fn branch_node(
    id: NodeId,
    label: &str,
    condition: BranchCondition,
    then_edge: NodeId,
    else_edge: Option<NodeId>,
) -> Node {
    Node {
        id,
        label: Some(label.to_string()),
        node_type: NodeType::Branch(BranchNode { condition, then_edge, else_edge }),
    }
}

pub fn edge(from: NodeId, to: NodeId) -> Edge {
    Edge { from, to, condition: None }
}

pub fn conditional_edge(from: NodeId, to: NodeId, condition: EdgeCondition) -> Edge {
    Edge { from, to, condition: Some(condition) }
}

pub fn make_spec(name: &str, nodes: Vec<Node>, edges: Vec<Edge>) -> FlowSpec {
    FlowSpec { id: None, name: Some(name.to_string()), nodes, edges, params: None }
}

/// Helper: register a webhook and return the response body.
#[allow(dead_code)]
pub async fn register_test_webhook(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    description: Option<&str>,
) -> Value {
    let resp = client
        .post(format!("{}/api/v1/webhooks", base))
        .json(&json!({
            "path": path,
            "flow_spec": single_delay_spec(),
            "description": description,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "register webhook returned {}", resp.status());
    resp.json().await.unwrap()
}
