//! Agent Embedding Example
//!
//! Demonstrates the full EmbeddedHost API:
//! 1. Create a ConnectorRegistry with built-in connectors
//! 2. Configure and start an EmbeddedHost
//! 3. Discover capabilities: list_capabilities(), describe()
//! 4. Single-op invocation: invoke_single("delay", ...)
//! 5. Submit a multi-step flow and poll to completion
//! 6. Inspect flow results via run_status()
//! 7. Graceful shutdown

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use worldinterface_connector::connectors::{DelayConnector, FsReadConnector, FsWriteConnector};
use worldinterface_connector::ConnectorRegistry;
use worldinterface_core::flowspec::*;
use worldinterface_core::id::NodeId;
use worldinterface_host::{EmbeddedHost, FlowPhase, HostConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Step 1: Build connector registry ---
    println!("=== Step 1: Building connector registry ===");
    let mut registry = ConnectorRegistry::new();
    registry.register(Arc::new(DelayConnector));
    registry.register(Arc::new(FsReadConnector));
    registry.register(Arc::new(FsWriteConnector));

    // --- Step 2: Configure and start host ---
    println!("=== Step 2: Starting EmbeddedHost ===");
    let dir = tempfile::tempdir()?;
    let config = HostConfig {
        aq_data_dir: dir.path().join("aq"),
        context_store_path: dir.path().join("context.db"),
        tick_interval: Duration::from_millis(20),
        ..Default::default()
    };

    let host = EmbeddedHost::start(config, registry).await?;
    println!("Host started successfully.");

    // --- Step 3: Discover capabilities ---
    println!("\n=== Step 3: Discovering capabilities ===");
    let capabilities = host.list_capabilities();
    println!("Available connectors ({}):", capabilities.len());
    for cap in &capabilities {
        println!("  - {} (idempotent: {})", cap.name, cap.idempotent);
    }

    let desc = host.describe("delay").expect("delay connector should exist");
    println!("\nDescribe 'delay': {}", desc.name);

    // --- Step 4: Single-op invocation ---
    println!("\n=== Step 4: Single-op invocation ===");
    let result = host
        .invoke_single("delay", json!({"duration_ms": 25}))
        .await?;
    println!("invoke_single('delay') returned: {}", result);

    // --- Step 5: Submit multi-step flow ---
    println!("\n=== Step 5: Submitting multi-step flow ===");
    let n1 = NodeId::new();
    let n2 = NodeId::new();
    let spec = FlowSpec {
        id: None,
        name: Some("embedding-demo".into()),
        nodes: vec![
            Node {
                id: n1,
                label: Some("short delay".into()),
                node_type: NodeType::Connector(ConnectorNode {
                    connector: "delay".into(),
                    params: json!({"duration_ms": 30}),
                    idempotency_config: None,
                }),
            },
            Node {
                id: n2,
                label: Some("identity transform".into()),
                node_type: NodeType::Transform(TransformNode {
                    transform: TransformType::Identity,
                    input: json!({"message": "hello from agent"}),
                }),
            },
        ],
        edges: vec![Edge {
            from: n1,
            to: n2,
            condition: None,
        }],
        params: None,
    };

    let flow_run_id = host.submit_flow(spec).await?;
    println!("Flow submitted: {flow_run_id}");

    // --- Step 6: Poll to completion and inspect ---
    println!("\n=== Step 6: Polling for completion ===");
    loop {
        let status = host.run_status(flow_run_id).await?;
        match status.phase {
            FlowPhase::Completed => {
                println!("Flow completed!");
                println!("Steps:");
                for step in &status.steps {
                    println!(
                        "  {} -- {:?} -- output: {}",
                        step.label.as_deref().unwrap_or(&step.node_id.to_string()),
                        step.phase,
                        step.output
                            .as_ref()
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".into()),
                    );
                }
                break;
            }
            FlowPhase::Failed => {
                println!("Flow failed: {}", status.error.unwrap_or_default());
                break;
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    // --- Step 7: Graceful shutdown ---
    println!("\n=== Step 7: Shutting down ===");
    host.shutdown().await?;
    println!("Host shut down gracefully.");
    println!("\nAgent embedding example complete.");

    Ok(())
}
