//! Integration tests for the daemon HTTP API.
//!
//! Each test starts a real daemon on a random port, makes HTTP requests, and
//! verifies responses.

mod helpers;

use std::time::Duration;

use helpers::*;
use serde_json::{json, Value};
use wi_core::flowspec::{ConnectorNode, FlowSpec, Node, NodeType};
use wi_core::id::{FlowRunId, NodeId};
use wi_daemon::routes::flows::{SubmitFlowRequest, SubmitFlowResponse};
use wi_host::FlowPhase;

// ---------- Health Endpoints ----------

#[tokio::test]
async fn healthz_returns_200() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/healthz", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    daemon.shutdown().await;
}

#[tokio::test]
async fn ready_returns_200() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/ready", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ready");

    daemon.shutdown().await;
}

// ---------- Capability Endpoints ----------

#[tokio::test]
async fn list_capabilities_returns_all() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp =
        client.get(format!("{}/api/v1/capabilities", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let capabilities = body["capabilities"].as_array().unwrap();
    assert!(capabilities.len() >= 3, "should have at least 3 connectors");

    // Check that known connectors are present (test registry excludes http.request)
    let names: Vec<&str> = capabilities.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"delay"), "should contain delay");
    assert!(names.contains(&"fs.read"), "should contain fs.read");
    assert!(names.contains(&"fs.write"), "should contain fs.write");

    daemon.shutdown().await;
}

#[tokio::test]
async fn describe_existing_connector() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/api/v1/capabilities/delay", daemon.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "delay");

    daemon.shutdown().await;
}

#[tokio::test]
async fn describe_nonexistent_connector_returns_404() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp =
        client.get(format!("{}/api/v1/capabilities/nope", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 404);
    let body: Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("not found"));

    daemon.shutdown().await;
}

// ---------- Flow Submission Endpoints ----------

#[tokio::test]
async fn submit_flow_returns_202() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/flows", daemon.base_url()))
        .json(&SubmitFlowRequest { spec: single_delay_spec() })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: SubmitFlowResponse = resp.json().await.unwrap();
    // FlowRunId should be a valid UUID string
    let _: uuid::Uuid = body.flow_run_id.to_string().parse().unwrap();

    daemon.shutdown().await;
}

#[tokio::test]
async fn submit_ephemeral_flow_returns_202() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/flows/ephemeral", daemon.base_url()))
        .json(&single_delay_spec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: SubmitFlowResponse = resp.json().await.unwrap();
    let _: uuid::Uuid = body.flow_run_id.to_string().parse().unwrap();

    daemon.shutdown().await;
}

#[tokio::test]
async fn submit_malformed_json_returns_422() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/flows", daemon.base_url()))
        .header("content-type", "application/json")
        .body(r#"{"not": "a flowspec"}"#)
        .send()
        .await
        .unwrap();
    // axum returns 422 for JSON deserialization failures
    assert!(
        resp.status() == 400 || resp.status() == 422,
        "expected 400 or 422, got {}",
        resp.status()
    );

    daemon.shutdown().await;
}

// ---------- Run Status Endpoints ----------

#[tokio::test]
async fn get_run_not_found() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let fake_id = FlowRunId::new();
    let resp =
        client.get(format!("{}/api/v1/runs/{}", daemon.base_url(), fake_id)).send().await.unwrap();
    assert_eq!(resp.status(), 404);

    daemon.shutdown().await;
}

#[tokio::test]
async fn get_run_invalid_uuid() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp =
        client.get(format!("{}/api/v1/runs/not-a-uuid", daemon.base_url())).send().await.unwrap();
    // Invalid UUID returns 400 from our handler (parse error)
    assert!(
        resp.status() == 400 || resp.status() == 404,
        "expected 400 or 404, got {}",
        resp.status()
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn list_runs_empty() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/api/v1/runs", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["runs"].as_array().unwrap().is_empty());

    daemon.shutdown().await;
}

#[tokio::test]
async fn list_runs_returns_submitted_flows() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    // Submit two flows
    let resp1 = client
        .post(format!("{}/api/v1/flows", daemon.base_url()))
        .json(&SubmitFlowRequest { spec: single_delay_spec() })
        .send()
        .await
        .unwrap();
    let id1: SubmitFlowResponse = resp1.json().await.unwrap();

    let resp2 = client
        .post(format!("{}/api/v1/flows", daemon.base_url()))
        .json(&SubmitFlowRequest { spec: single_delay_spec() })
        .send()
        .await
        .unwrap();
    let id2: SubmitFlowResponse = resp2.json().await.unwrap();

    // List runs
    let resp = client.get(format!("{}/api/v1/runs", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let runs = body["runs"].as_array().unwrap();
    assert_eq!(runs.len(), 2);

    let run_ids: Vec<String> =
        runs.iter().map(|r| r["flow_run_id"].as_str().unwrap().to_string()).collect();
    assert!(run_ids.contains(&id1.flow_run_id.to_string()));
    assert!(run_ids.contains(&id2.flow_run_id.to_string()));

    daemon.shutdown().await;
}

// ---------- Invoke Endpoint ----------

#[tokio::test]
async fn invoke_delay_returns_output() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/invoke/delay", daemon.base_url()))
        .json(&json!({ "duration_ms": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["output"]["slept_ms"].is_number());

    daemon.shutdown().await;
}

#[tokio::test]
async fn invoke_unknown_connector_returns_404() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/invoke/nonexistent", daemon.base_url()))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    daemon.shutdown().await;
}

// ---------- Full Round-Trip ----------

#[tokio::test]
async fn round_trip_submit_and_inspect() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    // Submit a two-step flow
    let resp = client
        .post(format!("{}/api/v1/flows", base))
        .json(&SubmitFlowRequest { spec: two_step_delay_spec() })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let submit: SubmitFlowResponse = resp.json().await.unwrap();

    // Poll until complete
    let status = poll_until_terminal(&client, &base, submit.flow_run_id).await;
    assert_eq!(status.phase, FlowPhase::Completed);
    assert!(status.outputs.is_some());
    assert_eq!(status.steps.len(), 2);

    // Verify timestamps are populated
    assert!(status.submitted_at > 0);
    assert!(status.last_updated_at >= status.submitted_at);

    daemon.shutdown().await;
}

#[tokio::test]
async fn round_trip_with_failure() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    // Submit a flow that reads a nonexistent file
    let node_id = NodeId::new();
    let spec = FlowSpec {
        id: None,
        name: Some("test-fail".to_string()),
        nodes: vec![Node {
            id: node_id,
            label: Some("read nonexistent".to_string()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "fs.read".to_string(),
                params: json!({ "path": "/nonexistent/file/that/does/not/exist" }),
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    };

    let resp = client
        .post(format!("{}/api/v1/flows", base))
        .json(&SubmitFlowRequest { spec })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let submit: SubmitFlowResponse = resp.json().await.unwrap();

    // Poll until terminal
    let status = poll_until_terminal(&client, &base, submit.flow_run_id).await;
    assert_eq!(status.phase, FlowPhase::Failed);
    assert!(status.error.is_some());

    daemon.shutdown().await;
}

// ---------- Webhook CRUD Endpoints (T-8) ----------

#[tokio::test]
async fn register_webhook_returns_201() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    let body = register_test_webhook(&client, &base, "github/push", Some("Push handler")).await;
    assert!(body["webhook_id"].is_string());
    assert_eq!(body["path"], "github/push");
    assert_eq!(body["invoke_url"], "/webhooks/github/push");

    daemon.shutdown().await;
}

#[tokio::test]
async fn register_webhook_invalid_path_returns_400() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/webhooks", daemon.base_url()))
        .json(&json!({
            "path": "",
            "flow_spec": single_delay_spec(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    daemon.shutdown().await;
}

#[tokio::test]
async fn register_webhook_invalid_spec_returns_400() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/api/v1/webhooks", daemon.base_url()))
        .json(&json!({
            "path": "test/hook",
            "flow_spec": {
                "nodes": [],
                "edges": [],
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    daemon.shutdown().await;
}

#[tokio::test]
async fn register_webhook_duplicate_path_returns_409() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    register_test_webhook(&client, &base, "dup/path", None).await;

    let resp = client
        .post(format!("{}/api/v1/webhooks", base))
        .json(&json!({
            "path": "dup/path",
            "flow_spec": single_delay_spec(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    daemon.shutdown().await;
}

#[tokio::test]
async fn list_webhooks_returns_all() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    register_test_webhook(&client, &base, "hook1", None).await;
    register_test_webhook(&client, &base, "hook2", None).await;

    let resp = client.get(format!("{}/api/v1/webhooks", base)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["webhooks"].as_array().unwrap().len(), 2);

    daemon.shutdown().await;
}

#[tokio::test]
async fn list_webhooks_empty() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/api/v1/webhooks", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert!(body["webhooks"].as_array().unwrap().is_empty());

    daemon.shutdown().await;
}

#[tokio::test]
async fn get_webhook_returns_200() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    let reg = register_test_webhook(&client, &base, "get/test", Some("desc")).await;
    let id = reg["webhook_id"].as_str().unwrap();

    let resp = client.get(format!("{}/api/v1/webhooks/{}", base, id)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["path"], "get/test");
    assert_eq!(body["description"], "desc");

    daemon.shutdown().await;
}

#[tokio::test]
async fn get_webhook_not_found_returns_404() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let fake_id = uuid::Uuid::new_v4();
    let resp = client
        .get(format!("{}/api/v1/webhooks/{}", daemon.base_url(), fake_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    daemon.shutdown().await;
}

#[tokio::test]
async fn delete_webhook_returns_204() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    let reg = register_test_webhook(&client, &base, "del/test", None).await;
    let id = reg["webhook_id"].as_str().unwrap();

    let resp = client.delete(format!("{}/api/v1/webhooks/{}", base, id)).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    daemon.shutdown().await;
}

#[tokio::test]
async fn delete_webhook_not_found_returns_404() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let fake_id = uuid::Uuid::new_v4();
    let resp = client
        .delete(format!("{}/api/v1/webhooks/{}", daemon.base_url(), fake_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    daemon.shutdown().await;
}

#[tokio::test]
async fn delete_then_list_excludes_removed() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    let reg1 = register_test_webhook(&client, &base, "keep", None).await;
    let reg2 = register_test_webhook(&client, &base, "remove", None).await;
    let id2 = reg2["webhook_id"].as_str().unwrap();

    // Delete the second webhook
    client.delete(format!("{}/api/v1/webhooks/{}", base, id2)).send().await.unwrap();

    // List should have only the first
    let resp = client.get(format!("{}/api/v1/webhooks", base)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    let webhooks = body["webhooks"].as_array().unwrap();
    assert_eq!(webhooks.len(), 1);
    assert_eq!(webhooks[0]["id"], reg1["webhook_id"]);

    daemon.shutdown().await;
}

// ---------- Webhook Invocation (T-9) ----------

#[tokio::test]
async fn invoke_webhook_returns_202() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    register_test_webhook(&client, &base, "invoke/test", None).await;

    let resp = client
        .post(format!("{}/webhooks/invoke/test", base))
        .json(&json!({"message": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let body: Value = resp.json().await.unwrap();
    assert!(body["flow_run_id"].is_string());
    assert!(body["receipt"].is_object());
    assert_eq!(body["receipt"]["connector"], "webhook.trigger");

    daemon.shutdown().await;
}

#[tokio::test]
async fn invoke_webhook_not_found_returns_404() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/webhooks/nonexistent", daemon.base_url()))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    daemon.shutdown().await;
}

// ---------- Full Round-Trip: Webhook to Completion (T-10) ----------

#[tokio::test]
async fn webhook_round_trip() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    // Register a webhook pointing to a delay flow
    register_test_webhook(&client, &base, "roundtrip", None).await;

    // Invoke the webhook
    let resp = client
        .post(format!("{}/webhooks/roundtrip", base))
        .json(&json!({"test": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let invoke_body: Value = resp.json().await.unwrap();
    let frid_str = invoke_body["flow_run_id"].as_str().unwrap();
    let frid: FlowRunId = frid_str.parse().unwrap();

    // Poll until the flow completes
    let status = poll_until_terminal(&client, &base, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    daemon.shutdown().await;
}

#[tokio::test]
async fn webhook_round_trip_with_trigger_data() {
    let dir = tempfile::tempdir().unwrap();
    let output_path = dir.path().join("trigger-output.txt");
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    // Build a FlowSpec that writes {{trigger.body.message}} to a file.
    // This proves trigger data injection works end-to-end through the daemon.
    let node_id = NodeId::new();
    let trigger_flow = FlowSpec {
        id: None,
        name: Some("trigger-data-test".to_string()),
        nodes: vec![Node {
            id: node_id,
            label: Some("write trigger message".to_string()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "fs.write".to_string(),
                params: json!({
                    "path": output_path.to_str().unwrap(),
                    "content": "{{trigger.body.message}}",
                    "mode": "create"
                }),
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    };

    // Register webhook with the trigger-referencing flow
    let resp = client
        .post(format!("{}/api/v1/webhooks", base))
        .json(&json!({
            "path": "trigger-data",
            "flow_spec": trigger_flow,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Invoke webhook with JSON body containing the message
    let resp = client
        .post(format!("{}/webhooks/trigger-data", base))
        .json(&json!({"message": "hello from webhook"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let invoke_body: Value = resp.json().await.unwrap();
    let frid: FlowRunId = invoke_body["flow_run_id"].as_str().unwrap().parse().unwrap();

    // Poll until the flow completes
    let status = poll_until_terminal(&client, &base, frid).await;
    assert_eq!(
        status.phase,
        FlowPhase::Completed,
        "flow should complete, error: {:?}",
        status.error
    );

    // Verify the file was written with the trigger body message
    let written = std::fs::read_to_string(&output_path).unwrap();
    assert_eq!(written, "hello from webhook");

    daemon.shutdown().await;
}

// ---------- Webhook Persistence Across Restart (T-11) ----------

#[tokio::test]
async fn webhooks_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_path = dir.path().to_path_buf();

    // --- First daemon: register a webhook ---
    let daemon1 = start_test_daemon_at_path(&data_path).await.with_dir(dir);
    let client = reqwest::Client::new();
    let base1 = daemon1.base_url();

    let reg_body =
        register_test_webhook(&client, &base1, "persist/test", Some("survives restart")).await;
    let webhook_id = reg_body["webhook_id"].as_str().unwrap().to_string();

    // Verify it's listed
    let resp = client.get(format!("{}/api/v1/webhooks", base1)).send().await.unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["webhooks"].as_array().unwrap().len(), 1);

    // Shut down the first daemon (graceful, data files preserved)
    let mut daemon1 = daemon1;
    let dir = daemon1.take_dir();
    daemon1.shutdown().await;

    // --- Second daemon: same data directory ---
    let daemon2 = start_test_daemon_at_path(&data_path).await.with_dir(dir);
    let base2 = daemon2.base_url();

    // Verify the webhook was loaded from ContextStore
    let resp = client.get(format!("{}/api/v1/webhooks", base2)).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    let webhooks = body["webhooks"].as_array().unwrap();
    assert_eq!(webhooks.len(), 1, "webhook should survive restart");
    assert_eq!(webhooks[0]["path"], "persist/test");
    assert_eq!(webhooks[0]["description"], "survives restart");
    assert_eq!(webhooks[0]["id"], webhook_id);

    // Verify the webhook is functional after restart — invoke it
    let resp = client
        .post(format!("{}/webhooks/persist/test", base2))
        .json(&json!({"after_restart": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
    let invoke_body: Value = resp.json().await.unwrap();
    let frid: FlowRunId = invoke_body["flow_run_id"].as_str().unwrap().parse().unwrap();

    // Poll until complete
    let status = poll_until_terminal(&client, &base2, frid).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    daemon2.shutdown().await;
}

// ---------- Observability (Sprint 8) ----------

// T-7: Metrics Endpoint

#[tokio::test]
async fn metrics_endpoint_returns_200() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/metrics", daemon.base_url())).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(content_type.contains("text/plain"));

    daemon.shutdown().await;
}

#[tokio::test]
async fn metrics_contain_wi_prefix() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();

    let resp = client.get(format!("{}/metrics", daemon.base_url())).send().await.unwrap();
    let body = resp.text().await.unwrap();
    // The wi_ prefix is applied via Registry::new_custom("wi", ...)
    assert!(body.contains("wi_flow_runs_active"), "missing wi_flow_runs_active");
    assert!(body.contains("wi_contextstore_writes_total"), "missing wi_contextstore_writes_total");
    assert!(body.contains("wi_webhook_errors_total"), "missing wi_webhook_errors_total");

    daemon.shutdown().await;
}

// T-8: Metrics Recording Integration

#[tokio::test]
async fn flow_completion_increments_counter() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    // Submit a simple flow
    let node_id = NodeId::new();
    let spec = FlowSpec {
        id: None,
        name: Some("metrics-test".into()),
        nodes: vec![Node {
            id: node_id,
            label: Some("delay step".into()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    };
    let resp = client
        .post(format!("{}/api/v1/flows", base))
        .json(&SubmitFlowRequest { spec })
        .send()
        .await
        .unwrap();
    let submit_resp: SubmitFlowResponse = resp.json().await.unwrap();

    // Wait for completion
    let status = poll_until_terminal(&client, &base, submit_resp.flow_run_id).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    // Wait for pruning cycle to record metrics (tick interval + gc interval)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Check metrics
    let resp = client.get(format!("{}/metrics", base)).send().await.unwrap();
    let metrics_body = resp.text().await.unwrap();

    // Step metrics should be recorded
    assert!(
        metrics_body.contains("wi_step_runs_total{connector=\"delay\",status=\"success\"} 1"),
        "missing step success counter in:\n{}",
        metrics_body
    );
    assert!(
        metrics_body.contains("wi_connector_invocations_total{connector=\"delay\"} 1"),
        "missing connector invocation counter"
    );

    daemon.shutdown().await;
}

// T-9: Run Inspect Enrichment

#[tokio::test]
async fn run_inspect_includes_receipts_and_connector() {
    let daemon = start_test_daemon().await;
    let client = reqwest::Client::new();
    let base = daemon.base_url();

    let node_id = NodeId::new();
    let spec = FlowSpec {
        id: None,
        name: Some("inspect-test".into()),
        nodes: vec![Node {
            id: node_id,
            label: Some("test delay".into()),
            node_type: NodeType::Connector(ConnectorNode {
                connector: "delay".into(),
                params: json!({"duration_ms": 10}),
                idempotency_config: None,
            }),
        }],
        edges: vec![],
        params: None,
    };
    let resp = client
        .post(format!("{}/api/v1/flows", base))
        .json(&SubmitFlowRequest { spec })
        .send()
        .await
        .unwrap();
    let submit_resp: SubmitFlowResponse = resp.json().await.unwrap();

    let status = poll_until_terminal(&client, &base, submit_resp.flow_run_id).await;
    assert_eq!(status.phase, FlowPhase::Completed);

    // Check enriched fields
    assert!(!status.steps.is_empty());
    let step = &status.steps[0];
    assert_eq!(step.connector.as_deref(), Some("delay"));
    assert_eq!(step.label.as_deref(), Some("test delay"));
    assert!(step.receipt.is_some(), "step should have a receipt");
    let receipt = step.receipt.as_ref().unwrap();
    assert_eq!(receipt.get("connector").and_then(|v| v.as_str()), Some("delay"));
    assert_eq!(receipt.get("status").and_then(|v| v.as_str()), Some("success"));

    daemon.shutdown().await;
}
