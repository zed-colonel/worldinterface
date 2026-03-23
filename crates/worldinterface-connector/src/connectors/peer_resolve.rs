//! The `peer.resolve` connector — resolves a vessel name to its inbox URL.
//!
//! Queries Observatory's fleet registry to resolve a vessel name to its inbox
//! URL and vessel ID. Uses async `reqwest::Client` with `Handle::current().block_on()`
//! to bridge the sync `Connector::invoke()` trait to async HTTP calls.

use reqwest::Client;
use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Resolves a vessel name to its inbox URL via Observatory's fleet registry.
#[derive(Clone)]
pub struct PeerResolveConnector {
    observatory_url: String,
    auth_token: Option<String>,
    client: Client,
}

impl PeerResolveConnector {
    pub fn new(observatory_url: String, auth_token: Option<String>) -> Self {
        Self {
            observatory_url,
            auth_token,
            client: Client::builder().build().expect("failed to build HTTP client"),
        }
    }

    /// Async implementation of the peer resolve logic.
    async fn invoke_async(
        &self,
        ctx: &InvocationContext,
        params: &Value,
    ) -> Result<Value, ConnectorError> {
        // Check cancellation before doing any work
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        // Extract required "name" parameter
        let name = params.get("name").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'name' (expected string)".into())
        })?;

        // Build request URL
        let url = format!("{}/api/v1/fleet/vessels/{}/inbox-url", self.observatory_url, name);

        // Build request with optional auth header
        let mut request = self.client.get(&url);
        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }

        // Send request
        let response = request
            .send()
            .await
            .map_err(|e| ConnectorError::Retryable(format!("peer resolve request failed: {e}")))?;

        // Handle non-2xx responses
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(ConnectorError::Terminal(format!("vessel '{}' not found", name)));
        }
        if !status.is_success() {
            return Err(ConnectorError::Terminal(format!(
                "fleet registry returned HTTP {}",
                status.as_u16()
            )));
        }

        // Parse response body as JSON
        let body: Value = response.json().await.map_err(|e| {
            ConnectorError::Terminal(format!("failed to parse fleet registry response: {e}"))
        })?;

        // Check cancellation after request completes
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        Ok(body)
    }
}

impl Connector for PeerResolveConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "peer.resolve".into(),
            display_name: "Peer Resolve".into(),
            description: "Resolves a vessel name to its inbox URL via the fleet registry.".into(),
            category: ConnectorCategory::Http,
            input_schema: Some(json!({
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": { "type": "string", "description": "Vessel name to resolve" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "inbox_url": { "type": "string" },
                    "vessel_id": { "type": "string" }
                }
            })),
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        tokio::runtime::Handle::current().block_on(self.invoke_async(ctx, params))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};
    use worldinterface_core::receipt::ReceiptStatus;

    use super::*;
    use crate::context::{CancellationToken, InvocationContext};
    use crate::receipt_gen::invoke_with_receipt;

    fn test_ctx() -> InvocationContext {
        InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: StepRunId::new(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        }
    }

    async fn invoke_on_blocking_thread(
        connector: &PeerResolveConnector,
        params: Value,
    ) -> Result<Value, ConnectorError> {
        let ctx = test_ctx();
        let connector = connector.clone();
        tokio::task::spawn_blocking(move || connector.invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    // ── E4S1-T1: peer_resolve_descriptor ──

    #[test]
    fn peer_resolve_descriptor() {
        let connector = PeerResolveConnector::new("http://localhost:9090".into(), None);
        let desc = connector.describe();
        assert_eq!(desc.name, "peer.resolve");
        assert_eq!(desc.category, ConnectorCategory::Http);
        assert!(desc.idempotent);
        assert!(!desc.side_effects);
        assert!(desc.input_schema.is_some());
        assert!(desc.output_schema.is_some());
    }

    // ── E4S1-T2: peer_resolve_success ──

    #[tokio::test]
    async fn peer_resolve_success() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/fleet/vessels/atlas/inbox-url")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "name": "atlas",
                    "inbox_url": "http://atlas:7600/api/v1/inbox",
                    "vessel_id": "550e8400-e29b-41d4-a716-446655440000"
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = PeerResolveConnector::new(server.url(), None);
        let result = invoke_on_blocking_thread(&connector, json!({"name": "atlas"})).await.unwrap();

        assert_eq!(result["name"], "atlas");
        assert_eq!(result["inbox_url"], "http://atlas:7600/api/v1/inbox");
        assert_eq!(result["vessel_id"], "550e8400-e29b-41d4-a716-446655440000");
        mock.assert_async().await;
    }

    // ── E4S1-T3: peer_resolve_not_found ──

    #[tokio::test]
    async fn peer_resolve_not_found() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/api/v1/fleet/vessels/unknown/inbox-url")
            .with_status(404)
            .with_body("not found")
            .create_async()
            .await;

        let connector = PeerResolveConnector::new(server.url(), None);
        let result = invoke_on_blocking_thread(&connector, json!({"name": "unknown"})).await;

        match result {
            Err(ConnectorError::Terminal(msg)) => {
                assert!(msg.contains("not found"), "expected 'not found' in: {msg}");
            }
            other => panic!("expected Terminal error, got: {other:?}"),
        }
        mock.assert_async().await;
    }

    // ── E4S1-T4: peer_resolve_network_error ──

    #[tokio::test]
    async fn peer_resolve_network_error() {
        let connector = PeerResolveConnector::new("http://127.0.0.1:1".into(), None);
        let result = invoke_on_blocking_thread(&connector, json!({"name": "atlas"})).await;

        assert!(
            matches!(result, Err(ConnectorError::Retryable(_))),
            "expected Retryable error, got: {result:?}"
        );
    }

    // ── E4S1-T5: peer_resolve_missing_name ──

    #[tokio::test]
    async fn peer_resolve_missing_name() {
        let connector = PeerResolveConnector::new("http://localhost:9090".into(), None);
        let result = invoke_on_blocking_thread(&connector, json!({})).await;

        assert!(
            matches!(result, Err(ConnectorError::InvalidParams(_))),
            "expected InvalidParams error, got: {result:?}"
        );
    }

    // ── E4S1-T6: peer_resolve_receipt_generated ──

    #[tokio::test]
    async fn peer_resolve_receipt_generated() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("GET", "/api/v1/fleet/vessels/atlas/inbox-url")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                json!({
                    "name": "atlas",
                    "inbox_url": "http://atlas:7600/api/v1/inbox",
                    "vessel_id": "550e8400-e29b-41d4-a716-446655440000"
                })
                .to_string(),
            )
            .create_async()
            .await;

        let connector = PeerResolveConnector::new(server.url(), None);
        let ctx = test_ctx();
        let params = json!({"name": "atlas"});

        let (result, receipt) =
            tokio::task::spawn_blocking(move || invoke_with_receipt(&connector, &ctx, &params))
                .await
                .expect("spawn_blocking join failed");

        assert!(result.is_ok());
        assert_eq!(receipt.connector, "peer.resolve");
        assert_eq!(receipt.status, ReceiptStatus::Success);
        assert!(!receipt.input_hash.is_empty());
        assert!(receipt.output_hash.is_some());
    }
}
