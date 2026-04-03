//! The `signal.await` connector — blocks until a matching signal is emitted or timeout expires.
//!
//! General-purpose coordination primitive for human-in-the-loop,
//! inter-agent communication, and workflow gates. Uses `std::sync::mpsc`
//! with 100ms polling loop for cancellation-aware waiting.

use std::sync::Arc;

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::signal_registry::SignalRegistry;
use crate::traits::Connector;

/// Blocks until a matching signal is emitted or timeout expires.
pub struct SignalAwaitConnector {
    registry: Arc<SignalRegistry>,
}

impl SignalAwaitConnector {
    pub fn new(registry: Arc<SignalRegistry>) -> Self {
        Self { registry }
    }
}

impl Connector for SignalAwaitConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "signal.await".into(),
            display_name: "Signal Await".into(),
            description: "Blocks until a matching signal is emitted or timeout expires. \
                          General-purpose coordination primitive for human-in-the-loop, \
                          inter-agent communication, and workflow gates."
                .into(),
            category: ConnectorCategory::Custom("signal".into()),
            input_schema: Some(json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Signal key to wait for"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Maximum wait time in seconds. Default: 30",
                        "minimum": 1
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string", "enum": ["received", "timeout", "cancelled"] },
                    "payload": { "type": "object" }
                }
            })),
            idempotent: false,
            side_effects: false,
            is_read_only: true,
            is_mutating: false,
            is_concurrency_safe: false,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let key = params
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::InvalidParams("missing 'key'".into()))?;
        let timeout_secs = params.get("timeout_secs").and_then(Value::as_u64).unwrap_or(30);

        let rx = self.registry.register_waiter(key);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

        loop {
            // Check cancellation
            if ctx.cancellation.is_cancelled() {
                self.registry.remove_waiter(key);
                return Ok(json!({ "status": "cancelled" }));
            }
            // Poll for signal
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(payload) => {
                    return Ok(json!({ "status": "received", "payload": payload }));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if std::time::Instant::now() >= deadline {
                        self.registry.remove_waiter(key);
                        return Ok(json!({ "status": "timeout" }));
                    }
                    // Continue polling
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    return Ok(json!({ "status": "cancelled" }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::descriptor::ConnectorCategory;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::CancellationToken;

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

    // ── T5: signal_await_descriptor_metadata ──
    #[test]
    fn signal_await_descriptor_metadata() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(registry);
        let desc = connector.describe();
        assert_eq!(desc.name, "signal.await");
        assert!(desc.is_read_only);
        assert!(!desc.is_mutating);
        assert_eq!(desc.category, ConnectorCategory::Custom("signal".into()));
    }

    // ── T6: signal_await_receives_signal ──
    #[test]
    fn signal_await_receives_signal() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(Arc::clone(&registry));
        let ctx = test_ctx();
        let payload = json!({"answer": "yes"});

        // Spawn a thread to emit the signal after a short delay
        let reg_clone = Arc::clone(&registry);
        let payload_clone = payload.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            reg_clone.emit("test-key", payload_clone);
        });

        let result =
            connector.invoke(&ctx, &json!({"key": "test-key", "timeout_secs": 5})).unwrap();
        assert_eq!(result["status"], "received");
        assert_eq!(result["payload"], payload);
    }

    // ── T7: signal_await_timeout_returns_timeout ──
    #[test]
    fn signal_await_timeout_returns_timeout() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(registry);
        let ctx = test_ctx();

        let start = Instant::now();
        let result = connector.invoke(&ctx, &json!({"key": "no-emit", "timeout_secs": 1})).unwrap();
        assert_eq!(result["status"], "timeout");
        assert!(start.elapsed() >= Duration::from_millis(900));
        assert!(start.elapsed() < Duration::from_secs(3));
    }

    // ── T8: signal_await_cancellation ──
    #[test]
    fn signal_await_cancellation() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(registry);
        let ctx = test_ctx();

        // Cancel after 50ms
        let cancel_token = ctx.cancellation.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancel_token.cancel();
        });

        let result =
            connector.invoke(&ctx, &json!({"key": "cancel-test", "timeout_secs": 30})).unwrap();
        assert_eq!(result["status"], "cancelled");
    }

    // ── T9: signal_await_missing_key_invalid_params ──
    #[test]
    fn signal_await_missing_key_invalid_params() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(registry);
        let ctx = test_ctx();

        let err = connector.invoke(&ctx, &json!({})).unwrap_err();
        assert!(matches!(err, ConnectorError::InvalidParams(_)));
    }

    // ── T10: signal_await_custom_timeout ──
    #[test]
    fn signal_await_custom_timeout() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalAwaitConnector::new(registry);
        let ctx = test_ctx();

        let start = Instant::now();
        let result =
            connector.invoke(&ctx, &json!({"key": "timeout-1s", "timeout_secs": 1})).unwrap();
        let elapsed = start.elapsed();
        assert_eq!(result["status"], "timeout");
        assert!(elapsed >= Duration::from_millis(900));
        assert!(elapsed < Duration::from_secs(3));
    }
}
