//! The `signal.emit` connector — delivers a payload to a waiting signal.await connector.

use std::sync::Arc;

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::signal_registry::SignalRegistry;
use crate::traits::Connector;

/// Delivers a payload to a waiting signal.await connector.
pub struct SignalEmitConnector {
    registry: Arc<SignalRegistry>,
}

impl SignalEmitConnector {
    pub fn new(registry: Arc<SignalRegistry>) -> Self {
        Self { registry }
    }
}

impl Connector for SignalEmitConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "signal.emit".into(),
            display_name: "Signal Emit".into(),
            description: "Delivers a payload to a waiting signal.await connector. Returns whether \
                          a waiter was found."
                .into(),
            category: ConnectorCategory::Custom("signal".into()),
            input_schema: Some(json!({
                "type": "object",
                "required": ["key"],
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Signal key to deliver to"
                    },
                    "payload": {
                        "type": "object",
                        "description": "JSON payload to deliver. Default: {}"
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "delivered": { "type": "boolean" }
                }
            })),
            idempotent: false,
            side_effects: true,
            is_read_only: false,
            is_mutating: false,
            is_concurrency_safe: true,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, _ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let key = params
            .get("key")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::InvalidParams("missing 'key'".into()))?;
        let payload = params.get("payload").cloned().unwrap_or(json!({}));

        let delivered = self.registry.emit(key, payload);
        Ok(json!({ "delivered": delivered }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    // ── T11: signal_emit_descriptor_metadata ──
    #[test]
    fn signal_emit_descriptor_metadata() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalEmitConnector::new(registry);
        let desc = connector.describe();
        assert_eq!(desc.name, "signal.emit");
        assert!(!desc.is_read_only);
        assert!(!desc.is_mutating);
        assert!(desc.side_effects);
        assert_eq!(desc.category, ConnectorCategory::Custom("signal".into()));
    }

    // ── T12: signal_emit_delivers_to_waiter ──
    #[test]
    fn signal_emit_delivers_to_waiter() {
        let registry = Arc::new(SignalRegistry::new());
        let rx = registry.register_waiter("test-key");

        let connector = SignalEmitConnector::new(Arc::clone(&registry));
        let ctx = test_ctx();
        let payload = json!({"answer": "yes"});

        let result =
            connector.invoke(&ctx, &json!({"key": "test-key", "payload": payload})).unwrap();
        assert_eq!(result["delivered"], true);

        let received = rx.recv().unwrap();
        assert_eq!(received, payload);
    }

    // ── T13: signal_emit_no_waiter_returns_false ──
    #[test]
    fn signal_emit_no_waiter_returns_false() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalEmitConnector::new(registry);
        let ctx = test_ctx();

        let result = connector.invoke(&ctx, &json!({"key": "no-waiter"})).unwrap();
        assert_eq!(result["delivered"], false);
    }

    // ── T14: signal_emit_missing_key_invalid_params ──
    #[test]
    fn signal_emit_missing_key_invalid_params() {
        let registry = Arc::new(SignalRegistry::new());
        let connector = SignalEmitConnector::new(registry);
        let ctx = test_ctx();

        let err = connector.invoke(&ctx, &json!({})).unwrap_err();
        assert!(matches!(err, ConnectorError::InvalidParams(_)));
    }
}
