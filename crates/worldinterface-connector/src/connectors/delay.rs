//! The `delay` connector — sleeps for a specified duration with cooperative cancellation.

use std::time::{Duration, Instant};

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Sleeps for a specified duration with cooperative cancellation.
pub struct DelayConnector;

impl Connector for DelayConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "delay".into(),
            display_name: "Delay".into(),
            description: "Waits for a specified duration.".into(),
            category: ConnectorCategory::Delay,
            input_schema: Some(json!({
                "type": "object",
                "required": ["duration_ms"],
                "properties": {
                    "duration_ms": { "type": "integer", "minimum": 0 }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "slept_ms": { "type": "integer" }
                }
            })),
            idempotent: true,
            side_effects: false,
            is_read_only: true,
            is_mutating: false,
            is_concurrency_safe: true,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let duration_ms = params.get("duration_ms").and_then(Value::as_u64).ok_or_else(|| {
            ConnectorError::InvalidParams(
                "missing or invalid 'duration_ms' (expected non-negative integer)".into(),
            )
        })?;

        let total = Duration::from_millis(duration_ms);
        let check_interval = Duration::from_millis(100);
        let start = Instant::now();

        while start.elapsed() < total {
            if ctx.cancellation.is_cancelled() {
                return Err(ConnectorError::Cancelled);
            }
            let remaining = total.saturating_sub(start.elapsed());
            std::thread::sleep(std::cmp::min(check_interval, remaining));
        }

        Ok(json!({ "slept_ms": duration_ms }))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde_json::json;
    use uuid::Uuid;
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

    #[test]
    fn delay_returns_slept_ms() {
        let ctx = test_ctx();
        let start = Instant::now();
        let result = DelayConnector.invoke(&ctx, &json!({"duration_ms": 50}));
        let elapsed = start.elapsed();
        let output = result.unwrap();
        assert_eq!(output["slept_ms"], 50);
        assert!(elapsed >= Duration::from_millis(50));
    }

    #[test]
    fn delay_zero_duration() {
        let ctx = test_ctx();
        let result = DelayConnector.invoke(&ctx, &json!({"duration_ms": 0}));
        let output = result.unwrap();
        assert_eq!(output["slept_ms"], 0);
    }

    #[test]
    fn delay_respects_cancellation() {
        let ctx = test_ctx();
        let token = ctx.cancellation.clone();

        // Cancel after 50ms from a separate thread
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            token.cancel();
        });

        let start = Instant::now();
        let result = DelayConnector.invoke(&ctx, &json!({"duration_ms": 5000}));
        let elapsed = start.elapsed();

        assert!(matches!(result, Err(ConnectorError::Cancelled)));
        assert!(elapsed < Duration::from_millis(500));
    }

    #[test]
    fn delay_rejects_missing_param() {
        let ctx = test_ctx();
        let result = DelayConnector.invoke(&ctx, &json!({}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn delay_rejects_negative_param() {
        let ctx = test_ctx();
        let result = DelayConnector.invoke(&ctx, &json!({"duration_ms": -1}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn delay_descriptor() {
        let desc = DelayConnector.describe();
        assert_eq!(desc.name, "delay");
        assert_eq!(desc.category, ConnectorCategory::Delay);
        assert!(desc.idempotent);
        assert!(!desc.side_effects);
        assert!(desc.is_read_only);
        assert!(!desc.is_mutating);
        assert!(desc.is_concurrency_safe);
        assert!(!desc.requires_read_before_write);
        assert!(desc.input_schema.is_some());
        assert!(desc.output_schema.is_some());
    }
}
