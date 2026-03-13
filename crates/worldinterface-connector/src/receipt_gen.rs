//! Receipt generation wrapper for connector invocations.
//!
//! Standardizes receipt creation so that individual connectors don't need to
//! implement it. Every crossing produces a receipt (Invariant 6).

use std::time::Instant;

use chrono::Utc;
use serde_json::Value;
use worldinterface_core::receipt::{sha256_hex, Receipt, ReceiptStatus};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Invoke a connector and produce a Receipt alongside the result.
///
/// This wrapper:
/// 1. Hashes the input params
/// 2. Measures wall-clock invocation time
/// 3. Invokes the connector
/// 4. Hashes the output (on success)
/// 5. Constructs a content-addressed Receipt
///
/// Returns both the connector result and the receipt. The receipt is produced
/// regardless of success or failure (Invariant 6).
pub fn invoke_with_receipt(
    connector: &dyn Connector,
    ctx: &InvocationContext,
    params: &Value,
) -> (Result<Value, ConnectorError>, Receipt) {
    let descriptor = connector.describe();

    let _span = tracing::info_span!(
        "connector_invoke",
        flow_run_id = %ctx.flow_run_id,
        node_id = %ctx.node_id,
        connector = %descriptor.name,
        attempt = ctx.attempt_number,
    )
    .entered();

    let input_hash = sha256_hex(params.to_string().as_bytes());
    let timestamp = Utc::now();

    let start = Instant::now();
    let result = connector.invoke(ctx, params);
    let duration_ms = start.elapsed().as_millis() as u64;

    let (output_hash, status, error) = match &result {
        Ok(output) => {
            let hash = sha256_hex(output.to_string().as_bytes());
            (Some(hash), ReceiptStatus::Success, None)
        }
        Err(ConnectorError::Cancelled) => (None, ReceiptStatus::Timeout, None),
        Err(e) => (None, ReceiptStatus::Failure, Some(e.to_string())),
    };

    let receipt = Receipt::new(
        ctx.flow_run_id,
        ctx.node_id,
        ctx.step_run_id,
        descriptor.name,
        timestamp,
        ctx.attempt_id,
        input_hash,
        output_hash,
        status,
        error,
        duration_ms,
    );

    (result, receipt)
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};
    use worldinterface_core::receipt::ReceiptStatus;

    use super::*;
    use crate::connectors::delay::DelayConnector;
    use crate::context::{CancellationToken, InvocationContext};

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
    fn receipt_generated_on_success() {
        let ctx = test_ctx();
        let (result, receipt) =
            invoke_with_receipt(&DelayConnector, &ctx, &json!({"duration_ms": 10}));

        assert!(result.is_ok());
        assert_eq!(receipt.status, ReceiptStatus::Success);
        assert!(receipt.output_hash.is_some());
        assert!(receipt.error.is_none());
        assert!(!receipt.input_hash.is_empty());
        assert!(receipt.duration_ms >= 10);
    }

    #[test]
    fn receipt_generated_on_failure() {
        let ctx = test_ctx();
        let (result, receipt) = invoke_with_receipt(&DelayConnector, &ctx, &json!({}));

        assert!(result.is_err());
        assert_eq!(receipt.status, ReceiptStatus::Failure);
        assert!(receipt.output_hash.is_none());
        assert!(receipt.error.is_some());
    }

    #[test]
    fn receipt_has_correct_identity_fields() {
        let ctx = test_ctx();
        let (_, receipt) = invoke_with_receipt(&DelayConnector, &ctx, &json!({"duration_ms": 0}));

        assert_eq!(receipt.flow_run_id, ctx.flow_run_id);
        assert_eq!(receipt.node_id, ctx.node_id);
        assert_eq!(receipt.step_run_id, ctx.step_run_id);
        assert_eq!(receipt.attempt_id_raw, ctx.attempt_id);
        assert_eq!(receipt.connector, "delay");
    }

    #[test]
    fn receipt_duration_is_reasonable() {
        let ctx = test_ctx();
        let (_, receipt) = invoke_with_receipt(&DelayConnector, &ctx, &json!({"duration_ms": 50}));

        assert!(receipt.duration_ms >= 50);
    }

    // T-10: Tracing Span Assertions (Sprint 8)

    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    /// Shared buffer writer for capturing tracing output in tests.
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Clone for BufWriter {
        fn clone(&self) -> Self {
            BufWriter(Arc::clone(&self.0))
        }
    }

    impl<'a> fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn capture_tracing<F: FnOnce()>(f: F) -> String {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let writer = BufWriter(Arc::clone(&buf));
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .with_writer(writer)
                .with_ansi(false)
                .with_target(false)
                .with_level(true)
                .with_span_events(fmt::format::FmtSpan::NEW | fmt::format::FmtSpan::CLOSE),
        );
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn connector_invoke_span_has_connector_name() {
        let ctx = test_ctx();

        let output = capture_tracing(|| {
            let _ = invoke_with_receipt(&DelayConnector, &ctx, &json!({"duration_ms": 10}));
        });

        assert!(
            output.contains("connector_invoke"),
            "tracing output should contain connector_invoke span, got:\n{}",
            output
        );
        assert!(
            output.contains("delay"),
            "tracing output should contain connector name 'delay', got:\n{}",
            output
        );
        assert!(
            output.contains(&ctx.flow_run_id.to_string()),
            "tracing output should contain flow_run_id, got:\n{}",
            output
        );
    }
}
