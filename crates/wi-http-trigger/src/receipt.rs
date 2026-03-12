//! Trigger receipt generation for webhook boundary crossings.

use chrono::Utc;
use uuid::Uuid;
use wi_core::id::{trigger_input_node_id, FlowRunId, StepRunId};
use wi_core::receipt::{sha256_hex, Receipt, ReceiptStatus};

use crate::trigger::TriggerInput;

/// Generate a Receipt for a webhook trigger boundary crossing.
///
/// The webhook is a boundary crossing: external HTTP request -> internal FlowRun.
/// The receipt captures the trigger metadata as immutable evidence per IBP section 6.
pub fn create_trigger_receipt(flow_run_id: FlowRunId, trigger_input: &TriggerInput) -> Receipt {
    let input_bytes = serde_json::to_vec(trigger_input).unwrap_or_default();
    let input_hash = sha256_hex(&input_bytes);

    Receipt::new(
        flow_run_id,
        trigger_input_node_id(),
        StepRunId::new(),
        "webhook.trigger".into(),
        Utc::now(),
        Uuid::new_v4(), // attempt_id = unique (triggers are not retried)
        input_hash.clone(),
        Some(input_hash), // output_hash = input_hash (trigger passes through)
        ReceiptStatus::Success,
        None, // no error
        0,    // duration_ms = 0 (trigger is instantaneous)
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample_trigger_input() -> TriggerInput {
        TriggerInput {
            body: json!({"event": "push"}),
            headers: json!({"content-type": "application/json"}),
            method: "POST".to_string(),
            path: "github/push".to_string(),
            source_addr: None,
            received_at: 1741200000,
        }
    }

    #[test]
    fn receipt_has_webhook_trigger_connector() {
        let receipt = create_trigger_receipt(FlowRunId::new(), &sample_trigger_input());
        assert_eq!(receipt.connector, "webhook.trigger");
    }

    #[test]
    fn receipt_has_success_status() {
        let receipt = create_trigger_receipt(FlowRunId::new(), &sample_trigger_input());
        assert_eq!(receipt.status, ReceiptStatus::Success);
    }

    #[test]
    fn receipt_has_correct_flow_run_id() {
        let frid = FlowRunId::new();
        let receipt = create_trigger_receipt(frid, &sample_trigger_input());
        assert_eq!(receipt.flow_run_id, frid);
    }

    #[test]
    fn receipt_has_trigger_node_id() {
        let receipt = create_trigger_receipt(FlowRunId::new(), &sample_trigger_input());
        assert_eq!(receipt.node_id, trigger_input_node_id());
    }

    #[test]
    fn receipt_has_zero_duration() {
        let receipt = create_trigger_receipt(FlowRunId::new(), &sample_trigger_input());
        assert_eq!(receipt.duration_ms, 0);
    }
}
