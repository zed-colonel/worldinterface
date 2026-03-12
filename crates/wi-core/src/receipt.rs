//! Receipt artifacts — immutable evidence of boundary crossings.
//!
//! Every connector invocation produces a `Receipt`. Receipts are
//! content-addressed (their ID is a SHA-256 hash of their fields),
//! making them tamper-evident and deduplication-friendly.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::id::{FlowRunId, NodeId, StepRunId};

/// Immutable evidence of a single boundary crossing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    /// Content-addressed ID (SHA-256 hash of all other fields).
    pub id: ReceiptId,
    /// Which flow run this crossing belongs to.
    pub flow_run_id: FlowRunId,
    /// Which node performed the crossing.
    pub node_id: NodeId,
    /// The step execution that performed the crossing.
    pub step_run_id: StepRunId,
    /// Name of the connector that was invoked.
    pub connector: String,
    /// When the crossing occurred.
    pub timestamp: DateTime<Utc>,
    /// The ActionQueue AttemptId (stored as raw UUID to avoid AQ dependency).
    pub attempt_id_raw: Uuid,
    /// SHA-256 hex digest of the serialized input parameters.
    pub input_hash: String,
    /// SHA-256 hex digest of the output. `None` if the crossing failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_hash: Option<String>,
    /// Whether the crossing succeeded, failed, or timed out.
    pub status: ReceiptStatus,
    /// Error message, present when status is `Failure` or `Timeout`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Wall-clock duration of the connector invocation in milliseconds.
    pub duration_ms: u64,
}

impl Receipt {
    /// Construct a new receipt. The content-addressed ID is computed
    /// automatically from all fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        flow_run_id: FlowRunId,
        node_id: NodeId,
        step_run_id: StepRunId,
        connector: String,
        timestamp: DateTime<Utc>,
        attempt_id_raw: Uuid,
        input_hash: String,
        output_hash: Option<String>,
        status: ReceiptStatus,
        error: Option<String>,
        duration_ms: u64,
    ) -> Self {
        let id = Self::compute_id(
            &flow_run_id,
            &node_id,
            &step_run_id,
            &connector,
            &timestamp,
            &attempt_id_raw,
            &input_hash,
            output_hash.as_deref(),
            &status,
            error.as_deref(),
            duration_ms,
        );
        Self {
            id,
            flow_run_id,
            node_id,
            step_run_id,
            connector,
            timestamp,
            attempt_id_raw,
            input_hash,
            output_hash,
            status,
            error,
            duration_ms,
        }
    }

    /// Compute the content-addressed ID by hashing fields in a fixed order.
    #[allow(clippy::too_many_arguments)]
    fn compute_id(
        flow_run_id: &FlowRunId,
        node_id: &NodeId,
        step_run_id: &StepRunId,
        connector: &str,
        timestamp: &DateTime<Utc>,
        attempt_id_raw: &Uuid,
        input_hash: &str,
        output_hash: Option<&str>,
        status: &ReceiptStatus,
        error: Option<&str>,
        duration_ms: u64,
    ) -> ReceiptId {
        let mut hasher = Sha256::new();
        hasher.update(flow_run_id.as_ref().as_bytes());
        hasher.update(node_id.as_ref().as_bytes());
        hasher.update(step_run_id.as_ref().as_bytes());
        hasher.update(connector.as_bytes());
        hasher.update(timestamp.to_rfc3339().as_bytes());
        hasher.update(attempt_id_raw.as_bytes());
        hasher.update(input_hash.as_bytes());
        match output_hash {
            Some(h) => {
                hasher.update(b"\x01");
                hasher.update(h.as_bytes());
            }
            None => hasher.update(b"\x00"),
        }
        hasher.update(status.as_str().as_bytes());
        match error {
            Some(e) => {
                hasher.update(b"\x01");
                hasher.update(e.as_bytes());
            }
            None => hasher.update(b"\x00"),
        }
        hasher.update(duration_ms.to_le_bytes());

        let hash = hasher.finalize();
        ReceiptId(format!("{:x}", hash))
    }
}

/// Content-addressed receipt identity (SHA-256 hex string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ReceiptId(String);

impl ReceiptId {
    /// The hex-encoded SHA-256 hash.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ReceiptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Outcome of a boundary crossing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    Success,
    Failure,
    Timeout,
}

impl ReceiptStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Timeout => "timeout",
        }
    }
}

/// Compute the SHA-256 hex digest of arbitrary bytes. Utility for callers
/// that need to hash connector inputs/outputs before constructing a Receipt.
pub fn sha256_hex(data: &[u8]) -> String {
    let hash = Sha256::digest(data);
    format!("{:x}", hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{FlowRunId, NodeId, StepRunId};

    fn sample_receipt() -> Receipt {
        Receipt::new(
            FlowRunId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()),
            NodeId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()),
            StepRunId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()),
            "http.request".into(),
            DateTime::parse_from_rfc3339("2026-03-05T12:00:00Z").unwrap().with_timezone(&Utc),
            Uuid::parse_str("00000000-0000-0000-0000-000000000004").unwrap(),
            "abc123".into(),
            Some("def456".into()),
            ReceiptStatus::Success,
            None,
            150,
        )
    }

    #[test]
    fn content_addressable_id_is_deterministic() {
        let a = sample_receipt();
        let b = sample_receipt();
        assert_eq!(a.id, b.id);
    }

    #[test]
    fn changing_any_field_changes_id() {
        let base = sample_receipt();

        // Change connector name
        let modified = Receipt::new(
            base.flow_run_id,
            base.node_id,
            base.step_run_id,
            "fs.write".into(),
            base.timestamp,
            base.attempt_id_raw,
            base.input_hash.clone(),
            base.output_hash.clone(),
            base.status,
            base.error.clone(),
            base.duration_ms,
        );
        assert_ne!(base.id, modified.id);

        // Change duration
        let modified2 = Receipt::new(
            base.flow_run_id,
            base.node_id,
            base.step_run_id,
            base.connector.clone(),
            base.timestamp,
            base.attempt_id_raw,
            base.input_hash.clone(),
            base.output_hash.clone(),
            base.status,
            base.error.clone(),
            999,
        );
        assert_ne!(base.id, modified2.id);

        // Change status
        let modified3 = Receipt::new(
            base.flow_run_id,
            base.node_id,
            base.step_run_id,
            base.connector.clone(),
            base.timestamp,
            base.attempt_id_raw,
            base.input_hash.clone(),
            base.output_hash.clone(),
            ReceiptStatus::Failure,
            Some("boom".into()),
            base.duration_ms,
        );
        assert_ne!(base.id, modified3.id);
    }

    #[test]
    fn receipt_json_roundtrip() {
        let receipt = sample_receipt();
        let json = serde_json::to_string(&receipt).unwrap();
        let back: Receipt = serde_json::from_str(&json).unwrap();
        assert_eq!(receipt, back);
    }

    #[test]
    fn receipt_status_variants_roundtrip() {
        for status in [ReceiptStatus::Success, ReceiptStatus::Failure, ReceiptStatus::Timeout] {
            let json = serde_json::to_string(&status).unwrap();
            let back: ReceiptStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(status, back);
        }
    }

    #[test]
    fn receipt_with_no_output_hash() {
        let receipt = Receipt::new(
            FlowRunId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()),
            NodeId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()),
            StepRunId::from(Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()),
            "http.request".into(),
            Utc::now(),
            Uuid::new_v4(),
            "abc".into(),
            None,
            ReceiptStatus::Failure,
            Some("connection refused".into()),
            0,
        );
        let json = serde_json::to_string(&receipt).unwrap();
        let back: Receipt = serde_json::from_str(&json).unwrap();
        assert_eq!(receipt, back);
        assert!(receipt.output_hash.is_none());
        assert!(receipt.error.is_some());
    }

    #[test]
    fn sha256_hex_utility() {
        let hash = sha256_hex(b"hello world");
        assert_eq!(hash.len(), 64); // 256 bits = 64 hex chars
                                    // Known SHA-256 of "hello world"
        assert_eq!(hash, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
    }

    #[test]
    fn receipt_id_display() {
        let receipt = sample_receipt();
        let display = receipt.id.to_string();
        assert_eq!(display, receipt.id.as_str());
        assert_eq!(display.len(), 64);
    }
}
