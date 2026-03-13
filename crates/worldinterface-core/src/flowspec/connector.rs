//! Connector node types for FlowSpec.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A boundary-crossing node that invokes an external connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorNode {
    /// Registry name of the connector (e.g., "http.request", "fs.write").
    pub connector: String,
    /// Connector-specific parameters. Opaque JSON validated by the connector at
    /// invocation time. May contain template references like
    /// `"{{nodes.step_a.output.field}}"` resolved at runtime.
    pub params: Value,
    /// Optional idempotency configuration override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_config: Option<IdempotencyConfig>,
}

/// Override for default idempotency behavior on a connector node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyConfig {
    pub strategy: IdempotencyStrategy,
}

/// Idempotency strategy for a connector invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyStrategy {
    /// Use the ActionQueue RunId as the idempotency key (default).
    RunId,
}
