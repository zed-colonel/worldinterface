//! Error types for the Coordinator and Step handlers.

use worldinterface_connector::ConnectorError;
use worldinterface_connector::TransformError;
use worldinterface_contextstore::ContextStoreError;
use worldinterface_core::id::NodeId;

/// Errors from the Coordinator handler.
#[derive(Debug, thiserror::Error)]
pub enum CoordinatorError {
    /// Failed to deserialize the payload.
    #[error("payload deserialization failed: {0}")]
    PayloadDeserializationFailed(#[from] serde_json::Error),

    /// ContextStore operation failed.
    #[error("context store error: {0}")]
    ContextStoreError(#[from] ContextStoreError),

    /// A step task failed terminally.
    #[error("step {node_id} failed: {error}")]
    StepFailed { node_id: NodeId, error: String },
}

/// Errors from the Step handler.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    /// Failed to deserialize the payload.
    #[error("payload deserialization failed: {0}")]
    PayloadDeserializationFailed(#[from] serde_json::Error),

    /// Connector not found in registry.
    #[error("connector not found: {name}")]
    ConnectorNotFound { name: String },

    /// Parameter resolution failed.
    #[error("parameter resolution failed: {0}")]
    ResolveFailed(#[from] ResolveError),

    /// ContextStore operation failed.
    #[error("context store error: {0}")]
    ContextStoreError(#[from] ContextStoreError),

    /// Connector invocation error.
    #[error("connector error: {0}")]
    ConnectorError(#[from] ConnectorError),

    /// Transform execution error.
    #[error("transform error: {0}")]
    TransformError(#[from] TransformError),

    /// Branch evaluation error.
    #[error("branch evaluation error: {0}")]
    BranchEvalError(#[from] BranchEvalError),
}

/// Errors from parameter resolution.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("node output not found for {node_id}")]
    NodeOutputNotFound { node_id: NodeId },

    #[error("path '{path}' not found in output of node {node_id}")]
    PathNotFound { node_id: NodeId, path: String },

    #[error("flow parameter not found at path '{path}'")]
    FlowParamNotFound { path: String },

    /// Trigger input not found in ContextStore.
    /// Flow was not started via webhook, or trigger data was not written.
    #[error("trigger input not found — was this flow started via a webhook?")]
    TriggerInputNotFound,

    #[error("context store error: {0}")]
    ContextStoreError(#[from] ContextStoreError),
}

/// Errors from branch evaluation.
#[derive(Debug, thiserror::Error)]
pub enum BranchEvalError {
    #[error("expression evaluation not implemented")]
    ExpressionNotImplemented,

    #[error("failed to resolve param ref: {0}")]
    ResolveFailed(#[from] ResolveError),
}
