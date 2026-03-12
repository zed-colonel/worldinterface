//! Compilation error types.

use wi_core::flowspec::validate::ValidationError;
use wi_core::id::NodeId;

/// Errors that can occur during FlowSpec compilation.
#[derive(Debug, thiserror::Error)]
pub enum CompilationError {
    /// The FlowSpec failed validation.
    #[error("FlowSpec validation failed: {0}")]
    ValidationFailed(#[from] ValidationError),

    /// Failed to serialize a payload to JSON.
    #[error("payload serialization failed: {0}")]
    PayloadSerializationFailed(#[from] serde_json::Error),

    /// Failed to construct an AQ TaskSpec.
    #[error("TaskSpec construction failed for node {node_id:?}: {source}")]
    TaskSpecFailed {
        node_id: Option<NodeId>,
        #[source]
        source: actionqueue_core::task::task_spec::TaskSpecError,
    },

    /// The FlowSpec has no nodes (should be caught by validation, but defensive).
    #[error("FlowSpec has no nodes")]
    EmptyFlowSpec,
}
