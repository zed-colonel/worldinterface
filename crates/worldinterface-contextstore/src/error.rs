//! Error types for ContextStore operations.

use worldinterface_core::id::{FlowRunId, NodeId};

/// Errors from ContextStore operations.
#[derive(Debug, thiserror::Error)]
pub enum ContextStoreError {
    /// Attempted to write a key that already has a value.
    /// This is expected during idempotent retries (Invariant 4).
    #[error("output already exists for ({flow_run_id}, {node_id})")]
    AlreadyExists { flow_run_id: FlowRunId, node_id: NodeId },

    /// Attempted to write a global key that already has a value.
    #[error("global key already exists: {key}")]
    GlobalAlreadyExists { key: String },

    /// The value could not be serialized to bytes for storage.
    #[error("serialization failed: {0}")]
    SerializationFailed(#[from] serde_json::Error),

    /// The stored bytes could not be deserialized back to a Value.
    #[error("deserialization failed: {source}")]
    DeserializationFailed {
        #[source]
        source: serde_json::Error,
    },

    /// Underlying storage I/O error.
    #[error("storage error: {0}")]
    StorageError(String),
}

/// Errors from the atomic write-before-complete protocol.
#[derive(Debug, thiserror::Error)]
pub enum AtomicWriteError {
    /// The ContextStore write failed (non-idempotent failure).
    #[error("context store write failed: {0}")]
    WriteFailed(ContextStoreError),

    /// The write succeeded but the completion callback failed.
    /// The output IS durable — on retry, the write will return AlreadyExists
    /// and completion will be re-attempted.
    #[error("completion failed after successful write: {0}")]
    CompletionFailed(Box<dyn std::error::Error + Send + Sync>),
}
