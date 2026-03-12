//! Error types for the Host.

use wi_contextstore::error::ContextStoreError;
use wi_core::id::FlowRunId;
use wi_flowspec::error::CompilationError;

/// Errors from the Host.
#[derive(Debug, thiserror::Error)]
pub enum HostError {
    /// Host configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// FlowSpec compilation failed (includes validation failures).
    #[error("flow compilation failed: {0}")]
    Compilation(#[from] CompilationError),

    /// ActionQueue engine bootstrap failed.
    #[error("engine bootstrap failed: {0}")]
    Bootstrap(#[from] actionqueue_runtime::engine::BootstrapError),

    /// ActionQueue engine error during operation.
    #[error("engine error: {0}")]
    Engine(#[from] actionqueue_runtime::engine::EngineError),

    /// ContextStore error.
    #[error("context store error: {0}")]
    ContextStore(#[from] ContextStoreError),

    /// FlowRun not found.
    #[error("flow run not found: {0}")]
    FlowRunNotFound(FlowRunId),

    /// Connector not found in registry.
    #[error("connector not found: {0}")]
    ConnectorNotFound(String),

    /// Flow execution failed.
    #[error("flow {flow_run_id} failed: {error}")]
    FlowFailed { flow_run_id: FlowRunId, error: String },

    /// Flow was canceled.
    #[error("flow {0} was canceled")]
    FlowCanceled(FlowRunId),

    /// I/O error (creating directories, etc.)
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// UUID parse error (for coordinator map restoration).
    #[error("UUID parse error: {0}")]
    UuidParse(#[from] uuid::Error),

    /// Internal error (should not happen — indicates a bug).
    #[error("internal error: {0}")]
    InternalError(String),
}
