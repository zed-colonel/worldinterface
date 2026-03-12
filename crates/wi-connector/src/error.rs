//! Error types for connector invocations and transform execution.

/// Errors from connector invocations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Transient failure — the invocation can be retried.
    /// Maps to AQ `HandlerOutput::RetryableFailure` in Sprint 4.
    #[error("retryable error: {0}")]
    Retryable(String),

    /// Permanent failure — retrying will not help.
    /// Maps to AQ `HandlerOutput::TerminalFailure` in Sprint 4.
    #[error("terminal error: {0}")]
    Terminal(String),

    /// The invocation was cancelled via the cancellation token.
    /// Maps to AQ `HandlerOutput::RetryableFailure` in Sprint 4.
    #[error("operation cancelled")]
    Cancelled,

    /// The connector received parameters it cannot process.
    /// Maps to AQ `HandlerOutput::TerminalFailure` in Sprint 4.
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
}

/// Errors from transform execution.
#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    /// A referenced field path does not exist in the input.
    #[error("field not found: {path}")]
    FieldNotFound { path: String },

    /// The input value is not the expected type for the transform.
    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}
