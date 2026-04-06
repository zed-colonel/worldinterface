//! Error types for connector invocations and transform execution.

use serde_json::Value;

/// Errors from connector invocations.
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    /// Transient failure — the invocation can be retried.
    /// Maps to AQ `HandlerOutput::RetryableFailure` in Sprint 4.
    #[error("retryable error: {0}")]
    Retryable(String),

    /// Permanent failure — retrying will not help.
    /// Maps to AQ `HandlerOutput::TerminalFailure` in Sprint 4.
    #[error("terminal error: {message}")]
    Terminal { message: String, diagnostics: Option<Value> },

    /// The invocation was cancelled via the cancellation token.
    /// Maps to AQ `HandlerOutput::RetryableFailure` in Sprint 4.
    #[error("operation cancelled")]
    Cancelled,

    /// The connector received parameters it cannot process.
    /// Maps to AQ `HandlerOutput::TerminalFailure` in Sprint 4.
    #[error("invalid parameters: {0}")]
    InvalidParams(String),
}

impl ConnectorError {
    pub fn terminal(message: impl Into<String>) -> Self {
        Self::Terminal { message: message.into(), diagnostics: None }
    }

    pub fn terminal_with_diagnostics(message: impl Into<String>, diagnostics: Value) -> Self {
        Self::Terminal { message: message.into(), diagnostics: Some(diagnostics) }
    }

    pub fn diagnostics(&self) -> Option<&Value> {
        match self {
            Self::Terminal { diagnostics, .. } => diagnostics.as_ref(),
            _ => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn terminal_with_diagnostics() {
        let err = ConnectorError::terminal_with_diagnostics(
            "old_string not found",
            json!({"line_count": 50, "closest_match_line": 12}),
        );
        match &err {
            ConnectorError::Terminal { message, diagnostics } => {
                assert_eq!(message, "old_string not found");
                assert!(diagnostics.is_some());
                assert_eq!(diagnostics.as_ref().unwrap()["line_count"], 50);
            }
            other => panic!("expected Terminal, got {other:?}"),
        }
        assert!(err.to_string().contains("old_string not found"));
    }

    #[test]
    fn terminal_without_diagnostics() {
        let err = ConnectorError::terminal("simple error");
        match &err {
            ConnectorError::Terminal { message, diagnostics } => {
                assert_eq!(message, "simple error");
                assert!(diagnostics.is_none());
            }
            other => panic!("expected Terminal, got {other:?}"),
        }
    }
}
