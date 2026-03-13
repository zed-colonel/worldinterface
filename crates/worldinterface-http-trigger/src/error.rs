//! Webhook error types.

use crate::types::WebhookId;

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    /// Webhook path is already registered.
    #[error("webhook path already registered: {0}")]
    PathAlreadyRegistered(String),

    /// Webhook not found by ID.
    #[error("webhook not found: {0}")]
    WebhookNotFound(WebhookId),

    /// Webhook not found by path (for invocation).
    #[error("no webhook registered for path: {0}")]
    PathNotFound(String),

    /// Invalid webhook path (empty, contains invalid characters, etc.)
    #[error("invalid webhook path: {0}")]
    InvalidPath(String),

    /// ContextStore error during persistence.
    #[error("storage error: {0}")]
    Storage(#[from] worldinterface_contextstore::ContextStoreError),

    /// JSON serialization error.
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),

    /// Host error during flow submission.
    #[error("flow submission error: {0}")]
    Host(#[from] worldinterface_host::HostError),
}
