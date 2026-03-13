//! Shared application state for route handlers.

use std::sync::{Arc, RwLock};

use worldinterface_host::EmbeddedHost;
use worldinterface_http_trigger::WebhookRegistry;

use crate::metrics::WiMetricsRegistry;

/// Shared application state for all route handlers.
pub struct AppState {
    /// The embedded host (owns AQ engine, ContextStore, ConnectorRegistry).
    pub host: EmbeddedHost,
    /// Webhook registry (loaded from ContextStore at startup, modified via API).
    pub webhook_registry: RwLock<WebhookRegistry>,
    /// Prometheus metrics registry.
    pub metrics: Arc<WiMetricsRegistry>,
}

/// Type alias for the shared state passed to axum handlers.
pub type SharedState = Arc<AppState>;
