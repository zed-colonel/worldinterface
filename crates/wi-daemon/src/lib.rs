//! HTTP API daemon for WorldInterface.
//!
//! Wraps `wi-host` in an axum HTTP server with endpoints for flow submission,
//! run inspection, capability discovery, and single-op invocation.

pub mod config;
pub mod error;
pub mod metrics;
pub mod router;
pub mod routes;
pub mod server;
pub mod state;

pub use config::DaemonConfig;
pub use error::{ApiError, DaemonError};
pub use metrics::WiMetricsRegistry;
pub use routes::flows::{SubmitFlowRequest, SubmitFlowResponse};
pub use server::run;
pub use state::{AppState, SharedState};
