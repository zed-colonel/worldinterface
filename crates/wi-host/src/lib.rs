//! Embedded Host for WorldInterface.
//!
//! The Host is the primary programmatic API surface. It owns the ActionQueue
//! engine, ContextStore, and ConnectorRegistry, and exposes flow submission,
//! capability discovery, and single-op invocation.
//!
//! This is the integration surface for agents and other embedders.

pub mod config;
pub mod error;
mod helpers;
pub mod host;
pub mod status;
mod tick;

pub use config::HostConfig;
pub use error::HostError;
pub use host::EmbeddedHost;
pub use status::{FlowPhase, FlowRunStatus, FlowRunSummary, StepPhase, StepStatus};
