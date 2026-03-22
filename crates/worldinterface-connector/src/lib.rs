//! Connector trait, registry, and built-in connectors for WorldInterface.
//!
//! Connectors are the boundary crossing units. Each connector implements
//! `describe()` for discovery and `invoke()` for execution, always receiving
//! a `RunId` as the idempotency key.

pub mod connectors;
pub mod context;
pub mod error;
pub mod receipt_gen;
pub mod registry;
pub mod traits;
pub mod transform;

pub use connectors::{
    default_registry, DelayConnector, FsReadConnector, FsWriteConnector, HttpRequestConnector,
    SandboxExecConnector, ShellExecConnector,
};
pub use context::{CancellationToken, InvocationContext};
pub use error::{ConnectorError, TransformError};
pub use receipt_gen::invoke_with_receipt;
pub use registry::ConnectorRegistry;
pub use traits::Connector;
pub use transform::execute_transform;
