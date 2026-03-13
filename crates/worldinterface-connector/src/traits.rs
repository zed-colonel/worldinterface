//! The Connector trait — contract for all boundary-crossing implementations.

use serde_json::Value;
use worldinterface_core::descriptor::Descriptor;

use crate::context::InvocationContext;
use crate::error::ConnectorError;

/// A boundary-crossing unit. Connectors interact with external systems
/// (HTTP endpoints, filesystems, clocks) on behalf of WorldInterface.
///
/// Connectors must be:
/// - `Send + Sync` (invoked from AQ handler threads)
/// - Idempotency-aware (receive `run_id` for external deduplication)
/// - Cooperative with cancellation (check `ctx.cancellation` periodically)
pub trait Connector: Send + Sync {
    /// Self-describe this connector's capabilities.
    fn describe(&self) -> Descriptor;

    /// Invoke the connector with the given parameters.
    ///
    /// The connector receives fully-resolved parameters (no template references).
    /// Parameter resolution is the Step handler's responsibility (Sprint 4).
    ///
    /// Returns the output value on success, or a typed error indicating whether
    /// the failure is retryable or terminal.
    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError>;
}
