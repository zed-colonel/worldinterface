//! ContextStore trait — the contract for durable node output storage.

use serde_json::Value;
use worldinterface_core::id::{FlowRunId, NodeId};

use crate::error::ContextStoreError;

/// Durable store for flow node outputs.
///
/// Keyed by `(FlowRunId, NodeId)` for node outputs and by `String` for globals.
/// All methods are synchronous — the SQLite backend runs on a blocking thread,
/// and callers (Step handlers in Sprint 4) will invoke from a blocking context
/// or use `spawn_blocking`.
pub trait ContextStore: Send + Sync {
    /// Write a node's output. Enforces write-once semantics:
    /// - First call for a given (flow_run_id, node_id): stores the value, returns Ok(())
    /// - Subsequent calls with the same key: returns Err(ContextStoreError::AlreadyExists)
    fn put(
        &self,
        flow_run_id: FlowRunId,
        node_id: NodeId,
        value: &Value,
    ) -> Result<(), ContextStoreError>;

    /// Read a node's output. Returns None if not yet written.
    fn get(
        &self,
        flow_run_id: FlowRunId,
        node_id: NodeId,
    ) -> Result<Option<Value>, ContextStoreError>;

    /// List all node IDs that have outputs for a given flow run.
    fn list_keys(&self, flow_run_id: FlowRunId) -> Result<Vec<NodeId>, ContextStoreError>;

    /// Write a global key-value pair. Globals are not scoped to a flow run.
    /// Write-once semantics: second put for the same key returns GlobalAlreadyExists.
    fn put_global(&self, key: &str, value: &Value) -> Result<(), ContextStoreError>;

    /// Write or update a global key-value pair.
    /// Unlike `put_global`, this allows overwriting an existing value.
    /// Used for mutable global metadata (e.g., coordinator map).
    fn upsert_global(&self, key: &str, value: &Value) -> Result<(), ContextStoreError>;

    /// Read a global value. Returns None if not yet written.
    fn get_global(&self, key: &str) -> Result<Option<Value>, ContextStoreError>;
}
