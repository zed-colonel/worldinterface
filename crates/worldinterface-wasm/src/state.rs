//! Per-invocation WASM store state.
//!
//! Each `WasmConnector::invoke()` creates a fresh `WasmState` containing
//! WASI context, resource table, capability policy, and resource pool references.

use std::sync::Arc;

use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::policy::CapabilityPolicy;
use crate::resource_pool::WasmResourcePool;

/// Per-invocation state stored in the wasmtime `Store`.
pub struct WasmState {
    /// WASI standard context (environment, preopened dirs, stdio).
    pub wasi_ctx: WasiCtx,
    /// WASI resource table (file handles, etc.).
    pub resource_table: ResourceTable,
    /// Capability policy for this module.
    pub policy: Arc<CapabilityPolicy>,
    /// Shared resource pool (HTTP client, KV store).
    pub resource_pool: Arc<WasmResourcePool>,
    /// Module name (for KV namespacing, logging attribution).
    pub module_name: String,
}

impl WasiView for WasmState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi_ctx, table: &mut self.resource_table }
    }
}
