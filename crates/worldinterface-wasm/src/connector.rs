//! WasmConnector — implements the `Connector` trait for WASM modules.
//!
//! The rest of WorldInterface doesn't know or care that a connector is WASM.

use std::sync::Arc;

use serde_json::Value;
use wasmtime::component::{Component, ResourceTable};
use wasmtime::Store;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};
use worldinterface_connector::context::InvocationContext;
use worldinterface_connector::error::ConnectorError;
use worldinterface_connector::traits::Connector;
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::manifest::ConnectorManifest;
use crate::metering::EPOCH_INTERVAL_MS;
use crate::policy::CapabilityPolicy;
use crate::runtime::WasmRuntime;
use crate::state::WasmState;

// Generate host-side bindings from WIT.
// This produces:
// - `ConnectorWorld` struct with `instantiate()` and `add_to_linker()`
// - Traits for each imported interface (logging::Host, kv::Host, etc.)
// - `exports::exo::connector::connector::Guest` with call_describe/call_invoke
wasmtime::component::bindgen!({
    world: "connector-world",
    path: "wit",
    imports: {
        "exo:connector/logging@0.1.0": trappable,
        "exo:connector/kv@0.1.0": trappable,
        "exo:connector/crypto@0.1.0": trappable,
        "exo:connector/process@0.1.0": trappable,
        "exo:connector/http@0.1.0": trappable,
        "exo:connector/websocket@0.1.0": trappable,
    },
});

/// A WASM connector module implementing the Connector trait.
pub struct WasmConnector {
    /// Shared runtime (Engine + Linker + resource pool).
    runtime: Arc<WasmRuntime>,
    /// Pre-compiled WASM component.
    component: Component,
    /// Parsed manifest metadata.
    manifest: ConnectorManifest,
    /// Compiled capability policy.
    policy: Arc<CapabilityPolicy>,
}

impl std::fmt::Debug for WasmConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmConnector").field("name", &self.manifest.connector.name).finish()
    }
}

impl WasmConnector {
    /// Create a new WasmConnector from a compiled component and manifest.
    pub fn new(
        runtime: Arc<WasmRuntime>,
        component: Component,
        manifest: ConnectorManifest,
        policy: CapabilityPolicy,
    ) -> Self {
        Self { runtime, component, manifest, policy: Arc::new(policy) }
    }

    /// Build a per-invocation WASI context scoped to the manifest's capabilities.
    fn build_wasi_ctx(&self) -> Result<WasiCtx, ConnectorError> {
        let mut builder = WasiCtxBuilder::new();

        // Only expose allowed environment variables
        for var in &self.manifest.capabilities.environment {
            if let Ok(val) = std::env::var(var) {
                builder.env(var, &val);
            }
        }

        // Only preopen allowed filesystem paths
        for prefix in &self.manifest.capabilities.filesystem {
            if prefix.exists() {
                builder
                    .preopened_dir(
                        prefix,
                        prefix.to_string_lossy().as_ref(),
                        wasmtime_wasi::DirPerms::all(),
                        wasmtime_wasi::FilePerms::all(),
                    )
                    .map_err(|e| ConnectorError::Terminal(format!("WASI preopened dir: {e}")))?;
            }
        }

        builder.inherit_stdio();
        Ok(builder.build())
    }

    /// Map a wasmtime error/trap to a ConnectorError.
    fn map_trap(err: wasmtime::Error) -> ConnectorError {
        let msg = err.to_string();
        if msg.contains("all fuel consumed") {
            ConnectorError::Terminal("WASM fuel exhausted".into())
        } else if msg.contains("epoch deadline") || msg.contains("epoch-deadline") {
            ConnectorError::Terminal("WASM execution timed out".into())
        } else if msg.contains("memory") && msg.contains("limit") {
            ConnectorError::Terminal("WASM memory limit exceeded".into())
        } else {
            ConnectorError::Terminal(format!("WASM trap: {msg}"))
        }
    }
}

impl Connector for WasmConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: self.manifest.connector.name.clone(),
            display_name: self.manifest.connector.name.clone(),
            description: self.manifest.connector.description.clone(),
            category: ConnectorCategory::Wasm(self.manifest.connector.name.clone()),
            input_schema: None,
            output_schema: None,
            idempotent: false,
            side_effects: true,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        // 1. Check cancellation before starting
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        // 2. Build per-invocation WASI context
        let wasi_ctx = self.build_wasi_ctx()?;

        // 3. Create Store with fuel budget and epoch deadline
        let mut store = Store::new(
            self.runtime.engine(),
            WasmState {
                wasi_ctx,
                resource_table: ResourceTable::new(),
                policy: Arc::clone(&self.policy),
                resource_pool: Arc::clone(self.runtime.resource_pool()),
                module_name: self.manifest.connector.name.clone(),
            },
        );
        store
            .set_fuel(self.policy.max_fuel)
            .map_err(|e| ConnectorError::Terminal(format!("fuel setup: {e}")))?;

        let epoch_ticks = self.policy.timeout.as_millis() as u64 / EPOCH_INTERVAL_MS;
        store.epoch_deadline_trap();
        store.set_epoch_deadline(epoch_ticks);

        // 4. Instantiate component
        let bindings =
            ConnectorWorld::instantiate(&mut store, &self.component, self.runtime.linker())
                .map_err(|e| ConnectorError::Terminal(format!("WASM instantiation: {e}")))?;

        // 5. Build guest invocation context
        let guest_ctx = exports::exo::connector::connector::InvocationContext {
            flow_run_id: ctx.flow_run_id.to_string(),
            node_id: ctx.node_id.to_string(),
            run_id: ctx.run_id.to_string(),
            attempt_number: ctx.attempt_number,
        };

        // 6. Serialize params and call guest invoke()
        let params_json = serde_json::to_string(params)
            .map_err(|e| ConnectorError::InvalidParams(e.to_string()))?;

        let result =
            bindings.exo_connector_connector().call_invoke(&mut store, &guest_ctx, &params_json);

        // 7. Check cancellation after execution
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        // 8. Map result
        match result {
            Ok(Ok(output_json)) => serde_json::from_str(&output_json)
                .map_err(|e| ConnectorError::Terminal(format!("WASM output not valid JSON: {e}"))),
            Ok(Err(guest_error)) => Err(ConnectorError::Terminal(guest_error)),
            Err(trap) => Err(Self::map_trap(trap)),
        }
    }
}
