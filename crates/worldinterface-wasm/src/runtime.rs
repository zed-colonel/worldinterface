//! WASM runtime — Engine, Linker, epoch ticker.
//!
//! The runtime is shared across all WasmConnector instances. It holds the
//! compiled engine configuration, registered host functions, and the
//! background epoch ticker for wall-clock timeouts.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use wasmtime::component::Linker;
use wasmtime::Engine;

use crate::metering::{self, EPOCH_INTERVAL_MS};
use crate::resource_pool::WasmResourcePool;
use crate::state::WasmState;

/// Configuration for the WASM runtime.
pub struct WasmRuntimeConfig {
    /// Default fuel budget for modules that don't specify one.
    pub default_fuel: u64,
    /// Default timeout for modules that don't specify one.
    pub default_timeout: Duration,
    /// Default memory limit.
    pub default_memory_bytes: usize,
    /// KV store directory path (for per-module SQLite databases).
    pub kv_store_dir: PathBuf,
}

impl Default for WasmRuntimeConfig {
    fn default() -> Self {
        Self {
            default_fuel: metering::DEFAULT_FUEL,
            default_timeout: metering::DEFAULT_TIMEOUT,
            default_memory_bytes: metering::DEFAULT_MAX_MEMORY,
            kv_store_dir: std::env::temp_dir().join("worldinterface-wasm-kv"),
        }
    }
}

/// The shared WASM runtime — holds Engine, Linker, resource pool, and epoch ticker.
pub struct WasmRuntime {
    engine: Engine,
    linker: Linker<WasmState>,
    resource_pool: Arc<WasmResourcePool>,
    config: WasmRuntimeConfig,
    /// Shutdown signal for the epoch ticker.
    shutdown: Arc<AtomicBool>,
    /// Handle to the epoch ticker background thread.
    _epoch_ticker: JoinHandle<()>,
}

impl WasmRuntime {
    /// Create a new WASM runtime with the given configuration.
    pub fn new(config: WasmRuntimeConfig) -> Result<Self, crate::error::WasmError> {
        // 1. Configure engine
        let mut engine_config = wasmtime::Config::new();
        engine_config.wasm_component_model(true);
        engine_config.consume_fuel(true);
        engine_config.epoch_interruption(true);
        engine_config.max_wasm_stack(metering::WASM_STACK_LIMIT);

        let engine = Engine::new(&engine_config)
            .map_err(|e| crate::error::WasmError::Compilation(e.to_string()))?;

        // 2. Create Linker and register host functions
        let mut linker: Linker<WasmState> = Linker::new(&engine);

        // Register WASI standard implementations
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| crate::error::WasmError::Compilation(format!("WASI linker: {e}")))?;

        // Register custom Exo host functions
        crate::host_functions::register_all(&mut linker)
            .map_err(|e| crate::error::WasmError::Compilation(format!("host functions: {e}")))?;

        // 3. Spawn epoch ticker
        let shutdown = Arc::new(AtomicBool::new(false));
        let ticker_engine = engine.clone();
        let ticker_shutdown = Arc::clone(&shutdown);
        let epoch_ticker = std::thread::Builder::new()
            .name("wasm-epoch-ticker".into())
            .spawn(move || {
                while !ticker_shutdown.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(EPOCH_INTERVAL_MS));
                    ticker_engine.increment_epoch();
                }
            })
            .map_err(crate::error::WasmError::Io)?;

        // 4. Create resource pool
        let resource_pool = Arc::new(WasmResourcePool::new(&config.kv_store_dir)?);

        Ok(Self { engine, linker, resource_pool, config, shutdown, _epoch_ticker: epoch_ticker })
    }

    /// Get the wasmtime Engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Get the Linker.
    pub fn linker(&self) -> &Linker<WasmState> {
        &self.linker
    }

    /// Get the shared resource pool.
    pub fn resource_pool(&self) -> &Arc<WasmResourcePool> {
        &self.resource_pool
    }

    /// Get the runtime configuration.
    pub fn config(&self) -> &WasmRuntimeConfig {
        &self.config
    }
}

impl Drop for WasmRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── E2S3-T16: WasmRuntime::new creates engine with fuel metering ──

    #[test]
    fn wasm_runtime_creates_engine_with_fuel_metering() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            WasmRuntimeConfig { kv_store_dir: dir.path().to_path_buf(), ..Default::default() };
        let runtime = WasmRuntime::new(config).unwrap();

        // Engine exists and was configured with component model + fuel
        let _engine = runtime.engine();

        // Verify we can create a store with fuel enabled
        let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new().build();
        let state = WasmState {
            wasi_ctx,
            resource_table: wasmtime::component::ResourceTable::new(),
            policy: Arc::new(crate::policy::CapabilityPolicy::deny_all()),
            resource_pool: Arc::clone(runtime.resource_pool()),
            module_name: "test".into(),
        };
        let mut store = wasmtime::Store::new(runtime.engine(), state);
        // set_fuel should succeed because consume_fuel was enabled
        store.set_fuel(1_000_000).unwrap();
        let fuel = store.get_fuel().unwrap();
        assert_eq!(fuel, 1_000_000);
    }
}
