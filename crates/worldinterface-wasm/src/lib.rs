//! WASM runtime for WorldInterface connector modules.
//!
//! Provides a wasmtime-based runtime that loads, sandboxes, and executes
//! third-party WASM connector modules with deny-by-default capability enforcement.
//!
//! # Architecture
//!
//! - **Manifest:** `.connector.toml` sidecar files define module identity and capabilities
//! - **Policy:** Deny-by-default `CapabilityPolicy` compiled from manifests
//! - **Runtime:** wasmtime Engine with fuel metering, epoch-based timeout, memory limits
//! - **WasmConnector:** Implements the `Connector` trait — transparent to the rest of WI
//! - **Host functions:** WASI standard + custom Exo interfaces (logging, crypto, KV, process)
//! - **Resource isolation:** Separate HTTP client, per-module KV namespace, rate limiters

pub mod connector;
pub mod error;
pub mod host_functions;
pub mod manifest;
pub mod metering;
pub mod module_loader;
pub mod policy;
pub mod resource_pool;
pub mod runtime;
pub mod state;
pub mod streaming;
pub mod streaming_bindings;

// Re-exports
pub use connector::WasmConnector;
pub use error::{ManifestError, PolicyViolation, WasmError};
pub use manifest::ConnectorManifest;
pub use module_loader::{load_module, load_modules_from_dir, LoadedModules, StreamingModuleInfo};
pub use policy::CapabilityPolicy;
pub use resource_pool::WasmResourcePool;
pub use runtime::{WasmRuntime, WasmRuntimeConfig};
pub use state::WasmState;
pub use streaming::StreamingLifecycleManager;
