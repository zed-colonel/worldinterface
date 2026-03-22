//! wasi:random — cryptographic random number generation.
//!
//! Uses the default `wasmtime_wasi` implementations registered via
//! `wasmtime_wasi::add_to_linker_sync()`. No policy concern — random
//! is always available to WASM modules.
//!
//! Guest access: `getrandom` crate or `wasi:random/random` directly.
