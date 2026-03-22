//! wasi:clocks — wall clock and monotonic clock.
//!
//! Uses the default `wasmtime_wasi` implementations registered via
//! `wasmtime_wasi::add_to_linker_sync()`. No policy concern — clocks are
//! always available to WASM modules.
//!
//! Guest access: `std::time::SystemTime`, `std::time::Instant` in Rust,
//! or directly via `wasi:clocks/wall-clock` and `wasi:clocks/monotonic-clock`.
