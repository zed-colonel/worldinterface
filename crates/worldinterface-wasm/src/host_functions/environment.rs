//! wasi:cli/environment — filtered environment variable access.
//!
//! Scoped at configuration time via `WasiCtxBuilder::env()` in
//! `WasmConnector::build_wasi_ctx()`. Only variables listed in
//! `manifest.capabilities.environment` are set in the WASI context.
//!
//! The WASI implementation naturally restricts access to what's configured —
//! no per-call policy checks needed. This is "policy at configuration time"
//! rather than "policy at call time".
//!
//! Guest access: `std::env::var()` in Rust, or `wasi:cli/environment` directly.
