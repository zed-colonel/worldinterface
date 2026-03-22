//! wasi:filesystem — scoped filesystem access via preopened directories.
//!
//! Scoped at configuration time via `WasiCtxBuilder::preopened_dir()` in
//! `WasmConnector::build_wasi_ctx()`. Only directories listed in
//! `manifest.capabilities.filesystem` are preopened.
//!
//! The WASI filesystem implementation restricts access to preopened
//! directories — no per-call policy checks needed. This is "policy at
//! configuration time" rather than "policy at call time".
//!
//! Guest access: `std::fs::*` in Rust, or `wasi:filesystem/types` directly.
