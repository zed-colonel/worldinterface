//! Error types for the WASM runtime.

use std::path::PathBuf;

/// Top-level errors from the WASM runtime.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),

    #[error("policy error: {0}")]
    Policy(#[from] PolicyViolation),

    #[error("WASM compilation failed: {0}")]
    Compilation(String),

    #[error("WASM instantiation failed: {0}")]
    Instantiation(String),

    #[error("WASM trap: {0}")]
    Trap(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("module '{name}' missing manifest: expected {path}")]
    MissingManifest { name: String, path: PathBuf },

    #[error("module '{name}' missing WASM file: expected {path}")]
    MissingWasm { name: String, path: PathBuf },

    #[error("module missing required exports: {0}")]
    MissingExports(String),
}

/// Errors from parsing or validating a `.connector.toml` manifest.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("TOML parse error: {0}")]
    Parse(String),

    #[error("manifest validation failed: {0}")]
    Validation(String),

    #[error("missing required section: {0}")]
    MissingSection(String),
}

/// Capability policy violations — returned when a WASM module attempts
/// an operation not permitted by its manifest.
#[derive(Debug, thiserror::Error)]
pub enum PolicyViolation {
    #[error("HTTP access denied for URL '{url}': no matching pattern")]
    HttpDenied { url: String },

    #[error("filesystem access denied for path '{path}': outside allowed prefixes")]
    FilesystemDenied { path: String },

    #[error("process execution denied for command '{command}': not in allowlist")]
    ProcessDenied { command: String },

    #[error("environment variable '{var}' not in allowlist")]
    EnvironmentDenied { var: String },

    #[error("raw socket access not permitted")]
    SocketsDenied,

    #[error("{capability} not permitted by manifest")]
    CapabilityDenied { capability: String },
}
