//! ConnectorManifest — parsed from `.connector.toml` sidecar files.
//!
//! Defines what a WASM module is and what capabilities it requests.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::ManifestError;

/// Parsed from a `.connector.toml` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorManifest {
    pub connector: ManifestConnector,
    #[serde(default)]
    pub capabilities: ManifestCapabilities,
    #[serde(default)]
    pub resources: ManifestResources,
}

/// Identity section of the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestConnector {
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub description: String,
    /// Whether this connector exports the streaming-connector interface.
    /// Streaming connectors get host-managed background WebSocket connections.
    /// Default: false.
    #[serde(default)]
    pub streaming: bool,
}

/// Capabilities section — deny-by-default. Empty = no permissions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestCapabilities {
    /// URL hostname patterns for HTTP access. Glob patterns: "*.github.com"
    #[serde(default)]
    pub http: Vec<String>,
    /// Filesystem path prefixes (prefix match, absolute paths only)
    #[serde(default)]
    pub filesystem: Vec<PathBuf>,
    /// Command allowlist for process execution (exact match on command name)
    #[serde(default)]
    pub process: Vec<String>,
    /// Environment variable allowlist (exact match on var name)
    #[serde(default)]
    pub environment: Vec<String>,
    /// Raw socket access (empty = denied). Reserved for future use.
    #[serde(default)]
    pub sockets: Vec<String>,
    /// Per-module KV store access
    #[serde(default)]
    pub kv: bool,
    /// Structured logging access
    #[serde(default)]
    pub logging: bool,
    /// Crypto operations access
    #[serde(default)]
    pub crypto: bool,
    /// WebSocket URL hostname patterns (same matching as HTTP patterns).
    /// Empty = WebSocket access denied (deny-by-default).
    #[serde(default)]
    pub websocket: Vec<String>,
}

fn default_max_fuel() -> u64 {
    1_000_000_000
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_max_memory() -> usize {
    67_108_864 // 64MB
}

fn default_max_http() -> usize {
    5
}

/// Resource limits section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestResources {
    /// wasmtime fuel budget (default: 1_000_000_000)
    #[serde(default = "default_max_fuel")]
    pub max_fuel: u64,
    /// Wall-clock execution timeout in ms (default: 30_000)
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// Maximum WASM linear memory in bytes (default: 64MB)
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: usize,
    /// Maximum concurrent HTTP requests (default: 5)
    #[serde(default = "default_max_http")]
    pub max_http_concurrent: usize,
}

impl Default for ManifestResources {
    fn default() -> Self {
        Self {
            max_fuel: default_max_fuel(),
            timeout_ms: default_timeout_ms(),
            max_memory_bytes: default_max_memory(),
            max_http_concurrent: default_max_http(),
        }
    }
}

impl ConnectorManifest {
    /// Parse a manifest from TOML string content.
    pub fn from_toml(content: &str) -> Result<Self, ManifestError> {
        let manifest: Self =
            toml::from_str(content).map_err(|e| ManifestError::Parse(e.to_string()))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate the manifest after parsing.
    pub fn validate(&self) -> Result<(), ManifestError> {
        // connector.name must be non-empty and contain only [a-z0-9._-]
        if self.connector.name.is_empty() {
            return Err(ManifestError::Validation("connector.name must not be empty".into()));
        }
        if !self.connector.name.chars().all(|c| {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
        }) {
            return Err(ManifestError::Validation(format!(
                "connector.name '{}' contains invalid characters (allowed: [a-z0-9._-])",
                self.connector.name
            )));
        }

        // filesystem paths must be absolute
        for path in &self.capabilities.filesystem {
            if !path.is_absolute() {
                return Err(ManifestError::Validation(format!(
                    "filesystem path '{}' must be absolute",
                    path.display()
                )));
            }
        }

        // resources must be positive
        if self.resources.max_fuel == 0 {
            return Err(ManifestError::Validation("resources.max_fuel must be > 0".into()));
        }
        if self.resources.timeout_ms == 0 {
            return Err(ManifestError::Validation("resources.timeout_ms must be > 0".into()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── E2S3-T1: Manifest: valid TOML parses all fields ──

    #[test]
    fn manifest_valid_toml_parses_all_fields() {
        let toml = r#"
[connector]
name = "test.echo"
version = "0.1.0"
description = "Echo connector for testing"

[capabilities]
http = ["api.example.com", "*.github.com"]
filesystem = ["/tmp/data"]
process = ["echo", "cat"]
environment = ["HOME", "PATH"]
kv = true
logging = true
crypto = true

[resources]
max_fuel = 500_000_000
timeout_ms = 10000
max_memory_bytes = 33554432
max_http_concurrent = 3
"#;
        let manifest = ConnectorManifest::from_toml(toml).unwrap();
        assert_eq!(manifest.connector.name, "test.echo");
        assert_eq!(manifest.connector.version, "0.1.0");
        assert_eq!(manifest.connector.description, "Echo connector for testing");
        assert_eq!(manifest.capabilities.http, vec!["api.example.com", "*.github.com"]);
        assert_eq!(manifest.capabilities.filesystem, vec![PathBuf::from("/tmp/data")]);
        assert_eq!(manifest.capabilities.process, vec!["echo", "cat"]);
        assert_eq!(manifest.capabilities.environment, vec!["HOME", "PATH"]);
        assert!(manifest.capabilities.kv);
        assert!(manifest.capabilities.logging);
        assert!(manifest.capabilities.crypto);
        assert_eq!(manifest.resources.max_fuel, 500_000_000);
        assert_eq!(manifest.resources.timeout_ms, 10000);
        assert_eq!(manifest.resources.max_memory_bytes, 33_554_432);
        assert_eq!(manifest.resources.max_http_concurrent, 3);
    }

    // ── E2S3-T2: Manifest: missing [connector] section → error ──

    #[test]
    fn manifest_missing_connector_section_errors() {
        let toml = r#"
[capabilities]
http = ["api.example.com"]
"#;
        let result = ConnectorManifest::from_toml(toml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ManifestError::Parse(_)), "expected Parse error, got: {err}");
    }

    // ── E2S3-T3: Manifest: empty capabilities → deny-all policy ──

    #[test]
    fn manifest_empty_capabilities_is_deny_all() {
        let toml = r#"
[connector]
name = "test.minimal"
"#;
        let manifest = ConnectorManifest::from_toml(toml).unwrap();
        assert!(manifest.capabilities.http.is_empty());
        assert!(manifest.capabilities.filesystem.is_empty());
        assert!(manifest.capabilities.process.is_empty());
        assert!(manifest.capabilities.environment.is_empty());
        assert!(manifest.capabilities.sockets.is_empty());
        assert!(!manifest.capabilities.kv);
        assert!(!manifest.capabilities.logging);
        assert!(!manifest.capabilities.crypto);
    }

    #[test]
    fn manifest_invalid_name_rejected() {
        let toml = r#"
[connector]
name = "Test Module!"
"#;
        let result = ConnectorManifest::from_toml(toml);
        assert!(
            matches!(result, Err(ManifestError::Validation(msg)) if msg.contains("invalid characters"))
        );
    }

    #[test]
    fn manifest_empty_name_rejected() {
        let toml = r#"
[connector]
name = ""
"#;
        let result = ConnectorManifest::from_toml(toml);
        assert!(
            matches!(result, Err(ManifestError::Validation(msg)) if msg.contains("must not be empty"))
        );
    }

    #[test]
    fn manifest_relative_filesystem_path_rejected() {
        let toml = r#"
[connector]
name = "test.bad"

[capabilities]
filesystem = ["relative/path"]
"#;
        let result = ConnectorManifest::from_toml(toml);
        assert!(
            matches!(result, Err(ManifestError::Validation(msg)) if msg.contains("must be absolute"))
        );
    }

    #[test]
    fn manifest_zero_fuel_rejected() {
        let toml = r#"
[connector]
name = "test.bad"

[resources]
max_fuel = 0
"#;
        let result = ConnectorManifest::from_toml(toml);
        assert!(matches!(result, Err(ManifestError::Validation(msg)) if msg.contains("max_fuel")));
    }

    // ── E4S3-T3: Manifest: streaming defaults to false ──

    #[test]
    fn manifest_streaming_default_false() {
        let toml = r#"
[connector]
name = "test.basic"
"#;
        let manifest = ConnectorManifest::from_toml(toml).unwrap();
        assert!(!manifest.connector.streaming);
    }

    // ── E4S3-T4: Manifest: streaming = true parsed correctly ──

    #[test]
    fn manifest_streaming_true_parsed() {
        let toml = r#"
[connector]
name = "test.streamer"
streaming = true
"#;
        let manifest = ConnectorManifest::from_toml(toml).unwrap();
        assert!(manifest.connector.streaming);
    }

    #[test]
    fn manifest_defaults_applied() {
        let toml = r#"
[connector]
name = "test.defaults"
"#;
        let manifest = ConnectorManifest::from_toml(toml).unwrap();
        assert_eq!(manifest.resources.max_fuel, 1_000_000_000);
        assert_eq!(manifest.resources.timeout_ms, 30_000);
        assert_eq!(manifest.resources.max_memory_bytes, 67_108_864);
        assert_eq!(manifest.resources.max_http_concurrent, 5);
    }
}
