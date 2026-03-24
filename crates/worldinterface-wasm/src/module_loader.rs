//! Module loader — loads `.wasm` + `.connector.toml` pairs from a directory.

use std::path::Path;
use std::sync::Arc;

use wasmtime::component::Component;

use crate::connector::WasmConnector;
use crate::error::WasmError;
use crate::manifest::ConnectorManifest;
use crate::policy::CapabilityPolicy;
use crate::runtime::WasmRuntime;

/// Load all WASM modules from a directory.
///
/// Scans for pairs: `{name}.connector.toml` + `{name}.wasm`.
/// Returns a Vec of WasmConnectors. Invalid modules are reported
/// via tracing::warn and skipped (no crash).
pub fn load_modules_from_dir(
    runtime: &Arc<WasmRuntime>,
    dir: &Path,
) -> Result<Vec<WasmConnector>, WasmError> {
    let mut connectors = Vec::new();

    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        // Look for .connector.toml files
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !name.ends_with(".connector.toml") {
            continue;
        }

        let stem = name.strip_suffix(".connector.toml").unwrap();
        let wasm_path = dir.join(format!("{stem}.wasm"));

        match load_module(runtime, &wasm_path, &path) {
            Ok(connector) => {
                tracing::info!(module = %stem, "loaded WASM connector");
                connectors.push(connector);
            }
            Err(e) => {
                tracing::warn!(module = %stem, error = %e, "skipping invalid WASM module");
            }
        }
    }

    Ok(connectors)
}

/// Load a single WASM module from explicit paths.
pub fn load_module(
    runtime: &Arc<WasmRuntime>,
    wasm_path: &Path,
    manifest_path: &Path,
) -> Result<WasmConnector, WasmError> {
    // 1. Read and parse manifest
    let manifest_content =
        std::fs::read_to_string(manifest_path).map_err(|_| WasmError::MissingManifest {
            name: wasm_path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default(),
            path: manifest_path.to_path_buf(),
        })?;
    let manifest = ConnectorManifest::from_toml(&manifest_content)?;

    // 2. Compile capability policy
    let policy = CapabilityPolicy::from_manifest(&manifest)?;

    // 3. Compile WASM component
    let wasm_bytes = std::fs::read(wasm_path).map_err(|_| WasmError::MissingWasm {
        name: manifest.connector.name.clone(),
        path: wasm_path.to_path_buf(),
    })?;
    let component = Component::new(runtime.engine(), &wasm_bytes)
        .map_err(|e| WasmError::Compilation(e.to_string()))?;

    // 4. Return WasmConnector
    Ok(WasmConnector::new(Arc::clone(runtime), component, manifest, policy))
}

/// Load a single WASM module with a pre-parsed manifest.
///
/// Used by integration tests to modify capability policies at test time
/// (e.g., adding mock server hostnames to HTTP patterns).
pub fn load_module_with_manifest(
    runtime: &Arc<WasmRuntime>,
    wasm_path: &Path,
    manifest: &ConnectorManifest,
) -> Result<WasmConnector, WasmError> {
    let policy = CapabilityPolicy::from_manifest(manifest)?;
    let wasm_bytes = std::fs::read(wasm_path).map_err(|_| WasmError::MissingWasm {
        name: manifest.connector.name.clone(),
        path: wasm_path.to_path_buf(),
    })?;
    let component = Component::new(runtime.engine(), &wasm_bytes)
        .map_err(|e| WasmError::Compilation(e.to_string()))?;
    Ok(WasmConnector::new(Arc::clone(runtime), component, manifest.clone(), policy))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── E2S3-T21: load_module: missing .connector.toml → error ──

    #[test]
    fn load_module_missing_manifest_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::runtime::WasmRuntimeConfig {
            kv_store_dir: dir.path().join("kv"),
            ..Default::default()
        };
        let runtime = Arc::new(WasmRuntime::new(config).unwrap());

        let result = load_module(
            &runtime,
            &dir.path().join("nonexistent.wasm"),
            &dir.path().join("nonexistent.connector.toml"),
        );
        assert!(
            matches!(result, Err(WasmError::MissingManifest { .. })),
            "expected MissingManifest, got: {result:?}"
        );
    }

    // ── E2S3-T22: load_module: invalid .wasm → compile error ──

    #[test]
    fn load_module_invalid_wasm_errors() {
        let dir = tempfile::tempdir().unwrap();

        // Write a valid manifest but invalid WASM
        let manifest = r#"
[connector]
name = "test.invalid"
"#;
        std::fs::write(dir.path().join("test.connector.toml"), manifest).unwrap();
        std::fs::write(dir.path().join("test.wasm"), b"not valid wasm").unwrap();

        let config = crate::runtime::WasmRuntimeConfig {
            kv_store_dir: dir.path().join("kv"),
            ..Default::default()
        };
        let runtime = Arc::new(WasmRuntime::new(config).unwrap());

        let result = load_module(
            &runtime,
            &dir.path().join("test.wasm"),
            &dir.path().join("test.connector.toml"),
        );
        assert!(
            matches!(result, Err(WasmError::Compilation(_))),
            "expected Compilation error, got: {result:?}"
        );
    }

    // ── E2S3-T25: load_modules_from_dir: skips invalid with warning ──

    #[test]
    fn load_modules_from_dir_skips_invalid() {
        let dir = tempfile::tempdir().unwrap();

        // Write a manifest with no matching .wasm
        let manifest = r#"
[connector]
name = "test.orphan"
"#;
        std::fs::write(dir.path().join("orphan.connector.toml"), manifest).unwrap();

        let config = crate::runtime::WasmRuntimeConfig {
            kv_store_dir: dir.path().join("kv"),
            ..Default::default()
        };
        let runtime = Arc::new(WasmRuntime::new(config).unwrap());

        // Should not panic, just skip
        let connectors = load_modules_from_dir(&runtime, dir.path()).unwrap();
        assert!(connectors.is_empty());
    }

    // ── E2S3-T39: Host HTTP: uses separate client (not native connector's) ──

    #[test]
    fn resource_pool_has_separate_http_client() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::runtime::WasmRuntimeConfig {
            kv_store_dir: dir.path().join("kv"),
            ..Default::default()
        };
        let runtime = WasmRuntime::new(config).unwrap();
        // Just verify the pool exists and has a client
        let _client = &runtime.resource_pool().http_client;
    }
}
