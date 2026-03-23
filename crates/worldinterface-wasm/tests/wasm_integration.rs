//! Integration tests for WASM module loading and invocation.
//!
//! These tests require compiled WASM test modules. Gate behind `wasm-tests` feature.

#![cfg(feature = "wasm-tests")]

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::json;
use uuid::Uuid;
use worldinterface_connector::context::{CancellationToken, InvocationContext};
use worldinterface_connector::traits::Connector;
use worldinterface_core::descriptor::ConnectorCategory;
use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};
use worldinterface_wasm::module_loader;
use worldinterface_wasm::runtime::{WasmRuntime, WasmRuntimeConfig};

fn compiled_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test-modules/compiled")
}

fn test_runtime() -> (tempfile::TempDir, Arc<WasmRuntime>) {
    let dir = tempfile::tempdir().unwrap();
    let config = WasmRuntimeConfig { kv_store_dir: dir.path().join("kv"), ..Default::default() };
    let runtime = Arc::new(WasmRuntime::new(config).unwrap());
    (dir, runtime)
}

fn test_ctx() -> InvocationContext {
    InvocationContext {
        flow_run_id: FlowRunId::new(),
        node_id: NodeId::new(),
        step_run_id: StepRunId::new(),
        run_id: Uuid::new_v4(),
        attempt_id: Uuid::new_v4(),
        attempt_number: 1,
        cancellation: CancellationToken::new(),
    }
}

fn load_echo() -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = compiled_dir();
    let connector =
        module_loader::load_module(&runtime, &dir.join("echo.wasm"), &dir.join("echo.connector.toml"))
            .expect("echo module should load");
    (_td, connector)
}

fn load_stress() -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = compiled_dir();
    let connector = module_loader::load_module(
        &runtime,
        &dir.join("stress.wasm"),
        &dir.join("stress.connector.toml"),
    )
    .expect("stress module should load");
    (_td, connector)
}

fn load_host_caller() -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = compiled_dir();
    let connector = module_loader::load_module(
        &runtime,
        &dir.join("host-caller.wasm"),
        &dir.join("host-caller.connector.toml"),
    )
    .expect("host-caller module should load");
    (_td, connector)
}

// ── E2S3-T20: load_module: valid .wasm + .connector.toml → WasmConnector ──

#[test]
fn load_valid_module() {
    let (_dir, _connector) = load_echo();
}

// ── E2S3-T24: load_modules_from_dir: loads all valid modules ──

#[test]
fn load_modules_from_dir_loads_all() {
    let (_td, runtime) = test_runtime();
    let connectors = module_loader::load_modules_from_dir(&runtime, &compiled_dir()).unwrap();
    assert!(
        connectors.len() >= 2,
        "expected at least echo + host-caller, got {}",
        connectors.len()
    );
}

// ── E2S3-T26: WasmConnector::describe returns manifest data ──

#[test]
fn wasm_connector_describe() {
    let (_dir, connector) = load_echo();
    let desc = connector.describe();
    assert_eq!(desc.name, "test.echo");
    assert!(matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "test.echo"));
    assert!(!desc.description.is_empty());
}

// ── E2S3-T27: WasmConnector::invoke calls guest invoke() ──

#[test]
fn wasm_connector_invoke_echo() {
    let (_dir, connector) = load_echo();
    let ctx = test_ctx();
    let params = json!({"hello": "world"});
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result, params);
}

// ── E2S3-T28: WasmConnector::invoke returns guest result as Value ──

#[test]
fn wasm_connector_invoke_returns_value() {
    let (_dir, connector) = load_echo();
    let ctx = test_ctx();
    let params = json!({"numbers": [1, 2, 3], "nested": {"a": true}});
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["numbers"], json!([1, 2, 3]));
    assert_eq!(result["nested"]["a"], json!(true));
}

// ── E2S3-T29: WasmConnector::invoke guest error → ConnectorError::Terminal ──

#[test]
fn wasm_connector_guest_error() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    // Send unknown action which triggers Err from guest
    let params = json!({"action": "unknown"});
    let result = connector.invoke(&ctx, &params);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, worldinterface_connector::error::ConnectorError::Terminal(ref msg) if msg.contains("unknown")),
        "expected Terminal error with 'unknown', got: {err:?}"
    );
}

// ── E2S3-T30: WasmConnector: cancellation checked before and after ──

#[test]
fn wasm_connector_cancellation_before() {
    let (_dir, connector) = load_echo();
    let ctx = test_ctx();
    ctx.cancellation.cancel();
    let result = connector.invoke(&ctx, &json!({"test": true}));
    assert!(matches!(result, Err(worldinterface_connector::error::ConnectorError::Cancelled)));
}

// ── E2S3-T31: Host logging: log call produces tracing event ──

#[test]
fn host_logging() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "log"})).unwrap();
    assert_eq!(result["logged"], true);
}

// ── E2S3-T32: Host crypto: sha256 returns correct hex digest ──

#[test]
fn host_crypto_sha256() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "sha256"})).unwrap();
    let digest = result["digest"].as_str().unwrap();
    // SHA-256 of "hello world" (as bytes)
    assert_eq!(digest, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9");
}

// ── E2S3-T33: Host KV: get/set/delete/list operations ──

#[test]
fn host_kv_operations() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();

    // Set a key
    let result = connector.invoke(&ctx, &json!({"action": "kv_set"})).unwrap();
    assert_eq!(result["stored"], true);

    // Get the key
    let result = connector.invoke(&ctx, &json!({"action": "kv_get"})).unwrap();
    assert_eq!(result["value"], "test-value");

    // Delete the key
    let result = connector.invoke(&ctx, &json!({"action": "kv_delete"})).unwrap();
    assert_eq!(result["deleted"], true);

    // Get should return null now
    let result = connector.invoke(&ctx, &json!({"action": "kv_get"})).unwrap();
    assert_eq!(result["value"], serde_json::Value::Null);
}

// ── E2S3-T35: Host process: allowed command → executes, returns stdout ──

#[test]
fn host_process_allowed() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "exec"})).unwrap();
    assert_eq!(result["stdout"], "hello from process");
    assert_eq!(result["exit_code"], 0);
}

// ── E2S3-T48: WasmConnector invocation produces receipt via invoke_with_receipt ──

#[test]
fn wasm_connector_receipt() {
    use worldinterface_connector::receipt_gen::invoke_with_receipt;

    let (_dir, connector) = load_echo();
    let ctx = test_ctx();
    let params = json!({"receipt": "test"});

    let (result, receipt) = invoke_with_receipt(&connector, &ctx, &params);
    assert!(result.is_ok());
    assert_eq!(receipt.connector, "test.echo");
    assert_eq!(receipt.status, worldinterface_core::receipt::ReceiptStatus::Success);
    assert!(receipt.output_hash.is_some());
    assert!(!receipt.input_hash.is_empty());
}

// ── E2S3-T17: Fuel exhaustion → ConnectorError::Terminal ──

#[test]
fn wasm_fuel_exhaustion() {
    let (_dir, connector) = load_stress();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "loop"}));
    let err = result.unwrap_err();
    // Fuel exhaustion or epoch deadline — both produce Terminal errors
    assert!(
        matches!(&err, worldinterface_connector::error::ConnectorError::Terminal(_)),
        "expected Terminal error from fuel/epoch limit, got: {err:?}"
    );
}

// ── E2S3-T18: Wall-clock timeout (epoch) → ConnectorError::Terminal ──

#[test]
fn wasm_epoch_timeout() {
    // The stress module has timeout_ms=2000, so an infinite loop should
    // trigger within a few seconds via epoch deadline.
    let (_dir, connector) = load_stress();
    let ctx = test_ctx();
    let start = std::time::Instant::now();
    let result = connector.invoke(&ctx, &json!({"action": "loop"}));
    let elapsed = start.elapsed();

    let err = result.unwrap_err();
    assert!(
        matches!(&err, worldinterface_connector::error::ConnectorError::Terminal(_)),
        "expected Terminal error, got: {err:?}"
    );
    // Should complete within the timeout window (2s + some margin), not hang
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "should be bounded by timeout, elapsed: {elapsed:?}"
    );
}

// ── E2S3-T19: Memory limit enforced → trap ──

#[test]
fn wasm_memory_limit() {
    // The stress module has max_memory_bytes=16MB. Allocating 1MB chunks
    // in a loop should hit the limit and trap.
    let (_dir, connector) = load_stress();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "allocate"}));
    let err = result.unwrap_err();
    assert!(
        matches!(&err, worldinterface_connector::error::ConnectorError::Terminal(_)),
        "expected Terminal error from memory limit, got: {err:?}"
    );
}

// ── E2S3-T23: Module missing required exports → error ──

#[test]
fn wasm_module_missing_exports() {
    // Create a minimal valid WASM module that doesn't export the connector interface
    let dir = tempfile::tempdir().unwrap();
    let manifest = r#"
[connector]
name = "test.bad-exports"
"#;
    std::fs::write(dir.path().join("bad.connector.toml"), manifest).unwrap();

    // Write a minimal valid WASM component (empty component)
    // WAT: (component)
    let wat = "(component)";
    let wasm = wat::parse_str(wat).expect("valid WAT");
    std::fs::write(dir.path().join("bad.wasm"), &wasm).unwrap();

    let (_td2, runtime) = test_runtime();
    let result = worldinterface_wasm::load_module(
        &runtime,
        &dir.path().join("bad.wasm"),
        &dir.path().join("bad.connector.toml"),
    );
    // Should fail during instantiation or loading because connector export is missing
    // (the exact error varies — could be compilation or instantiation)
    assert!(
        result.is_err() || {
            // If load succeeds, invoke should fail
            let connector = result.unwrap();
            let ctx = test_ctx();
            connector.invoke(&ctx, &json!({})).is_err()
        }
    );
}

// ── E2S3-T34: KV per-module namespace isolation via WASM ──

#[test]
fn wasm_kv_namespace_isolation() {
    // Both connectors share the same runtime (and thus the same KV store).
    // Since they have the same module name ("test.host-caller"), they share namespace.
    let (_td, runtime) = test_runtime();
    let dir = compiled_dir();

    let caller1 = module_loader::load_module(
        &runtime,
        &dir.join("host-caller.wasm"),
        &dir.join("host-caller.connector.toml"),
    )
    .unwrap();
    let caller2 = module_loader::load_module(
        &runtime,
        &dir.join("host-caller.wasm"),
        &dir.join("host-caller.connector.toml"),
    )
    .unwrap();

    let ctx1 = test_ctx();
    let ctx2 = test_ctx();

    // Set via instance 1
    caller1.invoke(&ctx1, &json!({"action": "kv_set"})).unwrap();

    // Get via instance 2 — should see the same value (same module name = same namespace)
    let result = caller2.invoke(&ctx2, &json!({"action": "kv_get"})).unwrap();
    assert_eq!(result["value"], "test-value");

    // Cleanup
    caller1.invoke(&ctx1, &json!({"action": "kv_delete"})).unwrap();
}

// ── E2S3-T40: Host environment: allowed var → value returned ──

#[test]
fn wasm_environment_allowed_var() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "env"})).unwrap();
    // HOME is in the manifest's environment allowlist and is set via WASI context
    let home = result["home"].as_str().unwrap();
    assert!(!home.is_empty(), "HOME should be non-empty");
}

// ── E2S3-T41: Host environment: disallowed var → not visible ──

#[test]
fn wasm_environment_denied_var() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "env"})).unwrap();
    // SECRET_KEY is not in the manifest's environment allowlist
    let secret = result["secret"].as_str().unwrap();
    assert!(secret.is_empty(), "SECRET_KEY should be empty (not in allowlist)");
}

// ── E2S3-T42: Host random: returns bytes of requested length ──

#[test]
fn wasm_random_returns_bytes() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "random"})).unwrap();
    let hex = result["random"].as_str().unwrap();
    assert_eq!(result["len"], 16);
    assert_eq!(hex.len(), 32); // 16 bytes = 32 hex chars
                               // Should not be all zeros (astronomically unlikely for real random)
    assert_ne!(hex, "00000000000000000000000000000000");
}

// ── E2S3-T43: Host clocks: monotonic time accessible ──

#[test]
fn wasm_clocks_accessible() {
    let (_dir, connector) = load_host_caller();
    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "clock"})).unwrap();
    let epoch_secs = result["epoch_secs"].as_u64().unwrap();
    // Should be a reasonable epoch timestamp (after 2020)
    assert!(epoch_secs > 1_577_836_800, "epoch_secs should be recent: {epoch_secs}");
}

// ── E2S3-T36: Host process: disallowed command → PolicyViolation trap ──

#[test]
fn wasm_process_denied() {
    // host-caller only allows "echo" in its manifest. Try to invoke with
    // an action that would call a different command — but the module hardcodes
    // "echo". Instead, test by loading with a restrictive manifest.
    let dir = compiled_dir();
    let (_td, runtime) = test_runtime();

    // Create a temporary manifest that allows NO process commands
    let tmp = tempfile::tempdir().unwrap();
    let manifest = r#"
[connector]
name = "test.host-caller"

[capabilities]
logging = true
crypto = true
kv = true
"#;
    std::fs::write(tmp.path().join("restricted.connector.toml"), manifest).unwrap();
    std::fs::copy(dir.join("host-caller.wasm"), tmp.path().join("restricted.wasm")).unwrap();

    let connector = worldinterface_wasm::load_module(
        &runtime,
        &tmp.path().join("restricted.wasm"),
        &tmp.path().join("restricted.connector.toml"),
    )
    .unwrap();

    let ctx = test_ctx();
    let result = connector.invoke(&ctx, &json!({"action": "exec"}));
    // Should fail because process capability is denied
    assert!(result.is_err(), "expected error from process policy denial, got: {result:?}");
}

// ══════════════════════════════════════════════════════════════════════════════
// Reference Connector Tests (Sprint E2-S4)
// ══════════════════════════════════════════════════════════════════════════════

fn ref_compiled_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/wasm-connectors/compiled")
}

fn load_json_validate() -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let connector = module_loader::load_module(
        &runtime,
        &dir.join("json-validate.wasm"),
        &dir.join("json-validate.connector.toml"),
    )
    .expect("json-validate module should load");
    (_td, connector)
}

fn load_host_demo() -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let connector = module_loader::load_module(
        &runtime,
        &dir.join("host-demo.wasm"),
        &dir.join("host-demo.connector.toml"),
    )
    .expect("host-demo module should load");
    (_td, connector)
}

// ── E2S4-T16: json-validate describe returns expected name ──

#[test]
fn json_validate_describe() {
    let (_dir, connector) = load_json_validate();
    let desc = connector.describe();
    assert_eq!(desc.name, "json.validate");
    assert!(matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "json.validate"));
    assert!(desc.description.contains("JSON"));
}

// ── E2S4-T17: json-validate valid document → {"valid": true} ──

#[test]
fn json_validate_valid_document() {
    let (_dir, connector) = load_json_validate();
    let ctx = test_ctx();
    let params = json!({
        "document": {"name": "test", "age": 25},
        "schema": {
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            }
        }
    });
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["valid"], true);
    assert_eq!(result["errors"].as_array().unwrap().len(), 0);
}

// ── E2S4-T18: json-validate invalid document → {"valid": false, "errors": [...]} ──

#[test]
fn json_validate_invalid_document() {
    let (_dir, connector) = load_json_validate();
    let ctx = test_ctx();
    let params = json!({
        "document": {"name": "test"},
        "schema": {
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"}
            }
        }
    });
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["valid"], false);
    let errors = result["errors"].as_array().unwrap();
    assert!(!errors.is_empty());
    assert!(errors.iter().any(|e| e.as_str().unwrap().contains("age")));
}

// ── E2S4-T19: json-validate missing document → guest error ──

#[test]
fn json_validate_missing_document() {
    let (_dir, connector) = load_json_validate();
    let ctx = test_ctx();
    let params = json!({"schema": {"type": "object"}});
    let result = connector.invoke(&ctx, &params);
    assert!(result.is_err());
}

// ── E2S4-T20: json-validate missing schema → guest error ──

#[test]
fn json_validate_missing_schema() {
    let (_dir, connector) = load_json_validate();
    let ctx = test_ctx();
    let params = json!({"document": {"a": 1}});
    let result = connector.invoke(&ctx, &params);
    assert!(result.is_err());
}

// ── E2S4-T21: json-validate receipt has connector = "json.validate" ──

#[test]
fn json_validate_receipt() {
    use worldinterface_connector::receipt_gen::invoke_with_receipt;

    let (_dir, connector) = load_json_validate();
    let ctx = test_ctx();
    let params = json!({
        "document": "hello",
        "schema": {"type": "string"}
    });

    let (result, receipt) = invoke_with_receipt(&connector, &ctx, &params);
    assert!(result.is_ok());
    assert_eq!(receipt.connector, "json.validate");
    assert_eq!(receipt.status, worldinterface_core::receipt::ReceiptStatus::Success);
}

// ── E2S4-T22: host-demo describe returns expected name ──

#[test]
fn host_demo_describe() {
    let (_dir, connector) = load_host_demo();
    let desc = connector.describe();
    assert_eq!(desc.name, "demo.host-functions");
    assert!(
        matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "demo.host-functions")
    );
}

// ── E2S4-T23: host-demo invoke returns SHA-256 digest ──

#[test]
fn host_demo_crypto() {
    let (_dir, connector) = load_host_demo();
    let ctx = test_ctx();
    let params = json!({"message": "hello world"});
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["message"], "hello world");
    // SHA-256 of "hello world"
    assert_eq!(
        result["sha256"].as_str().unwrap(),
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

// ── E2S4-T24: host-demo invoke stores and retrieves from KV ──

#[test]
fn host_demo_kv() {
    let (_dir, connector) = load_host_demo();
    let ctx = test_ctx();
    let params = json!({"message": "test-msg"});
    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["stored"], true);
}

// ── E2S4-T25: host-demo receipt has connector = "demo.host-functions" ──

#[test]
fn host_demo_receipt() {
    use worldinterface_connector::receipt_gen::invoke_with_receipt;

    let (_dir, connector) = load_host_demo();
    let ctx = test_ctx();
    let params = json!({"message": "receipt-test"});

    let (result, receipt) = invoke_with_receipt(&connector, &ctx, &params);
    assert!(result.is_ok());
    assert_eq!(receipt.connector, "demo.host-functions");
    assert_eq!(receipt.status, worldinterface_core::receipt::ReceiptStatus::Success);
}

// ── E2S4-T29: WasmRuntime persists across multiple invocations ──

#[test]
fn wasm_runtime_persists_across_invocations() {
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();

    let connector = module_loader::load_module(
        &runtime,
        &dir.join("json-validate.wasm"),
        &dir.join("json-validate.connector.toml"),
    )
    .unwrap();

    let ctx1 = test_ctx();
    let ctx2 = test_ctx();
    let params = json!({
        "document": "hello",
        "schema": {"type": "string"}
    });

    // Two invocations on the same connector/runtime
    let r1 = connector.invoke(&ctx1, &params).unwrap();
    let r2 = connector.invoke(&ctx2, &params).unwrap();
    assert_eq!(r1["valid"], true);
    assert_eq!(r2["valid"], true);
}
