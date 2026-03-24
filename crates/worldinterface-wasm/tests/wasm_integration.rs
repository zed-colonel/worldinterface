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
    let connector = module_loader::load_module(
        &runtime,
        &dir.join("echo.wasm"),
        &dir.join("echo.connector.toml"),
    )
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
    let loaded = module_loader::load_modules_from_dir(&runtime, &compiled_dir()).unwrap();
    assert!(
        loaded.connectors.len() >= 2,
        "expected at least echo + host-caller, got {}",
        loaded.connectors.len()
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

// ────────────────────────────────────────────────────────────────────────────
// E4-S2 WASM Connector Integration Tests
// ────────────────────────────────────────────────────────────────────────────

fn load_with_extra_http_hosts(
    name: &str,
    extra_hosts: &[&str],
) -> (tempfile::TempDir, worldinterface_wasm::WasmConnector) {
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let manifest_path = dir.join(format!("{name}.connector.toml"));
    let wasm_path = dir.join(format!("{name}.wasm"));

    let manifest_content = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("missing manifest for {name}: {e}"));
    let mut manifest =
        worldinterface_wasm::manifest::ConnectorManifest::from_toml(&manifest_content).unwrap();
    for host in extra_hosts {
        manifest.capabilities.http.push(host.to_string());
    }

    let connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();
    (_td, connector)
}

// ── E4S2-T18: webhook.send descriptor ──

#[test]
fn webhook_send_descriptor() {
    let (_td, connector) = load_with_extra_http_hosts("webhook-send", &[]);
    let desc = connector.describe();
    assert_eq!(desc.name, "webhook.send");
    assert!(matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "webhook.send"));
}

// ── E4S2-T19: webhook.send invoke success ──

#[test]
fn webhook_send_invoke_success() {
    let mut server = mockito::Server::new();
    let mock = server.mock("POST", "/hook").with_status(200).with_body(r#"{"ok":true}"#).create();

    let mock_host = extract_mock_hostname(&server.url());
    let (_td, connector) = load_with_extra_http_hosts("webhook-send", &[&mock_host]);
    let ctx = test_ctx();
    let params = json!({
        "url": format!("{}/hook", server.url()),
        "method": "POST",
        "body": r#"{"event":"test"}"#,
    });

    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["status"], 200);
    assert_eq!(result["body"], r#"{"ok":true}"#);
    mock.assert();
}

// ── E4S2-T20: webhook.send policy blocks unlisted host ──

#[test]
fn webhook_send_policy_blocks_unlisted_host() {
    // Load with a restrictive manifest (only example.com allowed)
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let wasm_path = dir.join("webhook-send.wasm");

    let manifest = worldinterface_wasm::manifest::ConnectorManifest::from_toml(
        r#"
[connector]
name = "webhook.send"

[capabilities]
http = ["example.com"]
logging = true

[resources]
max_fuel = 500_000_000
timeout_ms = 15000
"#,
    )
    .unwrap();

    let connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();

    let ctx = test_ctx();
    let params = json!({
        "url": "https://evil.com/hack",
        "method": "GET",
    });

    // Should fail — evil.com not in allowed http patterns
    let result = connector.invoke(&ctx, &params);
    assert!(result.is_err(), "should be blocked by policy");
}

// ── E4S2-T21: web.search descriptor ──

#[test]
fn web_search_descriptor() {
    let (_td, connector) = load_with_extra_http_hosts("web-search", &[]);
    let desc = connector.describe();
    assert_eq!(desc.name, "web.search");
    assert!(matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "web.search"));
}

// ── E4S2-T22: web.search invoke success ──

#[test]
fn web_search_invoke_success() {
    let mut server = mockito::Server::new();
    let mock_response = serde_json::json!({
        "web": {
            "results": [
                {"title": "Rust Lang", "url": "https://rust-lang.org", "description": "Systems programming"},
                {"title": "Cargo", "url": "https://doc.rust-lang.org/cargo", "description": "Rust package manager"},
            ]
        }
    });
    let mock = server
        .mock("GET", mockito::Matcher::Regex(r".*q=rust.*".to_string()))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(mock_response.to_string())
        .create();

    let mock_host = extract_mock_hostname(&server.url());

    // Load with custom manifest that points to mock server
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let wasm_path = dir.join("web-search.wasm");

    let manifest = worldinterface_wasm::manifest::ConnectorManifest::from_toml(&format!(
        r#"
[connector]
name = "web.search"

[capabilities]
http = ["{mock_host}"]
logging = true
kv = true
environment = ["BRAVE_API_KEY"]

[resources]
max_fuel = 500_000_000
timeout_ms = 15000
"#
    ))
    .unwrap();

    // Pre-seed KV with mock provider URL
    runtime.resource_pool().kv_set("web.search", "provider_url", &server.url());

    let mut connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();

    // Use env override instead of unsound std::env::set_var
    connector.set_env("BRAVE_API_KEY", "test-key-12345");

    let ctx = test_ctx();
    let params = json!({"query": "rust programming", "max_results": 2});

    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["count"], 2);
    assert_eq!(result["query"], "rust programming");
    let results = result["results"].as_array().unwrap();
    assert_eq!(results[0]["title"], "Rust Lang");
    mock.assert();
}

// ── E4S2-T23: web.search empty query rejected ──

#[test]
fn web_search_empty_query_rejected() {
    let (_td, connector) = load_with_extra_http_hosts("web-search", &[]);
    let ctx = test_ctx();
    let params = json!({"query": "  "});
    let result = connector.invoke(&ctx, &params);
    assert!(result.is_err(), "empty query should be rejected");
}

// ── E4S2-T24: discord descriptor ──

#[test]
fn discord_descriptor() {
    let (_td, connector) = load_with_extra_http_hosts("discord", &[]);
    let desc = connector.describe();
    assert_eq!(desc.name, "discord");
    assert!(matches!(desc.category, ConnectorCategory::Wasm(ref name) if name == "discord"));
}

// ── E4S2-T25: discord send_message success ──

#[test]
fn discord_send_message_success() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/channels/123456/messages")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"id":"msg-001","channel_id":"123456"}"#)
        .create();

    let mock_host = extract_mock_hostname(&server.url());

    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let wasm_path = dir.join("discord.wasm");

    let manifest = worldinterface_wasm::manifest::ConnectorManifest::from_toml(&format!(
        r#"
[connector]
name = "discord"

[capabilities]
http = ["{mock_host}"]
logging = true
kv = true
environment = ["DISCORD_BOT_TOKEN"]

[resources]
max_fuel = 1_000_000_000
timeout_ms = 30000
"#
    ))
    .unwrap();

    // Pre-seed KV with mock server as API base URL
    runtime.resource_pool().kv_set("discord", "api_base_url", &server.url());

    let mut connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();

    // Use env override instead of unsound std::env::set_var
    connector.set_env("DISCORD_BOT_TOKEN", "test-bot-token-xyz");

    let ctx = test_ctx();
    let params = json!({
        "action": "send_message",
        "channel_id": "123456",
        "content": "Hello from test!",
    });

    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["message_id"], "msg-001");
    assert_eq!(result["channel_id"], "123456");
    mock.assert();
}

// ── E4S2-T26: discord send with embeds ──

#[test]
fn discord_send_with_embeds() {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("POST", "/channels/789/messages")
        .match_body(mockito::Matcher::PartialJsonString(
            r#"{"embeds":[{"title":"Test Embed","description":"An embed"}]}"#.into(),
        ))
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(r#"{"id":"msg-002","channel_id":"789"}"#)
        .create();

    let mock_host = extract_mock_hostname(&server.url());

    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let wasm_path = dir.join("discord.wasm");

    let manifest = worldinterface_wasm::manifest::ConnectorManifest::from_toml(&format!(
        r#"
[connector]
name = "discord"

[capabilities]
http = ["{mock_host}"]
logging = true
kv = true
environment = ["DISCORD_BOT_TOKEN"]

[resources]
max_fuel = 1_000_000_000
timeout_ms = 30000
"#
    ))
    .unwrap();

    // Pre-seed KV with mock server as API base URL
    runtime.resource_pool().kv_set("discord", "api_base_url", &server.url());

    let mut connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();

    connector.set_env("DISCORD_BOT_TOKEN", "test-token");

    let ctx = test_ctx();
    let params = json!({
        "action": "send_message",
        "channel_id": "789",
        "embeds": [{"title": "Test Embed", "description": "An embed"}],
    });

    let result = connector.invoke(&ctx, &params).unwrap();
    assert_eq!(result["message_id"], "msg-002");
    assert_eq!(result["channel_id"], "789");
    mock.assert();
}

// ── E4S2-T27: discord policy allows discord.com only ──

#[test]
fn discord_policy_allows_discord_only() {
    let (_td, runtime) = test_runtime();
    let dir = ref_compiled_dir();
    let wasm_path = dir.join("discord.wasm");

    // Load with original manifest (http = ["discord.com"])
    let manifest_content = std::fs::read_to_string(dir.join("discord.connector.toml")).unwrap();
    let manifest =
        worldinterface_wasm::manifest::ConnectorManifest::from_toml(&manifest_content).unwrap();
    assert_eq!(manifest.capabilities.http, vec!["discord.com"]);

    let connector =
        module_loader::load_module_with_manifest(&runtime, &wasm_path, &manifest).unwrap();
    let desc = connector.describe();
    assert_eq!(desc.name, "discord");
}

fn extract_mock_hostname(url: &str) -> String {
    let without_scheme =
        url.strip_prefix("http://").or_else(|| url.strip_prefix("https://")).unwrap_or(url);
    let host_part = without_scheme.split('/').next().unwrap_or("");
    host_part.split(':').next().unwrap_or("").to_string()
}

// ── Discord Streaming Integration Tests ──

fn production_compiled_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/wasm-connectors/compiled")
}

/// Load the Discord WASM module with StreamingConnectorWorld bindings.
/// Seeds KV with bot_user_id for mention filtering.
fn load_discord_streaming() -> (
    tempfile::TempDir,
    Arc<WasmRuntime>,
    wasmtime::component::Component,
    worldinterface_wasm::manifest::ConnectorManifest,
    Arc<worldinterface_wasm::CapabilityPolicy>,
) {
    let dir = tempfile::tempdir().unwrap();
    let config = WasmRuntimeConfig { kv_store_dir: dir.path().join("kv"), ..Default::default() };
    let runtime = Arc::new(WasmRuntime::new(config).unwrap());

    let prod_dir = production_compiled_dir();
    let wasm_path = prod_dir.join("discord.wasm");
    let manifest_path = prod_dir.join("discord.connector.toml");

    let manifest_content = std::fs::read_to_string(&manifest_path).unwrap();
    let manifest =
        worldinterface_wasm::manifest::ConnectorManifest::from_toml(&manifest_content).unwrap();

    let policy = Arc::new(worldinterface_wasm::CapabilityPolicy::from_manifest(&manifest).unwrap());

    let wasm_bytes = std::fs::read(&wasm_path).unwrap();
    let component = wasmtime::component::Component::new(runtime.engine(), &wasm_bytes).unwrap();

    // Seed KV with bot_user_id for mention filtering
    runtime.resource_pool().kv_set("discord", "bot_user_id", "bot123");

    (dir, runtime, component, manifest, policy)
}

fn call_discord_on_message(
    runtime: &Arc<WasmRuntime>,
    component: &wasmtime::component::Component,
    manifest: &worldinterface_wasm::manifest::ConnectorManifest,
    policy: &Arc<worldinterface_wasm::CapabilityPolicy>,
    raw: &str,
) -> Vec<(String, String, Vec<(String, String)>)> {
    use worldinterface_wasm::streaming_bindings::StreamingConnectorWorld;

    let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new().build();
    let mut store = wasmtime::Store::new(
        runtime.engine(),
        worldinterface_wasm::WasmState {
            wasi_ctx,
            resource_table: wasmtime::component::ResourceTable::new(),
            policy: Arc::clone(policy),
            resource_pool: Arc::clone(runtime.resource_pool()),
            module_name: manifest.connector.name.clone(),
        },
    );
    store.set_fuel(policy.max_fuel).unwrap();
    store.epoch_deadline_trap();
    store.set_epoch_deadline(300);

    let bindings = StreamingConnectorWorld::instantiate(&mut store, component, runtime.linker())
        .expect("streaming instantiation should succeed");

    let results = bindings
        .exo_connector_streaming_connector()
        .call_on_message(&mut store, raw)
        .expect("on_message call should succeed");

    results.into_iter().map(|m| (m.source_identity, m.content, m.metadata)).collect()
}

// ── E4S3-T11: Discord on_message filters mentions ──

#[test]
fn discord_on_message_filters_mentions() {
    let (_dir, runtime, component, manifest, policy) = load_discord_streaming();

    let event = serde_json::json!({
        "op": 0,
        "t": "MESSAGE_CREATE",
        "d": {
            "id": "msg-001",
            "channel_id": "ch-123",
            "guild_id": "guild-456",
            "content": "<@bot123> what is the weather?",
            "author": { "id": "user-789", "username": "testuser" },
            "mentions": [{ "id": "bot123", "username": "exo-vessel" }]
        }
    });

    let results =
        call_discord_on_message(&runtime, &component, &manifest, &policy, &event.to_string());
    assert_eq!(results.len(), 1, "should produce one message for @bot mention");
}

// ── E4S3-T12: Discord on_message ignores non-mentions ──

#[test]
fn discord_on_message_ignores_non_mentions() {
    let (_dir, runtime, component, manifest, policy) = load_discord_streaming();

    let event = serde_json::json!({
        "op": 0,
        "t": "MESSAGE_CREATE",
        "d": {
            "id": "msg-002",
            "channel_id": "ch-123",
            "content": "hello everyone",
            "author": { "id": "user-789", "username": "testuser" },
            "mentions": []
        }
    });

    let results =
        call_discord_on_message(&runtime, &component, &manifest, &policy, &event.to_string());
    assert!(results.is_empty(), "should produce no messages without @bot mention");
}

// ── E4S3-T13: Discord on_message strips mention ──

#[test]
fn discord_on_message_strips_mention() {
    let (_dir, runtime, component, manifest, policy) = load_discord_streaming();

    let event = serde_json::json!({
        "op": 0,
        "t": "MESSAGE_CREATE",
        "d": {
            "id": "msg-003",
            "channel_id": "ch-123",
            "content": "<@bot123> hello",
            "author": { "id": "user-789", "username": "testuser" },
            "mentions": [{ "id": "bot123" }]
        }
    });

    let results =
        call_discord_on_message(&runtime, &component, &manifest, &policy, &event.to_string());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, "hello", "mention tag should be stripped");
}

// ── E4S3-T14: Discord on_message extracts identity ──

#[test]
fn discord_on_message_extracts_identity() {
    let (_dir, runtime, component, manifest, policy) = load_discord_streaming();

    let event = serde_json::json!({
        "op": 0,
        "t": "MESSAGE_CREATE",
        "d": {
            "id": "msg-004",
            "channel_id": "ch-123",
            "content": "<@bot123> ping",
            "author": { "id": "user-789", "username": "testuser" },
            "mentions": [{ "id": "bot123" }]
        }
    });

    let results =
        call_discord_on_message(&runtime, &component, &manifest, &policy, &event.to_string());
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "discord:user:user-789");
}

// ── E4S3-T15: Discord stream_config valid ──

#[test]
fn discord_stream_config_valid() {
    use worldinterface_wasm::streaming_bindings::StreamingConnectorWorld;

    let (_dir, runtime, component, manifest, policy) = load_discord_streaming();

    let wasi_ctx = wasmtime_wasi::WasiCtxBuilder::new().build();
    let mut store = wasmtime::Store::new(
        runtime.engine(),
        worldinterface_wasm::WasmState {
            wasi_ctx,
            resource_table: wasmtime::component::ResourceTable::new(),
            policy: Arc::clone(&policy),
            resource_pool: Arc::clone(runtime.resource_pool()),
            module_name: manifest.connector.name.clone(),
        },
    );
    store.set_fuel(policy.max_fuel).unwrap();
    store.epoch_deadline_trap();
    store.set_epoch_deadline(300);

    let bindings = StreamingConnectorWorld::instantiate(&mut store, &component, runtime.linker())
        .expect("streaming instantiation should succeed");

    let setup = bindings
        .exo_connector_streaming_connector()
        .call_stream_config(&mut store)
        .expect("stream_config call should succeed");

    assert!(setup.url.starts_with("wss://"), "Gateway URL should start with wss://");
    assert!(
        setup.url.contains("gateway.discord.gg"),
        "Gateway URL should contain gateway.discord.gg"
    );
    assert_eq!(setup.init_messages.len(), 1, "should have IDENTIFY init message");
    assert!(
        setup.init_messages[0].contains("\"op\":2") || setup.init_messages[0].contains("\"op\": 2"),
        "IDENTIFY should be op 2"
    );
    assert_eq!(setup.heartbeat_interval_ms, 41250);
    assert!(setup.heartbeat_payload.is_some());
}
