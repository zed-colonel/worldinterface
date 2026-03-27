# WASM Connector Development Guide

How to build, test, and deploy WASM connectors for WorldInterface.

---

## Overview

A WASM connector is a WebAssembly module that implements the `exo:connector` WIT interface. It runs inside a wasmtime sandbox with deny-by-default capabilities — the connector can only access resources explicitly granted in its manifest.

**Two connector types:**
- **Standard** — implements `describe()` + `invoke()`. Stateless request/response.
- **Streaming** — additionally implements `configure()` + `parse_message()`. The host manages a persistent WebSocket connection and calls `parse_message()` for each incoming frame.

---

## Quick Start

### 1. Create the project

```bash
cargo init --lib my-connector
cd my-connector
```

Edit `Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.39"
serde_json = "1"
```

### 2. Generate bindings

Create `src/lib.rs`:

```rust
wit_bindgen::generate!({
    world: "connector-world",
    path: "<path-to-worldinterface>/crates/worldinterface-wasm/wit",
});

struct MyConnector;

impl exports::exo::connector::connector::Guest for MyConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "my.connector".to_string(),
            display_name: "My Connector".to_string(),
            description: "Does something useful.".to_string(),
            input_schema: None,
            output_schema: None,
            idempotent: true,
            side_effects: false,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        // Parse params as JSON, do work, return JSON result
        Ok(r#"{"status": "ok"}"#.to_string())
    }
}

export!(MyConnector);
```

### 3. Build

```bash
cargo build --target wasm32-wasip2 --release
```

The output is `target/wasm32-wasip2/release/my_connector.wasm`.

### 4. Write the manifest

Create `my-connector.connector.toml`:

```toml
[connector]
name = "my.connector"
version = "0.1.0"
description = "Does something useful."

[capabilities]
# Deny-by-default. Only list what you need:
# http = ["api.example.com"]       # URL hostname patterns
# filesystem = ["/tmp/workspace"]  # Path prefixes (absolute)
# process = ["curl"]               # Command allowlist
# environment = ["API_KEY"]        # Env var allowlist
# kv = true                        # Per-module key-value store
# logging = true                   # Structured logging
# crypto = true                    # Hash/HMAC/sign/verify
# websocket = ["wss://example.com"]# WebSocket URL patterns

[resources]
max_fuel = 1_000_000_000           # CPU budget (default)
timeout_ms = 30_000                # Wall-clock timeout (default)
max_memory_bytes = 67_108_864      # 64MB (default)
max_http_concurrent = 5            # Concurrent HTTP requests (default)
```

### 5. Deploy

Place both files in the vessel's connectors directory:
```
connectors/
  my-connector.connector.toml
  my_connector.wasm
```

Load via daemon API:
```bash
curl -X POST http://localhost:7600/api/v1/connectors/load \
  -H "Content-Type: application/json" \
  -d '{"name": "my.connector"}'
```

Or enable directory watching for automatic loading.

---

## WIT Interface

The connector interface is defined in `crates/worldinterface-wasm/wit/world.wit`:

```wit
package exo:connector@0.1.0;

interface connector {
    record descriptor {
        name: string,
        display-name: string,
        description: string,
        input-schema: option<string>,   // JSON Schema
        output-schema: option<string>,  // JSON Schema
        idempotent: bool,
        side-effects: bool,
    }

    record invocation-context {
        flow-run-id: string,
        node-id: string,
        run-id: string,
        attempt-number: u32,
    }

    describe: func() -> descriptor;
    invoke: func(ctx: invocation-context, params: string) -> result<string, string>;
}
```

**Streaming connectors** additionally implement:
```wit
interface streaming-connector {
    configure: func() -> result<streaming-config, string>;
    parse-message: func(raw: string) -> result<option<parsed-event>, string>;
}
```

---

## Host Functions Available

When your connector needs I/O, it calls host functions. Each is gated by the capability manifest.

| Interface | Functions | Capability Key | Notes |
|-----------|-----------|----------------|-------|
| `exo:logging` | `log(level, message)`, `span(name)` | `logging = true` | Structured logging with module attribution |
| `exo:kv` | `get(key)`, `set(key, value)`, `delete(key)`, `list(prefix)` | `kv = true` | Per-module persistent store (SQLite-backed) |
| `exo:crypto` | `hash(algo, data)`, `hmac(algo, key, data)`, `sign(...)`, `verify(...)` | `crypto = true` | Cryptographic operations |
| `exo:process` | `exec(cmd, args, env, timeout)` | `process = [...]` | Scoped process spawning (command allowlist) |
| `wasi:http` | `outgoing-handler.handle(request)` | `http = [...]` | HTTP requests (hostname allowlist) |
| `exo:websocket` | `connect(url, headers)`, `send(handle, msg)`, `close(handle)` | `websocket = [...]` | WebSocket connections (URL patterns) |
| `wasi:filesystem` | `read`, `write`, `stat`, `readdir` | `filesystem = [...]` | File I/O (path prefix scoping) |
| `wasi:clocks` | `monotonic-clock`, `wall-clock` | Always allowed | Time |
| `wasi:random` | `get-random-bytes`, `get-random-u64` | Always allowed | Cryptographic randomness |
| `wasi:cli/environment` | `get-environment`, `get-arguments` | `environment = [...]` | Env vars (allowlist) |

---

## Capability Patterns

### HTTP hostname patterns
```toml
http = ["api.github.com"]           # Exact match
http = ["*.github.com"]             # Wildcard suffix
http = ["api.github.com", "*.s3.amazonaws.com"]  # Multiple
```

### Filesystem path prefixes
```toml
filesystem = ["/tmp/workspace"]     # Only /tmp/workspace and below
filesystem = ["/data", "/tmp"]      # Multiple prefixes
```

### Process command allowlist
```toml
process = ["git", "curl"]           # Only these commands
```

---

## Testing

### Unit tests (native Rust)

Test your connector logic with standard Rust tests — no WASM needed:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invoke_success() {
        let params = r#"{"key": "value"}"#;
        let result = do_work(params);
        assert!(result.is_ok());
    }
}
```

Run with `cargo test` (native target, not wasm32).

### Integration tests (with host)

Use `worldinterface-wasm`'s test infrastructure:

```rust
use worldinterface_wasm::WasmRuntime;

let runtime = WasmRuntime::new(config)?;
let connector = runtime.load_module(&manifest_path, &wasm_path)?;
let result = connector.invoke(ctx, params).await?;
```

---

## Examples

See `examples/wasm-connectors/` in the WorldInterface repository:

| Connector | Type | Capabilities | Purpose |
|-----------|------|-------------|---------|
| `json-validate` | Standard | None (pure computation) | Reference: minimal connector |
| `host-demo` | Standard | All | Reference: demonstrates all host functions |
| `webhook-send` | Standard | HTTP | Production: outbound webhook delivery |
| `web-search` | Standard | HTTP | Production: web search integration |
| `discord` | Streaming | HTTP, WebSocket | Production: Discord Gateway (bidirectional) |

---

## Common Patterns

### Parse params, return JSON result

```rust
fn invoke(_ctx: InvocationContext, params: String) -> Result<String, String> {
    let input: serde_json::Value =
        serde_json::from_str(&params).map_err(|e| format!("invalid JSON: {e}"))?;

    let field = input.get("field")
        .and_then(|v| v.as_str())
        .ok_or("missing 'field'")?;

    let result = serde_json::json!({ "result": field.to_uppercase() });
    serde_json::to_string(&result).map_err(|e| e.to_string())
}
```

### Make HTTP requests (requires `http` capability)

Use the `wasi:http/outgoing-handler` host function through wit-bindgen generated types. The host enforces the hostname allowlist from your manifest.

### Use the KV store (requires `kv` capability)

```rust
use exo::kv;

// Store state
kv::set("last_run", &timestamp_string)?;

// Retrieve state
if let Some(value) = kv::get("last_run")? {
    // ...
}
```

---

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| "capability denied: http" | Hostname not in manifest | Add to `[capabilities] http` |
| "fuel exhausted" | Computation exceeded budget | Increase `max_fuel` in `[resources]` |
| "timeout" | Wall-clock time exceeded | Increase `timeout_ms` or optimize |
| "memory limit exceeded" | WASM linear memory full | Increase `max_memory_bytes` |
| Module won't load | Wrong crate-type | Must be `crate-type = ["cdylib"]` |
| `wit_bindgen::generate!` fails | Path wrong | Check `path` points to WIT directory |
