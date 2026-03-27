# WorldInterface

WorldInterface is a boundary fabric and internal container built on top of
[ActionQueue](https://github.com/zed-colonel/actionqueue). It compiles declarative
FlowSpecs into durable, crash-recoverable workflows with idempotent connector
invocations and pure transforms.

## Quick Start

```bash
# Build
cargo build --workspace

# Run tests
cargo test --workspace

# Start the daemon
cargo run -p worldinterface-cli -- serve

# Submit a flow
cargo run -p worldinterface-cli -- flow submit examples/golden-path/flow.yaml --wait
```

## Architecture

```
FlowSpec (YAML/JSON)
    │
    ▼
┌─────────────┐
│  worldinterface-flowspec │  Compile to AQ DAG
└─────┬───────┘
      │
      ▼
┌─────────────────┐
│  ActionQueue     │  Durable task execution
│  (WAL + leases)  │
└─────┬───────────┘
      │
      ▼
┌─────────────────┐     ┌──────────────────────────────────────┐
│  worldinterface-coordinator  │────▶│  worldinterface-connector              │
│  (AQ handler)    │     │  Native: delay, http.request, fs.read,│
└─────┬───────────┘     │  fs.write, shell.exec, sandbox.exec,  │
      │                 │  peer.resolve                          │
      │                 └──────────────────────────────────────┘
      │                        │
      │                        ▼
      │                 ┌──────────────────────────────────────┐
      │                 │  worldinterface-wasm                   │
      │                 │  WASM: json-validate, streaming-echo,  │
      │                 │  webhook.send, web.search, discord     │
      │                 └──────────────────────────────────────┘
      │
      ▼
┌─────────────────┐
│  worldinterface-contextstore │  Atomic output storage
│  (SQLite)        │
└─────────────────┘
```

## Crate Layout

| Crate | Description |
|-------|-------------|
| `worldinterface-core` | FlowSpec model, identity types, descriptors, receipts |
| `worldinterface-flowspec` | Parser, validator, compiler to ActionQueue DAG |
| `worldinterface-contextstore` | Atomic durable store (SQLite) |
| `worldinterface-connector` | Connector trait, registry, built-in connectors |
| `worldinterface-coordinator` | ActionQueue handler for FlowRun orchestration |
| `worldinterface-host` | Embedded host API (for programmatic use) |
| `worldinterface-http-trigger` | Dynamic webhook ingress |
| `worldinterface-daemon` | HTTP API daemon |
| `worldinterface-wasm` | WASM connector runtime (wasmtime, WIT, capability model) |
| `worldinterface-cli` | CLI binary (`wi`) |

## HTTP API

```
GET  /healthz                    Health check
GET  /ready                      Readiness check
GET  /api/v1/capabilities        List connectors
GET  /api/v1/capabilities/:name  Describe connector
POST /api/v1/flows               Submit FlowSpec
POST /api/v1/flows/ephemeral     Submit ephemeral FlowSpec
GET  /api/v1/runs/:id            Flow run status
GET  /api/v1/runs                List runs
POST /api/v1/invoke/:op          Single-op invocation
POST /api/v1/webhooks            Register webhook
GET  /api/v1/webhooks            List webhooks
DELETE /api/v1/webhooks/:id      Delete webhook
POST /webhooks/*path             Invoke webhook
GET  /metrics                    Prometheus metrics
```

## CLI

```
wi serve [-c config.toml]          Start daemon
wi flow submit <file> [--wait]     Submit FlowSpec
wi run inspect <id>                Inspect run
wi run list                        List runs
wi capabilities list               List connectors
wi capabilities describe <name>    Describe connector
wi webhook register <path> <flow>  Register webhook
wi webhook list                    List webhooks
wi webhook delete <id>             Delete webhook
```

## Configuration

### TOML (`config.toml`)

```toml
bind_address = "127.0.0.1:7800"
aq_data_dir = "data/aq"
context_store_path = "data/context.db"
tick_interval_ms = 50
dispatch_concurrency = 4
lease_timeout_secs = 300
shutdown_timeout_secs = 30
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WI_BIND_ADDRESS` | `127.0.0.1:7800` | Server bind address |
| `WI_AQ_DATA_DIR` | `data/aq` | ActionQueue data directory |
| `WI_CONTEXT_STORE_PATH` | `data/context.db` | SQLite database path |
| `WI_TICK_INTERVAL_MS` | `50` | Tick loop interval (ms) |
| `WI_DISPATCH_CONCURRENCY` | `4` | Max concurrent handlers |
| `WI_LEASE_TIMEOUT_SECS` | `300` | AQ lease timeout |
| `WI_SHUTDOWN_TIMEOUT_SECS` | `30` | Graceful shutdown timeout |
| `WI_DAEMON_URL` | `http://127.0.0.1:7800` | CLI daemon URL |

## Embedding

For programmatic use (e.g., Agent), use `EmbeddedHost` directly:

```rust
use wi_host::{EmbeddedHost, HostConfig};
use wi_connector::ConnectorRegistry;

let host = EmbeddedHost::start(config, registry).await?;
let caps = host.list_capabilities();
let result = host.invoke_single("delay", params).await?;
let flow_run_id = host.submit_flow(spec).await?;
host.shutdown().await?;
```

See `examples/agent-embedding/` for a complete working example.

## Examples

- **Golden path:** `examples/golden-path/` — delay + transform + fs.write
- **Agent embedding:** `examples/agent-embedding/` — standalone EmbeddedHost usage

## Documentation

- [FlowSpec Reference](docs/flowspec-reference.md)
- [Data Directory Layout](docs/data-directory.md)
- [Idempotency Guide](docs/idempotency.md)

## Sacred Invariants

1. ActionQueue is the sole executor of record
2. Step completion only after durable ContextStore write
3. `run_id` is the idempotency key for every connector invocation
4. Outputs written once per `(flow_run_id, node_id)` — no overwrites
5. Coordinator eligibility derived from AQ state + ContextStore only
6. Every boundary crossing produces an immutable Receipt

## License

Apache-2.0
