# Data Directory Layout

WorldInterface stores durable state in two locations configured via `HostConfig`
(or `DaemonConfig` / environment variables).

## Directory Structure

```
data/                              # Default root (configurable)
  aq/                              # ActionQueue WAL directory
    wal/                           # Write-ahead log files
      *.wal                        # Binary WAL segments
    snapshots/                     # Periodic snapshots
      snapshot.bin                 # Latest projection snapshot
  context.db                       # SQLite ContextStore database
```

## ActionQueue WAL (`aq/`)

The WAL (write-ahead log) is ActionQueue's durable state. It records every
mutation: task creation, state transitions, lease events. On startup, the WAL
is replayed to reconstruct the in-memory projection.

**Files:** Binary WAL segments. Do not modify manually.

**Recovery:** On restart, AQ replays the WAL from the last snapshot, restoring
all task states. In-flight tasks resume execution after lease expiry.

**Snapshots:** Periodic snapshots compact the WAL. The snapshot threshold is
configurable (default: 10,000 events).

## ContextStore (`context.db`)

SQLite database storing:
- **Node outputs:** Keyed by `(flow_run_id, node_id)`. Written exactly once
  per node (Invariant 4).
- **Step receipts:** Immutable evidence of boundary crossings (Invariant 6).
- **Coordinator map:** Maps `flow_run_id` to AQ `task_id` for crash-resume.
- **Webhook registrations:** Persisted webhook → FlowSpec bindings.
- **Global keys:** System metadata (e.g., webhook registry state).

**Schema:** Created automatically on first use. Migrations are applied at startup.

## Configuration

### DaemonConfig (TOML)

```toml
aq_data_dir = "data/aq"
context_store_path = "data/context.db"
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WI_AQ_DATA_DIR` | `data/aq` | ActionQueue WAL directory |
| `WI_CONTEXT_STORE_PATH` | `data/context.db` | SQLite database path |

### EmbeddedHost (Rust)

```rust
let config = HostConfig {
    aq_data_dir: PathBuf::from("data/aq"),
    context_store_path: PathBuf::from("data/context.db"),
    ..Default::default()
};
```

## Idempotency Markers

The `fs.write` connector creates marker files alongside output files:

```
/tmp/output.txt              # The written file
/tmp/output.txt.wid-<run_id> # Idempotency marker (empty file)
```

The marker prevents duplicate writes on retry. See `docs/idempotency.md`.

## Backup & Recovery

1. **Stop the daemon** before backing up
2. Copy the entire data directory (WAL + context.db)
3. Restore by placing files back and restarting

Do not copy while the daemon is running — the WAL and SQLite database may be
in an inconsistent state.
