# worldinterface-contextstore

Atomic durable store for WorldInterface node outputs.

## Overview

This crate provides the persistence layer for flow execution state:

- **store** -- ContextStore trait with write-once node outputs and upsertable globals
- **atomic** -- AtomicWriter enforcing write-before-complete discipline (Sacred Invariant #2)
- **sqlite** -- SqliteContextStore backed by SQLite (in-memory or on-disk)

Node outputs are keyed by `(flow_run_id, node_id)` and are immutable once written. Global keys support upsert for mutable shared state.

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

Apache-2.0
