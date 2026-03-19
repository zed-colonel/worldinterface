# worldinterface-connector

Connector trait, registry, and built-in connectors for WorldInterface.

## Overview

This crate defines the boundary crossing abstraction and ships built-in connectors:

- **Connector trait** -- `describe()` + `invoke(ctx, params)` with `Send + Sync` requirement
- **ConnectorRegistry** -- Immutable registry with capability discovery
- **InvocationContext** -- Per-invocation identity (flow_run_id, run_id, attempt) and cancellation
- **transform** -- Pure transform executor for identity and field mapping nodes
- **receipt_gen** -- Receipt generation wrapper around connector invocations

## Built-in Connectors

- `HttpRequestConnector` -- HTTP requests with configurable method, headers, and body
- `FsReadConnector` -- File system reads
- `FsWriteConnector` -- File system writes
- `DelayConnector` -- Timed delays for scheduling and testing

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

Apache-2.0
