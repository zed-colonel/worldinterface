# WorldInterface Agent Notes

## Purpose

WorldInterface is the tool and workflow boundary fabric built on ActionQueue. It
compiles FlowSpecs into durable workflows and exposes connectors, daemon APIs,
and embedded host surfaces.

## Repository Structure

- `crates/worldinterface-core`: FlowSpec model, receipts, identity types
- `crates/worldinterface-flowspec`: parsing, validation, compilation to AQ DAGs
- `crates/worldinterface-contextstore`: SQLite-backed output storage
- `crates/worldinterface-connector`: connector trait, registry, built-in connectors
- `crates/worldinterface-coordinator`: ActionQueue handler for flow orchestration
- `crates/worldinterface-host`: embedded host API
- `crates/worldinterface-http-trigger`: webhook ingress
- `crates/worldinterface-daemon`: HTTP daemon
- `crates/worldinterface-cli`: `wi` CLI
- `crates/worldinterface-wasm`: WASM connector runtime and test modules
- `examples/`: embedding and wasm examples
- `plans/`, `docs/`, `scripts/`: design notes and repo helpers

## Working Conventions

- Tool-path issues usually involve the connector layer, coordinator, or host.
- Flow behavior issues often require checking both the FlowSpec compiler and the ActionQueue-backed execution path.
- Run workspace Rust commands from the repo root.
- Avoid editing generated or build-output directories such as `target/` and `dev-output/`.

## Verification

- Default validation from the repo root:
  - `cargo build --workspace`
  - `cargo test --workspace`
- For daemon and CLI changes, smoke the primary surfaces:
  - `cargo run -p worldinterface-cli -- serve`
  - `cargo run -p worldinterface-cli -- capabilities list`
- For connector-path issues, prefer targeted crate tests first:
  - `cargo test -p worldinterface-connector`

## Generated And Runtime State

- Avoid editing `target/`, `dev-output/`, and compiled WASM test-module outputs unless the task explicitly targets them.
- Treat context-store SQLite files and submitted flow run state as inspection/runtime artifacts, not source files.
