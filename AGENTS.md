# WorldInterface Agent Notes

## Purpose

WorldInterface is the tool and workflow boundary fabric built on ActionQueue. It
compiles FlowSpecs into durable workflows and exposes connectors, daemon APIs,
and embedded host surfaces.

## Active implementation

- `crates/worldinterface-foundation/Cargo.toml`: independent AQ-free workspace.
- `crates/worldinterface-runtime/Cargo.toml`: embedded runtime on crates.io AQ 0.3.0.
- `tools/wi-contract-check/Cargo.toml`: independent contract/publication tooling.
- Evidence and consumer entry point: [WI-EMBEDDED-WORKER-PROOF-1](docs/evidence/WI-EMBEDDED-WORKER-PROOF-1.md).
  `handoff check` validates the committed descriptor; `handoff verify DIRECTORY`
  validates fresh external campaign evidence on the unchanged committed head.
  Reviewed closure is effective only on reviewed merge with adopted checks.
- WSP-01 operator decision (2026-11-09): a reviewed, merged evaluation packet is
  accepted without an operator receipt, trusted anchor or calendar expiry.
  Prepare and non-live validate packet-bound changes, refresh the complete manifest
  in the same commit, then run live candidate verification only when the manifest
  and every bound source byte match HEAD exactly. Drift fails closed, including in
  adopted contract-check. Tests grant no review, integration, publication or closure.
- The rule also covers host catalog registration of reviewed nonsensitive fixtures
  in a reviewed commit, with exact bindings and no separate approval record. Scope
  remains the same cases/tasks and six operations, 15000 ms wall time and 65536
  retained output bytes; ordinary use and production credentials remain disabled.
  Historical receipts and anchors remain provenance. WSP-01 supersedes only the
  imported profile's explicit evaluation-approval procedure, not its exclusions.

## Historical repository structure

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

## Active work items

- Work targets `wi-worker-proof-1` under the [active ledger](docs/work-items/wi-worker-proof-1.md).
- Register implemented cases in `conformance/wi-fabric-2/proof/cases.json`; preserve
  authoritative scope coverage and exact manifest/package/test references.
- Run `cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh`
  after source edits. Structural drift needs an explicit reviewed disposition change.
- Run every adopted check on the committed head. `contract-test` and the strengthened
  `runtime-aq-graph` definition are approved for adoption at WIP-01 merge.
  Only CraftingTable integrates reviewed work.

## Working Conventions

- Tool-path issues usually involve the connector layer, coordinator, or host.
- Flow behavior issues often require checking both the FlowSpec compiler and the ActionQueue-backed execution path.
- Run workspace Rust commands from the repo root.
- Avoid editing generated or build-output directories such as `target/` and `dev-output/`.

## Verification

- Since 2026-10-13 WorldInterface is developed as single-application work, outside the
  former cross-project map: there is no controller Cargo launcher, `ct-act`, `ct-native`
  or supplied AQ pin. ActionQueue comes from crates.io at 0.3.0 (the runtime crates
  match `contracts/aq-integration-target.json`'s `runtime_release`; the imported
  `target` and `published_release` preserve the historical 0.2.0 baseline); never path- or
  patch-override AQ. Run `cargo fetch` first when dependencies must be downloaded.
- Before review, run the checks CraftingTable will run from `.craftingtable/checks.json`
  (offline, in a private clone of the reviewed commit). With Rust 1.89.0:
  - `scripts/check-wi-domain.sh`: the AQ-free foundation workspace
    (`crates/worldinterface-foundation`): format, Clippy, build, test, domain isolation.
  - `scripts/check-wi-integration.sh`: the runtime (`crates/worldinterface-runtime`, its
    own workspace and lock): format, Clippy, build, test, and the resolved-graph check
    that AQ is crates.io 0.3.0 with the published checksums.
  - `scripts/check-wi-contract.sh`: the contract tool (`tools/wi-contract-check`).
  `CARGO_TARGET_DIR` and `TMPDIR` must be outside the worktree; they default under
  `/tmp`. Foundation stays an independent AQ-free workspace; never resolve the runtime
  to satisfy a foundation check.
- Historical whole-workspace guidance below is retained for runtime changes and
  future public maintenance. Reinstate it for the supported complete workspace
  before public maintenance; scoped development does not waive release checks.
- Historical default validation from the repo root:
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
