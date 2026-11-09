# Contributing to WorldInterface

Thank you for your interest in contributing to WorldInterface! This document covers the
development workflow, coding standards, and submission process.

## WI-WORKER-PROOF-1 development contract

Work proceeds on managed whole-item branches targeting `wi-worker-proof-1`.
CraftingTable alone integrates after qualifying review of the exact source and
integration commits, under operator approval or recorded integration policy.
Repository policy settings version 1, observed 2026-10-13, retains the local
freeze of `main` at `f48df82e98203fa94c86eecae60425205c3e00e9`. Final promotion
requires explicit operator approval. Remote protection and publication remain
operator-owned before first publication and final release approval, as described
in [publication staging](docs/release/publication-staging.md).

The active workspaces are the AQ-free `crates/worldinterface-foundation` and the
AQ-linked `crates/worldinterface-runtime`. The runtime resolves AQ from crates.io
at exactly 0.3.0, pinned by `runtime_release` in
`contracts/aq-integration-target.json`; `target` and `published_release` preserve
the historical 0.2.0 baseline. No path, git or patch AQ override is
permitted. The legacy root workspace remains historical during this plan.

Every item records maturity, invariants, evidence, decisions and retained work
in `docs/work-items/<item>.md`. Register implemented cases in
[proof/cases.json](conformance/wi-fabric-2/proof/cases.json), referencing existing
scope coverage and exact `{manifest, package, test}` names. Test eligibility is
checked against Cargo metadata and compiled harnesses under each adopted test
configuration. Ignored, ambiguous, disabled, unreachable and misplaced tests
cannot count. Review must assess assertions against the source criterion.

Run all checks defined in `.craftingtable/checks.json` on the committed head.
CraftingTable establishes passage offline in a private clone. The Rust 1.89.0
scripts `scripts/check-wi-domain.sh`, `scripts/check-wi-integration.sh` and
`scripts/check-wi-contract.sh` are local preparation. Fetch missing dependencies
with `cargo fetch --locked --manifest-path <workspace>/Cargo.toml` first.
Keep `CARGO_TARGET_DIR`, `TMPDIR`, logs and evidence outside the checkout.
There is no map Cargo launcher or supplied dependency environment. The new
`contract-test` check is operator-approved for adoption at WIP-01 merge.

Refresh source hashes after edits:

```sh
cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh
```

This refresh updates only inventory and catalog/disposition source hashes. It
rejects structural drift and preserves families, ownership, destinations,
registration sites and treatments. Structural additions require an explicit
reviewed inventory/disposition change. It never refreshes normative-source or
approval pins. Do not edit `contracts/sources/` to claim results.

Gate records under `proof/gates/` preserve the pinned retired-map criteria and
complete prerequisites. Closed records become effective through reviewed merge
with adopted checks passing. Historical submissions are provenance, not current
passage. `evidence check` remains historical tooling. ADR approvals are byte-pinned
operator decisions, not implementation results. `contracts/milestones.json` stays
at `WI-01`; legacy clean-break deletions and full-parent release obligations are
deferred, as documented in [the plan ledger](docs/work-items/wi-worker-proof-1.md).

Use the source identity and idempotency mappings in the approved source plan and
`contracts/wi-fabric-2.yaml`. AQ RunId is queue replay identity; effect identity,
authority and provider replay identity remain distinct. Pure state changes need
property tests, durable changes replay tests, and external/cross-store changes
crash tests. Publication and full legacy maintenance requirements remain due at
their recorded milestones.

## License

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0 that covers this project.

## Historical experimental-baseline guidance (inactive)

The following original guidance is retained for the frozen experimental baseline.
All subsections below are historical-only; their imperative wording does
not govern WI-FABRIC-2 work. In particular, do not follow the historical `main`
submission route or universal `run_id` invariant for target contributions.

Per the recorded operator instruction of `2026-09-18T07:49:45.778Z`, retain this
guidance for the transition to a publicly maintained open source project. At that
transition, the original public contribution workflow and guidance must be
reinstated through an operator-approved documentation revision, with branch names,
build/test commands and architectural invariants reconciled to the approved release
contract. This is a required future transition, not automatic permission to unfreeze
`main` or restore superseded runtime semantics. The remote protection and publication
requirements above still apply.

### Prerequisites

- **Rust toolchain:** historical baseline 1.86.0; current WI-FABRIC-2 pin is 1.89.0 in `rust-toolchain.toml`
- **cargo** with fmt, clippy components

### Building

```bash
cargo build --workspace
```

### Running Tests

```bash
# Full test suite
cargo test --workspace

# Specific crate
cargo test -p worldinterface-core
cargo test -p worldinterface-coordinator

# Integration and acceptance tests
cargo test --test integration -p worldinterface-daemon
cargo test --test acceptance -p worldinterface-daemon
```

### Linting & Formatting

All code must pass these checks before merge:

```bash
cargo fmt --all -- --check
cargo clippy --all --all-targets -- -D warnings
```

Key lint thresholds (see `clippy.toml`):
- `too-many-arguments-threshold = 8`
- `too-many-lines-threshold = 100`
- `cognitive-complexity-threshold = 20`

Formatting rules (see `rustfmt.toml`):
- 4-space indent, 100-character max line width
- `group_imports = "StdExternalCrate"`

### Crate Architecture

WorldInterface is organized as 9 workspace crates with a strict dependency DAG:

```
worldinterface-core (types, FlowSpec model)
 ├─ worldinterface-flowspec (compiler)
 ├─ worldinterface-contextstore (durable store)
 └─ worldinterface-connector (trait + connectors)
     └─ worldinterface-coordinator (AQ handler)
         └─ worldinterface-host (embedded host)
             ├─ worldinterface-daemon (HTTP API)
             ├─ worldinterface-cli (CLI binary)
             └─ worldinterface-http-trigger (webhooks)
```

Key principles:
- **`worldinterface-core`** has no ActionQueue dependency and no I/O
- All execution goes through ActionQueue — never bypass it
- Connectors must be `Send + Sync`
- Use `thiserror` for error types, `tracing` for instrumentation

### Sacred Invariants

These are non-negotiable — never weaken them in a contribution:

1. ActionQueue is the sole executor of record
2. Step completion only after durable ContextStore write
3. `run_id` is the idempotency key for every connector invocation
4. Outputs written once per `(flow_run_id, node_id)` — no overwrites
5. Coordinator eligibility derived from AQ state + ContextStore only
6. Every boundary crossing produces an immutable Receipt

See `world-interface-v1.0-invariant-boundaries-policy.md` for the full specification.

### Submitting Changes

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all tests pass
3. Run `cargo fmt --all` and `cargo clippy` before committing
4. Open a pull request against `main` with a clear description of the change
