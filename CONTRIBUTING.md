# Contributing to WorldInterface

Thank you for your interest in contributing to WorldInterface! This document covers the
development workflow, coding standards, and submission process.

## Prerequisites

- **Rust toolchain:** 1.86.0 (pinned in `rust-toolchain.toml`)
- **cargo** with fmt, clippy components

## Building

```bash
cargo build --workspace
```

## Running Tests

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

## Linting & Formatting

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

## Crate Architecture

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

## Sacred Invariants

These are non-negotiable — never weaken them in a contribution:

1. ActionQueue is the sole executor of record
2. Step completion only after durable ContextStore write
3. `run_id` is the idempotency key for every connector invocation
4. Outputs written once per `(flow_run_id, node_id)` — no overwrites
5. Coordinator eligibility derived from AQ state + ContextStore only
6. Every boundary crossing produces an immutable Receipt

See `world-interface-v1.0-invariant-boundaries-policy.md` for the full specification.

## Submitting Changes

1. Fork the repository and create a feature branch
2. Make your changes, ensuring all tests pass
3. Run `cargo fmt --all` and `cargo clippy` before committing
4. Open a pull request against `main` with a clear description of the change

## License

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0 that covers this project.

## WI-FABRIC-2 development contract

Target feature work uses the single `wi-fabric-2` integration branch. The
experimental baseline is frozen at `worldinterface/pre-wi-fabric-2`; extract
historical evidence without changing its semantics. Only the controller/operator
integrates reviewed source branches. Publication and repository administration
follow [publication staging](docs/release/publication-staging.md).

Every PR records maturity (Independent, Assumption-bound, Contract-blocked or
Integration-only), invariants, evidence and exact upstream identities. Use the PR
and upstream-requirement templates. Changes to connectors update the complete
inventory and dispositions. Pure state changes require property tests, durable
changes replay tests, and external/cross-store changes crash tests. WI-01 changes
only development contracts; it does not implement new runtime recovery.

Run `scripts/check-wi-contract.sh` using the supplied controller Cargo launcher and
explicit `CRAFTINGTABLE_CARGO_CONFIG`. Keep `CARGO_TARGET_DIR` and verification
artifacts outside the checkout. Local CI is
`ct-act -W .github/workflows/wi-contract.yml -j contract`. Controller `ct-check`
retains repository check evidence. Historical runtime characterization is separate
from these independent checks; no current AQ integration is claimed.

Generated inventory is reviewable source; regenerate with
`wi-contract-check inventory generate > inventory/current.json`, then review every
changed disposition in `inventory/dispositions.json`. Do not auto-accept target
ownership or ADR decisions. Milestone advancement is controller-owned: externally
supplied `WI_MILESTONE_STATE` requires `WI_APPROVED_MILESTONE_SHA256` and a
set of unique approved milestone IDs including WI-01 entry. Each rule activates
only when its own milestone is present; WI-04 and WI-05 need no WI-03 entry.
Scheduling, dependency admission and rollback protection remain controller-owned.
The checked-in state admits WI-01 only and asserts no completed milestone. Do not weaken policy files to make deletion gates pass.
