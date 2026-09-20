# Contributing to WorldInterface

Thank you for your interest in contributing to WorldInterface! This document covers the
development workflow, coding standards, and submission process.

## WI-FABRIC-2 development contract

This section governs WI-FABRIC-2 contributions and supersedes incompatible
instructions in the historical section below, including its `main` PR target and
universal `run_id` idempotency rule. The retained baseline instructions describe
the experimental implementation, not the target architecture.

The authority is adopted `craftingtable-repository-policy.json` policy version 1,
branch settings version 1, adopted `2026-09-18T00:41:03.548Z`. Work proceeds on
controller-managed work-item/slice branches targeting the single `wi-fabric-2`
integration branch. Only the controller may integrate after qualifying review of
the exact current source and target commits, under operator approval or recorded
integration policy. Final promotion requires explicit operator approval.

Experimental `main` and the `worldinterface/pre-wi-fabric-2` evidence tag identify
the frozen baseline `f48df82e98203fa94c86eecae60425205c3e00e9`. Extract historical
evidence without further feature work on `main`. These controller-local controls
are not remote hosting protections; matching Git refs are observations, not proof
of protection. Remote branch/review protections remain unverified and must be
configured and independently verified by the operator before the first remote
publication and final release approval. Publication and repository administration
follow [publication staging](docs/release/publication-staging.md).

For target identity and idempotency, follow the approved implementation plan's
[§1.2 identity separation](contracts/sources/wi-fabric-2-foundational-implementation-plan.md#12-current-surgical-points)
and [§12.3 explicit mapping requirement](contracts/sources/wi-fabric-2-foundational-implementation-plan.md#123-static-architecture-checks),
with the repository-owned [WI contract](contracts/wi-fabric-2.yaml).
ActionQueue `RunId` supports queue replay identity; it is not actor identity,
effect identity, authorization, provider idempotency identity, or proof identity.
Use of `RunId` as provider idempotency identity requires an explicit mapping record.
The historical connector examples do not establish that target contract or prove
provider deduplication or crash safety.

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

### WI-02 integration verification

For the active integration slice, use Rust 1.89.0 and the supplied controller
Cargo launcher. Run `ct-check -- scripts/check-wi-domain.sh`,
`ct-check -- scripts/check-wi-contract.sh`, and
`ct-check -- scripts/check-wi-integration.sh`; run each corresponding scoped
workflow with `ct-act -W .github/workflows/wi-<scope>.yml -j <scope>`.
Set the supplied manifest/config paths and `CRAFTINGTABLE_VERIFICATION_MODE` for
native scripts. CI receives them from the controller and uses explicit Cargo
configuration. The current-upstream-build launcher requires an AQ-resolved graph;
native foundation/checker commands therefore need an operator-approved
explicit-config exception or a controller launcher supporting AQ-free scoped checks.
Do not add artificial AQ dependencies to make those checks qualify as integration.
CI can run the independent manifests with the supplied explicit Cargo config.

Keep evidence outside build caches. Candidate case results require independent
review before WI-AQ-G1 acceptance; repeat verification at the actual merged or
rebased revision. Repository policy version 1 (adopted 2026-09-18) governs local
integration and freeze controls. The operator owns remote protections before
first publication and final release approval. Historical public maintenance and
release obligations below remain due at their recorded milestones.

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

- **Rust toolchain:** 1.86.0 (pinned in `rust-toolchain.toml`)
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
