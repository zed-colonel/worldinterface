# WI contract development checker

This independent, unpublished Rust workspace parses Rust with `syn`, YAML/JSON
with `serde_yaml`, and Cargo manifests with `toml`. Its lockfile is separate from
the unchanged production lockfile. It does not depend on AQ or the WI runtime.

From the repository root:

```
wi-contract-check inventory generate
wi-contract-check inventory check
wi-contract-check hashes refresh
wi-contract-check proof check
wi-contract-check contracts check
wi-contract-check clean-break check
wi-contract-check all check
wi-contract-check evidence check RECEIPT.json
wi-contract-check publication check RECEIPT.json
```

`all check` emits separate structured records for baseline, inventory, contracts,
fixtures, provenance, conformance, documentation and clean-break obligations.
Each finding has a stable rule/item ID, source path, governing source,
disposition and diagnostic. Failures return nonzero. `WI_REPO_ROOT` overrides the
working directory for tooling/tests; it grants no authority.

Run with Rust 1.89.0 using the independent Cargo manifest. The adopted checks
and three local preparation scripts are described in CONTRIBUTING.md. Unit and mutation
tests use TMPDIR, never runtime stores or publication targets. A passing checker
record is independent development evidence, not runtime conformance.

The default milestone state is WI-01 entry only. External milestone state requires
a matching controller-provided digest; file and digest are trusted controller
inputs, not an authorization signature. Milestones are an unordered set of unique
known IDs containing WI-01 entry, not a numeric prefix. Each deletion rule checks
its own approved milestone. The controller owns dependency admission and rollback
protection for historical external milestone inputs. During WI-WORKER-PROOF-1,
`contracts/milestones.json` stays at WI-01; no external advancement is supplied.

Archive isolation conservatively checks Cargo members, target and dependency
paths, aliases and local transitive dependencies, Rust path attributes and static
include expressions. Code routes must resolve to `.rs` files in the inspected
`crates/` or `examples/` tree, outside skipped build/test directories. Unsupported
extensions, target layouts, and inline-module path attributes fail closed;
`inventory generate/check` also reject these routes before claiming completeness.
The finite source set is scanned once, including nested routes and cycles. Resource
`include_str!`/`include_bytes!` files need not be Rust but remain archive-isolated.
Dynamic source inclusion and arbitrary build scripts fail
closed until a source-generator policy is reviewed. All feature branches are
scanned; only exact cfg(test) bodies and separate test source directories are
excluded from production symbol checks. Explicit archive routes are rejected even
through development dependencies/targets. Optional identity adapters remain
permitted outside foundational crates; default-enabled identity dependencies are
mandatory. EXO dependencies are always forbidden, including optional, renamed,
local transitive and locked registry routes. Protocol/capability dependency direction applies to crates and staged
modules. This is a repository guardrail, not a general Rust build sandbox.

JSON Schema validation supports only the keywords used in the checked-in schemas;
unknown keywords fail closed. Execution evidence validation also verifies artifact
bytes, planned-case rejection and upstream/source/reviewer identity bindings. It
cannot determine whether a reviewer or external observation is truthful. Independent
review and controller authority remain external obligations.

## Off-map proof and maintenance

`proof check`, included in `all check`, validates 55 cases and ten gates against
independent normative matrix and export pins. It derives required cases separately
from gate records, checks scope coverage, and resolves executable tests through
Cargo metadata and compiled harness listings under the adopted configurations.
Discovery runs `cargo test --locked --no-run`, then each executable with `--list`
and `--ignored --list`; it also inspects reachable Rust test attributes. Discovery
establishes eligibility only. Adopted test checks establish passage and review
assesses assertions. Fixture mutation tests supply a separate discovery index.

`hashes refresh` computes both inventory and connector-disposition updates before
writing either. Only `source_sha256` fields may change. Structural inventory or
catalog drift is rejected for explicit review; ownership, destinations, surfaces,
registration sites and treatments are retained. Repeat refresh is deterministic.
Approval bytes, source matrices and gate-source pins never refresh automatically.

Closed gates become effective through reviewed merge with adopted checks passing.
`evidence check` remains historical receipt tooling and does not close these gates.
The new `contract-test` check executes WP-007 and checker regressions after its
operator-approved adoption at WIP-01 merge.
