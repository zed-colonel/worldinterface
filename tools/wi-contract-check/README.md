# WI contract development checker

This independent, unpublished Rust workspace parses Rust with `syn`, YAML/JSON
with `serde_yaml`, and Cargo manifests with `toml`. Its lockfile is separate from
the unchanged production lockfile. It does not depend on AQ or the WI runtime.

From the repository root:

```
wi-contract-check inventory generate
wi-contract-check inventory check
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

Run builds/tests with the controller Cargo launcher and supplied config. The
repository script and act job are described in CONTRIBUTING.md. Unit and mutation
tests use TMPDIR, never runtime stores or publication targets. A passing checker
record is independent development evidence, not runtime conformance.

The default milestone state is WI-01 entry only. External milestone state requires
a matching controller-provided digest; file and digest are trusted controller
inputs, not an authorization signature. Milestones are an unordered set of unique
known IDs containing WI-01 entry, not a numeric prefix. Each deletion rule checks
its own approved milestone. The controller owns dependency admission and rollback
protection; the checker adds no predecessor edges. The checker cannot advance gates. Future
controller integration must prevent PR authors from setting these trusted inputs.

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
