# Baseline inventory and target dispositions

`current.json` is deterministic Rust-syntax inventory; `dispositions.json` assigns
one source-backed target treatment per item. These assignments implement the
approved plan and are reviewable in this source PR; they do not accept ADRs or
claim independent review has occurred. Regeneration never writes dispositions.

The frozen-baseline counts below are retained for historical comparison. Current
inventory additionally includes the four isolated foundation crates and every
physical target Rust source, declaration and test. Foundation packages are marked
`foundation-crate`; their dependency locks are inspected independently.

Baseline coverage includes all ten workspace crates, ten auxiliary example/test crates,
all physical Rust modules and nested declarations regardless of feature cfg,
macro-generated IDs, connector implementations, registration call sites and all
15 checked-in WASM manifest locations. Repeated manifest locations are retained.
The 25 Connector implementations include 23 native connectors, the WASM adapter,
and a test stub. The default registry has 21 entries; signal connectors are
constructed separately. Dynamic registration calls remain explicit inventory
items; registration matching records syntactic sites, not proof of runtime reachability.

`define_id!` is parsed as a declaration generator. Reviewed WIT/wasmtime/export
macros are inventoried as generator invocations with exact input hashes; generated
third-party binding types are not hand-written WI declarations. `proptest!` creates
test cases and is retained as test evidence. Unknown item generators fail closed.
Source file hashes bind modules/helpers and generator definitions to the reviewed
bytes. Conditional declarations retain cfg context and qualified identities.

Run `wi-contract-check inventory generate` from the repository root, review the
diff, then update dispositions individually. `inventory check` rejects missing,
duplicate, stale and incomplete assignments. Pure transforms remain deterministic
and I/O-free; coding technique goes to specialists; optional semantic WASM
connectors still require capability admission. There is no legacy-only category.

WI-10 adds `connector-disposition.json`: exhaustive family references, source
hashes, selective removal surfaces and replacement requirements. `catalog check`
includes root Markdown documentation such as `README.md` in surface discovery;
frozen source imports, historical fixtures, and ADR records remain excluded.
It validates this plan independently of removal readiness. Removal is blocked until
verified replacements exist. See [connector development](../docs/connector-development.md).
