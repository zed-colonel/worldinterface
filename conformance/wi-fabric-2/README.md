# WI-FABRIC-2 conformance package, revision 1

The active off-map proof registry is [proof/cases.json](proof/cases.json): 55
in-scope cases from the immutable source matrices. WIP-01 registers WP-001–008
and AB-WI-001/015/016 as implemented. Every other case remains planned. Source
matrix result flags and unrelated W1–W59 skeleton slots remain unchanged.

[proof/gates/](proof/gates/) contains ten gate records. Their pass criteria and
complete requirements come verbatim from the independently byte-pinned
[retired-map export](proof/wi-retired-map-gates-2026-10-13.json). Worker G1 and AQ
G1 are closed for reviewed integration; closure becomes effective through the
operator-approved merge with adopted checks passing. Eight gates remain open.
AB-WI-019 is required by worker G4; AB-WI-021 by worker G3.

Run `wi-contract-check proof check` or `all check`. The checker binds case IDs,
source-record digests (recursively key-sorted compact JSON, array source order),
allocations, complete prerequisites and authoritative scope coverage files. It
builds tests with `--no-run` and uses Cargo metadata, compiled harness listings,
ignored-test listings and Rust test attributes to establish eligibility under
foundation all-workspace/all-features, runtime default members/default features,
and checker default test configurations. It never recursively executes the checker
suite. Exact unambiguous test names and selected package ownership are required.

Adopted `foundation-test`, `runtime-test` and `contract-test` execute those tests;
review establishes that assertions cover each criterion. The new contract test
check is approved for adoption at WIP-01 merge. AB-WI-001 additionally tests the
historical AQ 0.2.0 identity, complete source tree, conformance revision
16 and feature profile against substitutions. `runtime-aq-graph` validates the
actual dependency graph. Store lineage and AB-WI-015/016 real AQ tests remain.

Existing `domain/`, `egress/`, `receipts/`, `worker/`, `ingress/` and
`integration/` coverage stays authoritative. Implementation registration never
asserts provider qualification or source-matrix runtime passage. Historical
`evidence check RECEIPT.json` tooling validates external receipt shapes and the
current runtime release binding; it is
not an off-map acceptance gate. Historical records remain separate from current
adopted-check passage. See [the active ledger](../../docs/work-items/wi-worker-proof-1.md).

The current runtime binds the separately recorded crates.io AQ 0.3.0 release
(`contracts/aq-integration-target.json::runtime_release`), with exact checksums,
versions and features enforced by runtime-aq-graph. Runtime receipts must bind
this release in addition to the unchanged imported baseline/source records.
