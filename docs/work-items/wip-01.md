# WIP-01 — Re-baseline verification off the map

Maturity: verification. Base: `6d5cbd7d117ece6664c8cbc634fcf02c5e7a5af7`.
Integration destination: `wi-worker-proof-1`. Scope: the verification boundary;
production runtime behavior is unchanged.

## Implementation and evidence

The checker validates 55 cases against immutable worker and AQ source matrices,
source-record digests, exact plan allocations and authoritative scope coverage.
Compiled Cargo harness discovery under the adopted configurations proves exact
selected-package ownership and non-ignored eligibility. Rust attribute inspection
follows reachable target/module routes. Missing, ignored, ambiguous, misplaced,
unreachable and configuration-disabled tests cannot count. Discovery uses
`--no-run` and harness listings; it does not execute the checker suite recursively.
Adopted tests establish passage; review assesses assertion sufficiency.

Ten gate records preserve the complete verbatim criteria and prerequisites from
the retired-map export, SHA-256
`a092d50054dc125772498cdb268d709cf4a2800d427c7ea07e6ceb7e2c37c3e4` (19,057 bytes).
The checker pins it independently. Records retain historical submissions and
explicit off-map requirement bindings. Only WI-WORKER-G1 and WI-AQ-G1 are closed
for reviewed integration; their effect is conditional on reviewed merge with
adopted checks passing. Eight gates stay open. AQ G1–G4 require both baseline
checkpoints, with no invented numbered AQ prerequisites. AB-WI-019 remains with
worker G4/WIP-14; AB-WI-021 with worker G3/WIP-10.

WP-001/003/004/005/006/008 bind worker coverage; WP-002 binds all three protocol
representation tests; WP-007 binds all three catalog definition tests. Full
ADR-016 approval `10a13b9c-cbfc-4469-9e03-9011adbdd507` and ADR-005/006/008/010 remain
required. Historical worker submissions `264d791e`/`b0f3e948` remain provenance.
AB-WI-001/015/016 bind every corresponding integration fixture test. AB-WI-001's
new runtime test validates the actual resolved graph and rejects historical source
identity, wrong tree/revision/checksum, registry 0.1.x, git/path override and
unsupported feature profile. Existing store-lineage and actual AQ tests remain.

ADR-011/014/015/018 are imported byte-exactly from the supplied 2026-09-29 operator
records and independently pinned. Pages and queue preserve full coverage, exact
proposal, rationale, retained obligations and decision packet digests. ADR-019/020
stay queued. Source matrices and their result flags remain unchanged.

`hashes refresh` computes source-hash updates, rejects structural drift, and
preserves all other inventory/catalog semantics and original file bytes. It never
refreshes normative-source or approval pins. The active ledger records the
inherited baseline and slice status; historical work-item notes now identify their
map-era verification wording as superseded. Milestones remain at WI-01.

## Operator decisions and check adoption

Recorded operator guidance on 2026-10-13 (design recovery `fcb0ae98...`, source
cycle `1a50decb...`) authorizes adding `contract-test` with argv
`["cargo","test","--locked","--manifest-path","tools/wi-contract-check/Cargo.toml"]`
and definitionPaths `.cargo/config.toml`,
`fixtures/pre-wi-fabric-2/source/rust-toolchain.toml`, `rust-toolchain.toml`.
WIP-01 is `check_adoption_at_merge: true`, overriding the original plan field.
Current adoption remains binding until merge; the operator adopts the new version
then. `scripts/check-wi-integration.py` is a changed definition path for
`runtime-aq-graph`: identity and feature validation is strengthened so the new
runtime regression can exercise the same graph checker. On 2026-10-13, in the
remediation run `3969a222-9611-4825-b4a0-9fc154868201`, the operator explicitly
approved: "I approve adopting the strengthened runtime-aq-graph definition
together with contract-test when WIP-01 merges" (F-002). The approved script's
SHA-256 is `5afba7a35fb8af5df9f8149915fb994f1f28aa536ba572ede4b53a01e729b049`.
This is the concrete implementation of the accepted identity check;
no runtime production behavior or AQ source is changed.

The supplied gate export is authoritative. Map-only requirements are translated
as authorized: plan import/start, historical plus published AQ baseline, accepted
ADR records, closed gate records, and historical verified slices or covering WIP
scopes effective through reviewed merge with adopted checks. Slice scope review
remains an external review obligation, not an assertion inferred from test names.

## Verification

Local preparation commands are `scripts/check-wi-domain.sh`,
`scripts/check-wi-integration.sh` and `scripts/check-wi-contract.sh` with Rust
1.89.0 and output/cache directories outside the worktree. The committed head is
then checked with every current `ct-check --declared <id>`; `contract-test` is
supplemental before its adoption at merge. Logs and machine-readable results are
kept under the implementation run's `scratch/verification/`, outside the source.
Local preparation passed: foundation 123 tests, runtime 31 tests and contract
tooling 76 tests (including 12 new proof/refresh regressions), zero failures or
ignored tests. Format, Clippy, builds, domain isolation, actual AQ graph and all
ten contract families passed. CraftingTable establishes final adopted-check
passage on the reviewed merge.

Review remediation on 2026-10-13 retains F-001's withdrawn disposition. F-002's
explicit adoption approval is recorded above. For F-003,
`ab_wi_001_published_identity_rejects_substitutions` now requires each negative
case's expected `AssertionError` message as well as a failing exit. For F-004,
`scope_digest_and_closed_gate_adr_status_disagreement_reject` selects WI-ADR-016 by
ID rather than queue position. Both focused tests passed before commit. Evidence
for this run is under
`/mnt/workhorse/craftingtable/runs/3969a222-9611-4825-b4a0-9fc154868201/scratch/verification/`;
the final run report records the committed-head declared-check results.

Retained work: the remaining 44 planned cases, eight open gates, later WIP
implementation/review, full-parent and legacy clean-break obligations, consumer
qualification, final promotion and operator-owned remote publication/protection.
None is claimed complete by this item.
