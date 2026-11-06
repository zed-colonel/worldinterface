# WIP-15: WI-owned worker acceptance campaign

This candidate adds the non-ignored `worker_campaign` runtime target. It invokes
contract tooling as a subprocess and discovers compiled harnesses in the three
adopted configurations. Each registered test runs once by exact name under a
600-second timeout. Zero-test, ignored, missing, ambiguous and unsuccessful
results fail the campaign. Discovery never asserts passage. Authoritative scope
coverage and definition-review evidence classes remain registered separately.

The embedded composition now accepts a trusted, evaluation-only seam installation
through the existing provenance verifier. Unqualified external providers remain
rejected. `Application::inspect_worker_evidence` authenticates and authorizes the
same exact worker object as `inspect_worker`; it discloses only bound identity,
requested/advertised/applied/observed controls and sanitized cleanup measurements.
Recovery without live observations returns unknown controls and cleanup. It
never reconstructs observations from admission declarations.

Coding and non-coding fixtures execute all six operations through one application
and AQ engine. Additional cases cover embedded cancellation/output bounds,
scoped evidence denial, and the delayed, reordered, duplicated seam observations
and uncertain Start acknowledgment. Existing crash/recovery, credentials,
catalog, artifact and WI03P-F001 regressions remain in the same campaign.
`campaign-coverage.json` distinguishes embedded execution from prerequisite
service/domain evidence instead of treating every unit test as an embedded run.

The receipt binds the clean initial/final commit and tree, toolchain, three active
lockfiles, all six published AQ package checksums, OS/kernel/architecture,
execution identity, actual evaluation approval, compiled harness digests,
individual logs/outcomes, all 55 case aggregates and embedded control/cleanup
observations. Observation digests are emitted in their producing test logs.
Evidence is written outside the source and build cache. Reproduction is
`scripts/worker-proof-evidence.sh [EMPTY_EXTERNAL_DIRECTORY]`; verification is
`cargo run --locked --offline --manifest-path tools/wi-contract-check/Cargo.toml -- campaign check RECEIPT`.
The helper is supplemental, while adopted `runtime-test` runs the same campaign.

WP-033 permits only eligibility for `WI-EMBEDDED-WORKER-PROOF-1`. Worker completion,
WI receipts and campaign passage establish no consumer task acceptance, ordinary
use, parent acceptance, G5, remote parity, release or strong isolation. Same-user
co-residency and absent CPU/memory/network/filesystem/VM containment remain limits.
The seam is explicitly a **seam simulation**.

WP-034's executable pinned, nonsemantic producer-evidence contract is implemented
and registered here under the operator's recorded scheduling decision in run
`2f87cc54-2eee-4162-a164-dcab9a326c16`, recovered from cycle
`b3686455-c03a-4a5f-b88a-5b239dcb0fb8` on 2026-11-06. Derived handoff scope now
includes WIP-15 and WIP-16; imported source matrices are unchanged. The concrete
candidate is generated in scratch from a validated execution receipt, resolving
the producer commit at execution time. WIP-16 retains final consumer guidance,
publication of `proof/handoff.json`, G4 and target closure. Neither gate closes
in this candidate. The runtime uses crates.io AQ 0.3.0 under the WIP-07 operator
migration; the imported 0.2.0 baseline remains separately identified.

## Verification state

The prior approval receipt and trusted host anchor remain historical and
unchanged. Changed executable sources require renewed acceptance of the exact
packet after its non-live build. Until then this candidate has no positive live
campaign evidence and does not satisfy the exit gate. Six campaign checker
regressions, runtime compilation and Clippy passed during implementation.
Committed-head adopted checks and the final source-bound build results are
reported in the implementation run's final message; scratch reports are not
committed source. No final handoff or gate closure is published here.
