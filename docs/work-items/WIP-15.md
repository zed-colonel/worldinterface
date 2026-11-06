# WIP-15: WI-owned worker acceptance campaign

This candidate adds the non-ignored `worker_campaign` runtime target. It invokes
contract tooling as a subprocess and discovers compiled harnesses in the three
adopted configurations. Two harness groups run concurrently; each registered test runs once by exact
name under a 600-second timeout. Harness bytes are measured before and after
each group, and receipt validation measures every distinct harness again. Zero-test, ignored, missing, ambiguous and unsuccessful
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

## Evaluation history and remediation

The operator accepted packet
`4780080955b38a775f724de0970c02531cbe542a145bf15b2ecc8088c549ed83`
in run `23917702-7b27-4be4-8317-2ff31d928044` through
2026-11-11T00:00:00Z, under the unchanged evaluation-request limits. Commit
`4e9feaa` records the actual decision and trusted receipt anchor; the accepted
packet's 315 source files were unchanged.

Its live campaign executed all 222 registered tests: 221 passed, but WP-034's
seam observation failed when the domain canonical encoder rejected JSON null
for unknown controls. Consequently WP-031 and WP-034 aggregates failed; no
passing campaign or handoff candidate was produced. The exact evidence-enabled
WP-034 test reproduced the failure in isolation. The focused target had passed
without the evidence sink enabled, so that earlier result did not cover this
failure. Adopted runtime-test timed out at 30 minutes during its nested campaign.
The isolated 10,000-run Flow capacity diagnostic passed in 379 seconds.

The remediation writes observation JSON with its unavailable values intact,
retains create-once publication and exact-byte digest binding, and adds a
non-live regression for null controls, digest agreement and overwrite rejection.
It does not change the domain request codec or invent supported seam controls.
The runner groups independent harnesses across two workers, measures each
binary before and after execution and once per validation, and avoids repeated
full validation when publishing an already validated candidate. Per-test
outcomes, failures, source-generation checks and substituted-binary rejection
remain mandatory. The receipt records the applied campaign parallelism. A
bounded two-harness regression proves overlap, retained child failures and
rejection of conflicting or replaced executable pins.

These changed covered bytes require renewed exact-packet acceptance after the
source-bound non-live build. The actual receipt/anchor retain only the accepted
historical packet above. Positive verification of the remediation and complete
adopted-check passage remain due after acceptance. Non-live check results and
external evidence paths are reported in the implementation run's final message;
scratch reports are not committed source. WIP-16 retains final handoff
publication and G4/target closure.
