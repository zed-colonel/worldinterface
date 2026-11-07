# WIP-15: WI-owned worker acceptance campaign

The adopted `runtime-test` runs the non-ignored `worker_campaign` entry point.
It invokes contract tooling as a subprocess, discovers the three workspace
configurations once, and executes every registered test by exact name under a
600-second per-test limit. The campaign owns all runtime unit and integration
tests: their Cargo targets use `test = false` for default selection, while the
campaign explicitly builds them with `--lib --test '*'`. Targeted `--lib` and
`--test NAME` diagnosis remains available. The checker rejects a missing entry,
feature-gated entry or a second default owner. Unregistered runtime tests run
once as the remainder of the suite and must pass, with separate receipt outcomes.
Shared support tests keep a separate outcome and log for each compiled harness,
even when their names coincide; registered tests still require exactly one
eligible harness. There is no ordinary runtime test pass followed by a repeated
campaign pass.

Two harness groups run concurrently. Harness bytes are measured before and after
each group and once in receipt validation. The same compiled listings serve
execution and validation; independent receipt checks perform fresh discovery.
Zero-test, ignored, missing, ambiguous and unsuccessful results fail. Discovery
never asserts passage. The campaign subprocess has a 1785-second timeout plus a
15-second kill grace. The unchanged 1800-second adopted check budget covers the
complete command, including the initial Cargo build; it normally binds first.
Authoritative scope coverage and definition-review evidence remain separate.

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
The observation-file assertions remain in trusted test setup; the consumer
entry point delegates to them and retains the existing boundary scanner.
The runner groups independent harnesses across two workers, measures each
binary before and after execution and once per validation, and avoids repeated
full validation when publishing an already validated candidate. Per-test
outcomes, failures, source-generation checks and substituted-binary rejection
remain mandatory. The receipt records the applied campaign parallelism. A
bounded two-harness regression proves overlap, retained child failures and
rejection of conflicting or replaced executable pins.

The operator then accepted the final packet
`4dcbe731183fb3f5e461ecaf52101f0b40a12187e136e019891c4423d7f973ae`
in run `328acdc1-0146-4bed-b13e-b7553acdbd99` through
2026-11-11T00:00:00Z. Commit `b92a58ebd2a65c4e8ec14dfe44f4b97c77a287b7`
records that decision. The campaign passed **224/224 registered tests and 55/55
cases** on that clean commit. Receipt SHA-256:
`fe85c995fd2390ee89a14d1e87935ced74e7746fc0fc0523423189dcbb54cbfc`;
handoff candidate SHA-256:
`63d2a2358f1f8a995937fc96701830cd5a6d40bdc622fb789a5bd75d31dd4d65`.
The receipt and individual logs are in that run's
`scratch/verification/accepted-campaign/`; its consolidated check report is
`scratch/review/wip-15-status.json`. All ten adopted checks passed, with
runtime-test passing only on retry after a 30-minute timeout. The first run's
cold build took 8m09s while overlapping an independent campaign. The warm retry
built in 2m24s and used about 1543 seconds of test time. The independent review's
adopted run built in 3m07s and used 1598.7 seconds of test time: about 29m48s
against the unchanged 30-minute budget. These were passing historical results,
not sufficient headroom for the next integration.

## Review remediation F-001, F-002 and F-003

F-001 removes the duplicate runtime execution and second discovery as described
above. Receipt version 2 binds both the registered outcomes and every additional
runtime outcome; omitted, duplicate, ignored or failed remainder tests fail
validation. Non-live compiled Cargo fixtures prove one default entry, eligibility
of explicitly selected `test = false` targets, retained ignored-test exclusion,
and rejection of competing owners. Existing failure/log/binary substitution
regressions remain mandatory. F-002 records the actual accepted packet and
passing campaign above and in the active ledger. F-003 now generates a candidate
from fixture receipt evidence and all three observation files, then exercises
the payload validator shared by `validate_handoff` against substituted producer
pins, fixture bytes, observations, claims and missing embedded evidence.

Cold verification uses this worktree's supplied `CARGO_TARGET_DIR` after
`cargo clean`, without a competing campaign. A run records discovery/build/listing,
registered campaign, remainder suite and validation wall times in the external
campaign directory's `timing.json`; the enclosing log records Cargo's initial
build time and the unchanged 1800-second limit. The external cold record for this
remediation is run `e73756a8-8e69-4f9a-acb7-ba3074a3dbba`,
`scratch/verification/cold-runtime-timing.json`, with the full log at
`scratch/verification/cold-runtime-test.log`. The measured command passed in
1349.90 seconds on `555a8b0`, leaving 450.10 seconds under the adopted limit.
[WIP-15-verification.md](WIP-15-verification.md) preserves the phase timings and
receipt digest in source; external scratch artifacts may expire.

The operator subsequently accepted replacement packet `289f6efa…1cd07d6` in
run `e73756a8-8e69-4f9a-acb7-ba3074a3dbba`; the verification addendum records
that decision, the cold campaign and the committed-head adopted checks.

## Review remediation F-004 and F-005

F-004 raises only the campaign subprocess timeout from 1500 to 1785 seconds,
retaining its 15-second kill grace. The full command remains subject to the
unchanged 1800-second adopted check limit. This removes 285 seconds of earlier
inner cutoff: the reviewed 1348-second inner run would have 437 seconds of
inner headroom instead of 152, while its full-command headroom stays unchanged.
This is deadline alignment, not a reduction in test cost or a larger check
budget. WIP-16 must remeasure the adopted check when it adds tests.

F-005 corrects the cold-run pointer above and names the producing run explicitly
in the verification addendum. The recorded cold measurements are unchanged.

These packet-bound changes require renewed exact acceptance after the
source-bound non-live build. The actual receipt and trusted anchor retain only
the historical `289f6efa…` acceptance until the operator accepts the concrete
replacement. Positive evaluation and adopted runtime-test must use that
accepted replacement; non-live checks may run beforehand. WIP-16 retains final
handoff publication and G4/target closure. Scratch evidence is not committed
source.
