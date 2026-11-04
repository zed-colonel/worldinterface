# WIP-10 — Worker lifecycle through real embedded WI and AQ

Candidate implementation under the accepted lifecycle design. AQ remains crates.io
0.3.0, under WIP-07 operator cycle 47b9f34c-4ffd-4ef6-8849-55aae36a8999;
imported 0.2.0 source criteria remain unchanged. Only CraftingTable integrates.
Ordinary worker use remains disabled. G3 is open. The last accepted packet failed
runtime verification at `abd7a36`. This remediation fixes F-001/F-003, makes every
crash-cut result visible for F-002, and corrects the evaluation metadata for F-004.
Renewed exact packet acceptance, passing live tests, all ten adopted checks and
qualifying review remain required.

## Operator scheduling decision

Operator cycle 03de9255-3009-4b3c-ba39-9aae402f6aaa, source version 6,
recorded 2026-11-02T18:49:08.807Z in this run's
`plan/craftingtable-operator-decisions.json`, accepted the narrow premerge timing
exception: “Permit explicit operator acceptance of an exact evaluation packet
before merge. This decision itself authorizes no executable.” It also requires:
“Obtain approval of that completed packet before positive live tests.”

This supersedes only the merge-effective timing in plan §§5.2/5.4. The operator's
user ID is 0698a39b-dda2-4870-8f3f-97ef94784ae9; recovery run
c2d2033a-8905-4c3e-8c4b-b305c5b6b7c0 and snapshot
c196df65742d0720aad0ed79df4d58dd0fd6644ebecff002c26185b58737b7e3
preserve the scheduling decision lineage. The later explicit operator packet
acceptance is recorded in `evaluation-approval.json` and the test-host
`worker_approval_anchor.rs`, with its provenance, scope and finite expiry. This
acceptance authorizes evaluation of the unchanged packet only; ordinary use,
integration and gate acceptance remain outside its scope.

## Implementation

Runtime worker descriptors cover provision, start, inspect, terminate,
collect-artifacts and release on the existing Connector SPI. EffectService's
Direct plan path and EffectAttemptHandler perform real WI admission and AQ
execution. Preparation only validates. The process capability derives from exact
executor input, persisted authorization, worker realization, authenticated scope,
approved case/task/environment, installed controls and immutable artifact bindings.
Unsupported command/checkpoint/suspend/resume have no descriptor or execution path.

The evaluation manifest binds relevant sources, recipes, lockfiles, toolchain,
profile and environment restrictions. Its approval receipt and host provisioning
anchor are separate to avoid recursive hashes. TrustedApprovals must authenticate
actual current acceptance independently of request or repository assertions;
TrustedWorkerBuild must verify the measured helper/fixture build provenance.
The trusted test host verifies packet sources, runs a fixed locked/offline Cargo
build with an allowlisted compiler environment, verifies compiler identity,
profile and output source paths, and rechecks packet sources. It retains the
compiled bytes and digests before creating private launch artifact copies.
TrustedWorkerBuild compares those independent digests to launch measurements;
measuring a pathname alone grants no build provenance. WI source identity binds the packet and AQ identity binds the locked graph. The anchor may be provisioned only from the actual operator
acceptance and its provenance, never from a test-created accepted field. Neither
an observed binary digest nor a scheduling decision authorizes execution.

Fabric owns immutable run bindings, revision-checked lifecycle history, durable
stop obligations and worker capacity. The first reservation is the only creator;
retries reconcile the same run. AQ attempt/key release does not release capacity.
WI provider keys, canonical request digests and acknowledgements are journaled before success; reconciliation queries the original key. Running, stop requested, exit, successful completion and cleanup differ. The
portable reducer permits cancellation before Running. Only trusted confirmed
cleanup releases a reservation; a provider event cannot do so.

Adapter-private bounded, hash-chained launch journals reside outside workspaces.
Durable intent precedes helper spawn, stable ownership persistence precedes launch
permission, and fixture ownership precedes enabling its work. A helper-only
journal descriptor is excluded from fixture exec. Cleanup is synced before report;
acknowledgement loss cannot authorize another launch. Recovery quarantines all
unfinished reservations, requests stop and retains capacity. Helpers stop on host
disconnect within existing bounds. Recovery does not adopt or signal a PID from
serialized evidence. Missing/damaged/foreign evidence retains quarantine. Private
journals are not same-UID filesystem confinement; there is no hostile-code claim.

Worker observation intake atomically commits lifecycle, provider sequence/gaps,
WI revision and the WIP-08 outbox. Duplicate identity/content converges; conflicting
reuse rejects. Event revisions express WI commit order, separately from provider
sequence. Late facts cannot revive terminal work. Wake publication/acknowledgement
uses the existing durable outbox and exact signal identity.

## Store and source dispositions for review

Fresh store feature profile `local-worker-11` replaces `local-continuation-10`;
schema remains 1. Older development profiles fail before SQLite recovery writes;
there is no implicit migration. The worker journal, limits and projections join
integrity validation, replay, rebuild and backup. Foundation remains AQ-free,
process-free and dependency-isolated.

Inventory additions are explicitly assigned to worker domain/storage/runtime
implementation or test evidence; catalog family destinations and owners retain
their established classification. These are candidate review dispositions.

The WIP-09 exact-source supervisor exception is unchanged in scope. Changes to
`worker_process/mod.rs` and `worker_process/supervision.rs` need a reviewed pin
disposition for the launch journal and handshake ordering described above. The
other three supervisor pins and the generic continuation ban remain intact;
changed/unlisted-source regression tests remain required. Proposed exact new
pins are recorded in `conformance/wi-fabric-2/worker/supervisor-disposition.json`.
The explicit packet acceptance includes these exact-source dispositions for
evaluation; qualifying source review remains required. A hash refresh does not
accept them. No check definition is changed.

## Evidence and remaining checkpoint

The non-ignored lifecycle/crash tests and case mappings are implemented candidate
evidence, not assertions that live evaluation passed. WP-A03 is addressed by the
durable start/quarantine design; acceptance awaits crash results and review.
Artifacts/provider seam qualification remains WIP-11 and catalog governance WIP-12.

Preapproval verification uses Rust 1.89.0 and the supplied cache: the focused
storage tests (including provider-key replay) and non-process approval rejection tests pass; runtime targets
compile and Clippy passes. Full foundation verification is permitted. Positive
process tests and the full runtime suite have not run in this implementation step.
Logs and exact command outcomes are recorded under the run's scratch/verification.

Covered source changes require renewed packet acceptance. G3 closure requires
passing lifecycle/crash/concurrency evidence, all ten adopted checks on the
committed candidate and qualifying reviewed CraftingTable integration with no
blocking or major finding.

The first committed candidate's declared runtime Clippy check found that fresh
check-only targets have no binary files to embed. The replacement test-host path
uses non-authorizing sentinel hashes under Clippy only; runnable tests still
embed Cargo outputs and pin their bytes before sealing. The original packet
7c89f1cc09c190982fe37e53758fc5bd519bff2255904360db8e935ddb6e9334
was withdrawn without live evaluation or acceptance; its replacement needs
explicit acceptance of the new exact digest. Check definitions are unchanged.
The declared tooling suite also exposed the previous 27-connector inventory
count. It now requires 28 and explicitly checks the new embedded WorkerConnector;
the legacy default-registry registration count remains 21.

Worker notification correlation hashes provider and run together. Reusing a run
name under another provider cannot share its stream, revision index or wake; an
atomic outbox/replay regression covers both valid parallel streams and cross-route
rejection.

## Approved evaluation result, 2026-11-03

The exact approved packet is unchanged: all 264 file bindings, supervisor pins
and measured system support binaries match. The approved build recipe completes
with Rust 1.89.0 and records executable digests in scratch verification evidence.
Both approval rejection tests pass.

All eleven `worker_lifecycle` tests fail before process launch at
`tests/support/worker_fixture.rs:313`: the harness publishes the fixture ELF as
an executable artifact, but its 27,938,656 bytes exceed the existing 16 MiB local
artifact bound. No lifecycle success or crash/concurrency passage is claimed.
The combined focused command stops at that failure before running the crash and
adapter integration targets. The store bound and packet-bound sources remain
unchanged. A proposed build-profile fix requires a revised packet and renewed
acceptance before further live evaluation of that revision.

Run `a26cf478-833c-49c5-bf03-a9c8c09c0e69` retains exact command results and logs in
`scratch/verification`, including `worker-build.json`, `worker-build.log`,
`hashes-refresh.log` and `worker-focused.log`. Adopted checks are requested
against the committed candidate; their individual results live in that run's
verification evidence. This note records the failure, not passing qualification.


## Revised packet preparation authorized 2026-11-03

The operator explicitly requested preparation of a revised packet addressing all
three blockers. That direction authorizes compilation and non-live validation,
not positive fixture evaluation or a new acceptance receipt. The separate receipt
and test-host anchor retain the actual prior packet acceptance unchanged.

Dev/test debug information is disabled, while debug assertions, overflow checks
and crash injection remain enabled. Ownership journal identity/report fields and
an unknown cleanup exit status now omit absent values rather than emitting null.
The new journal regression round-trips intent, owned identity and cleanup with
missing/false/true status, and rejects mismatched, truncated or corrupt evidence;
it observes only its own process and launches/signals nothing.

Clean Cargo test compilation no longer includes unfinished executable bytes.
The test-only builder performs the verified compilation described above before
any launch, keeps independent digests and private copies, and supplies the existing
TrustedWorkerBuild boundary. The build regressions publish both actual executables
through the 16 MiB artifact store and reject altered executable paths. Crash
parents warm only the compilation cache before starting observation deadlines.
There is no build script or source-generator policy exception. The compiler
subprocesses are preparation, never worker operations or an approval assertion.

Nine inventory entries register the builder and non-launching regressions. One
adapter-test registration reference changes because its expected executable digest
now comes from the trusted build rather than measuring the launch pathname.
Existing catalog family owners/destinations and foundation-worker exclusions are
preserved; source hashes refresh without changing their structural decisions.

The only additional supervisor pin change is `supervision.rs` at
`7210b73647db0ade87f0cdf489a901fed9151d52ca2950135bf10526590fb259`, for the optional
exit-status serialization attribute. The five-file scope, other supervisor pins
and generic continuation ban remain unchanged. The proposed exact-source
disposition is part of the revised evaluation request and needs renewed explicit
acceptance together with the completed canonical manifest digest.

The revised build recipe compiles all tests without prebuilding binaries and runs
only the two non-launching build regressions. `scratch/verification/revised-*`
and `scratch/verification/revised-packet/` retain commands, compiler measurements,
cold-cache compile evidence and test/check results. Positive lifecycle, crash,
concurrency and adapter evaluation, every adopted check on the approved committed
head, and G3 review/integration remain due after renewed acceptance. No live
fixture or helper has been run on this revision.


## Revised packet acceptance and live evaluation, 2026-11-03

The operator accepted the completed revised package in this conversation:
“Yes, I accept the revised package.” The receipt and host anchor record the
canonical digest `4602f6c489562f1c73424786ed3180b091b79cfc93e6f51dc242fbddf8aacb53`,
its exact supervisor disposition and the finite expiry 2026-11-10T19:00:00Z.
All 266 source bindings and measured support programs match. This authorizes
only the evaluation scope in the request; it supplies no gate acceptance.

On acceptance commit `747d4f5`, fourteen of fifteen adapter regressions pass.
The path-replacement regression fails at its deliberate rewrite because fs::copy
inherits the builder artifact's owner-read/execute mode (0500). Only that attack
copy needs to be made writable; this finding does not authorize relaxing sealed
executable verification or private build artifact permissions.

The unavailable-operation lifecycle test passes; ten lifecycle tests and
AB-WI-021 fail with Uncertain outcomes. The crash matrix completes the cuts before
cancel and stops at `worker-cancel-before-commit`, after the child reports the
same Uncertain outcome. Source inspection identifies incomplete publication:
WorkerConnector returns one WorkerState output and no boundary record, while the
harness declares zero required outputs. The existing WIP-05 executor correctly
converts that claimed success to uncertainty. Its strict publication checks must
remain intact. A proposed follow-up would declare a deterministic operation
acknowledgement as the output and attach authoritative state as boundary evidence,
plus correct the attack-copy permissions and add publication-contract regressions.
Covered source changes need another packet and renewed acceptance.

Live logs are retained in `scratch/verification/approved-revision/`. All ten
adopted checks are requested on the committed candidate with these findings;
individual commands/results are retained there. G3 remains open. No live passage,
completed qualification or clean qualifying review is asserted by this record.

## Remediation evaluation

Review run f729811c-ddbb-4485-a380-88056015476e reported four findings at
`abd7a36`. The current implementation run is
69567943-836b-436e-aa25-c15d8636fb00. Its remediation instruction authorizes fixing
the findings and preparing the completed revised packet. The recorded scheduling
decision still requires actual exact-digest acceptance before positive live tests.
At preparation, the prior receipt and test-host anchor remained unchanged.

F-001: all four success branches now share one publication constructor. The output
is an acknowledgement of the validated operation request, so its exact artifact
can be declared before execution and remains stable across retries. Authoritative
WorkerState is published as lifecycle evidence and the boundary record. The
harness declares that exact acknowledgement. The existing executor and domain
publication requirements are unchanged. The non-launching contract regression
validates all six operations across initial, running, quarantined and cleaned
states, checks state evidence, and rejects missing boundary or wrong outputs.

F-002: the crash harness reports each completed cut, with its launch-journal count,
after asserting bounded process cleanup, retained capacity or confirmed cleanup,
quarantine before replacement, immutable run identity and authoritative event/wake
replay. At preparation, all 27 cuts, including four cancel and two cleanup cuts, required
a new authorized live run; no convergence result was then claimed.

F-003: only the test's attack copy receives owner-write permission after registry
sealing. The private built fixture remains 0500; executable sealing and digest
verification are unchanged. At preparation, the live path-replacement regression awaited acceptance.

F-004: lifecycle-coverage.json now records the previous packet as evaluated and
failing, and refers here for the changing current evaluation result. Its complete
case/test mappings stay packet-bound. This result note is separate from the
packet, like approval provisioning, so recording verification after acceptance
cannot invalidate the exact source that was approved. The note grants no authority.

Preparation result: non-live preparation passed. The publication-contract test,
two approval rejection tests, two build/provenance regressions, runtime formatting,
runtime Clippy and compile-only build of every runtime test target pass. The fixed
build recipe rechecks all 266 packet bindings before and after validation; both
actual executables remain below the unchanged 16 MiB artifact limit. A first
preparation attempt detected source drift while fixes were still being formatted;
it was rerun successfully after packet refresh. No worker/helper was launched.

The only structural inventory additions are WorkerAcknowledgement and its test
module, explicitly assigned to existing fabric-support/runtime dispositions for
review. Family owners, destinations, check definitions and supervisor pins are
unchanged; the required source hash refresh passes.

The revised canonical packet digest is
`abd936f432985aae400a6ae3c3d4c3b3ab66ef3eea0abc311fa6e6805c4d6342`.
At preparation, positive lifecycle, crash, concurrency and adapter tests, adopted
runtime-test, and G3 closure remained pending explicit packet acceptance and review.
Non-live logs and results are retained in this run's scratch/verification.


### Renewed acceptance and current result, 2026-11-04

The operator answered the exact-digest/unchanged-disposition/scope/expiry question:
“Yes, I accept the revised packet, including its unchanged supervisor disposition
and evaluation-only scope.” This accepts canonical packet
`abd936f432985aae400a6ae3c3d4c3b3ab66ef3eea0abc311fa6e6805c4d6342`
at `836b8b5` through 2026-11-10T19:00:00Z. The receipt and trusted host anchor
record that actual acceptance in run 69567943-836b-436e-aa25-c15d8636fb00.
All 266 source bindings, exact supervisor dispositions and measured support
programs match; no packet-bound source changed to provision this acceptance.
Ordinary worker use and integration remain outside the approval scope.

Current result: acceptance recorded; live lifecycle, crash, concurrency and
adapter evaluation and all ten adopted checks are in progress. G3 closure remains
conditional on passing evidence and qualifying reviewed CraftingTable integration.
Logs and exact results go under this run's
`scratch/verification/accepted-remediation/`.
