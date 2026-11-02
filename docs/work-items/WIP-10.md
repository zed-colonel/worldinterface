# WIP-10 — Worker lifecycle through real embedded WI and AQ

Candidate implementation under the accepted lifecycle design. AQ remains crates.io
0.3.0, under WIP-07 operator cycle 47b9f34c-4ffd-4ef6-8849-55aae36a8999;
imported 0.2.0 source criteria remain unchanged. Only CraftingTable integrates.
Ordinary worker use remains disabled. G3 is open pending approved live evaluation,
all ten adopted checks on the committed candidate and qualifying review.

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
preserve the decision lineage. No exact executable packet acceptance has been
supplied. The separate approval receipt is deliberately a draft; the trusted
host anchor is absent and positive evaluation fails closed.

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
The test host embeds Cargo's binary outputs at compile time and verifies their
digests independently of runtime path measurement, after checking the accepted
packet. WI source identity binds the packet and AQ identity binds the locked graph. The anchor may be provisioned only from the actual operator
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
They are not accepted by a hash refresh. No check definition is changed.

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

Before positive tests: accept the completed evaluation manifest digest with actual
operator provenance, provision its separate receipt/host anchor, and disposition
the exact supervisor pins. Covered source changes invalidate that approval.
After acceptance: run the lifecycle/crash/concurrency tests, fix any findings with
renewed packet acceptance when bindings change, refresh hashes, commit, and run
all ten adopted checks on that head. G3 closure requires passing evidence and
qualifying reviewed CraftingTable integration with no blocking or major finding.

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
