# WIP-08 — Durable waits, signals and event outbox

Candidate implementation for WI-WORKER-PROOF-1, using crates.io AQ 0.3.0 and
`contracts/aq-integration-target.json::runtime_release`. The imported baseline
matrices, their record digests, and the historical 0.2.0 release remain intact.
Gate closure becomes effective through qualifying review and CraftingTable
integration with all adopted checks. This document does not attest that review.

## Durable sources and delivery

Foundation's AQ-free continuation contract binds immutable event identity to
source revision/digest, tenant, audience, logical stream, kind and generation.
Domain-separated canonical hashes bound correlations and identities. External
intake authenticates the producer; host policy selects the authorized route.
Callbacks additionally bind an existing dispatched effect. Typed worker
observations retain provider/run/event identity and monotonic stream sequencing;
process lifecycle integration and AB-WI-021 remain WIP-10 work.

The source owner inserts its append-only event record and derived signal outbox
inside the same SQLite transaction. Ingress readiness/acknowledgments,
reconciliation receipts and Flow node results use their existing owner journals.
Callback/worker intake uses the continuation journal. Events exist without a
waiter. The existing ingress transport obligations retain expiry, recovery
generations, retired signals and subscriber acknowledgments; the new shared
notifications do not acknowledge subscriber ingestion.

Delivery freezes the exact versioned AQ request before admission. Admitted and
AlreadyExists both acknowledge the original SignalSequence. Bounded indexed
scans retain identity under capacity rejection and rotate scan cursors so one
pending prefix cannot starve later obligations. Rejection classifications are
journaled and exposed to authenticated inspection. Delivery failure does not
prevent unrelated queue scheduling. Child-binding readiness remains signal-free.

`local-continuation-8` replaces the development `local-flow-7` profile. Older
profiles reject before writes; there is no migration. Replay reconstructs all
outbox progress from source histories and delivery history, compares projections,
and checks event/checkpoint artifact closure. Paired reopen and backup/restore
also verify acknowledged signals and accepted AQ resume contexts.

## Wait and resume semantics

Static dependencies remain AQ DAG edges and the coordinator keeps its bounded
AllTerminal child wait. Compiler concurrency restrictions remain. Each wait/timer
child returns one Awaiting WaitSpec. WI persists plan/run binding, route,
checkpoint, actual signal eligibility and declared deadline first. Early waits
use exact correlated AnyRetained; AfterSequence and AfterHandled use actual AQ
SignalSequence values. FreshGeneration, including inheritance through
AfterHandled, changes the scoped route. A non-early wait freezes the live AQ
signal high-water mark when its dependencies are durably eligible.

Timers have a private route and their existing absolute deadline. Signal waits
retain their optional deadline, without implicit timeout. Whole-second validation
remains. ResumeWithTimeout records Continue/Fail as a WI outcome; signals,
children, deadlines, controls and administrative wakes never independently prove
business success. Controls/admin wakes reestablish unresolved waits.

AQ requires every emitted checkpoint to be new and owned by the current physical
attempt. This imposes two transport details on the accepted design: after death
between WI preparation and the first AQ checkpoint commit, WI journals a rebind
of the unpublished checkpoint's creating attempt while retaining its identity,
data and wait; a control/admin reestablishment retains the WI checkpoint and its
original AQ history reference without emitting that checkpoint again. Its
Awaiting disposition carries no new checkpoint. This allows exact acceptance
redelivery without emitting a checkpoint owned by a previous attempt.

Acceptance keys include queue-store identity, AQ run and ResumeContextId. Exact
context digests reproduce the recorded disposition. Conflicting reuse rejects.
Acceptance, handled sequence, domain result and resulting source/outbox record
commit together. Every wake verifies the pinned plan/checkpoint, current agency,
record scope and artifact access. Revocation cannot erase a committed outcome.

## Evidence and inventory

`conformance/wi-fabric-2/continuations/coverage.json` owns the complete executable
obligation map and AB-WI-008/011 references. The contract tool checks that every
reference resolves to one enabled, non-ignored test in an adopted package.
Runtime fixtures exercise production services/stores and real AQ, including six
process-death cuts, all five wake shapes, redelivery, deadlines, cancellation,
retained-event fan-out, cursor advancement, backpressure and physical-attempt
release. Storage tests cover rollback, frozen requests, replay and tampering.

The continuation static rule inspects production tokens and source routes in
foundation/runtime, including imports, aliases and macros. It rejects local wait
registries, wake channels, sleeps and legacy signal/delay capability names. Only
the exact approved development crash barrier is exempt; existing production
exclusion checks remain mandatory. QueueWakePort and the WI-03 MockWake were
removed while retaining source-record/subscriber deduplication assertions.
Legacy root-workspace connector deletion and milestones remain deferred.

The inventory disposition change adds the continuation domain/storage/runtime
interfaces as retain-align within their owning packages and removes the obsolete
QueueWakePort inventory item. Source hashes are refreshed after this structural
change. No imported source matrix, adopted check definition or AQ pin is changed.

Verification logs belong to this run's scratch/verification directory. All ten
adopted checks must pass on the committed candidate; supplemental development
runs do not replace CraftingTable's private-clone checks.
