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
signal high-water mark and the append position of its WI source store when its
dependencies are durably eligible. Source positions belong to the append-only
`event_sources` table and survive rebuild and paired backup/restore independently
of delivery order. A delayed older event is acknowledged and its resume is
accepted as a reestablished wait past that AQ sequence, retaining the deadline.

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

## Review remediation

These are implementation claims for the next review, preserving findings from
reviewed head `6eeccb4`.

| Finding | Disposition and regression evidence |
| --- | --- |
| F-001 | Fixed: a trusted wake verifies the frozen request and repairs its WI acknowledgment before acceptance, independently of scan order. Storage failures remain retryable. `admitted_unacknowledged_wake_with_backlog_recovers_independently_of_scan` covers 300 lower-id obligations and restart. |
| F-002 | Fixed: exact bound accepted contexts replay before current authorization, including coordinator Finish. Conflicting digests reject. `accepted_child_and_coordinator_replay_after_authority_revocation` forces AQ retry after WI acceptance on both paths and replays after reopening. A new coordinator context still requires current authority; a committed child result does not authorize a new Finish. |
| F-003 | Fixed: eligibility also records the authoritative WI append position. `non_early_wait_rejects_preexisting_outbox_events_after_backpressure_and_rebuild` covers pending events with and without capacity rejection. |
| F-004 | Fixed: collection key/element identities, aliases and macro bodies are inspected independently of field names. Timer, barrier, spin and third-party channel probes reject; storage locks and durable checkpoint collections remain allowed. The policy mutation tests include the review probes. |
| F-005 | Fixed: `continuation_producers` injects outbox insertion failures for ingress readiness, acknowledgment, reconciliation and Flow child results, asserting owner/source rollback, no-waiter persistence, rebuild and real Flow wakes. Exact producer tests are mandatory in contract-check coverage. |
| F-006 | Fixed: `signal_cancel_acceptance_and_deadline_winners_replay_across_paired_reopen` covers both signal/cancel orders, accepted-child then cancel and both deadline/signal winners. The backpressure test now completes unrelated work while delivery remains rejected. |
| F-007 | Fixed: AcceptResume checks next-wait, result and Finish artifacts independently. `accept_resume_validates_exports_even_with_other_artifacts` rejects foreign exports with and without other artifact fields. |
| F-008 | Fixed: missing and unauthorized events return one denial. `continuation_inspection_authorizes_and_reports_pending_and_accepted_chain` checks tenant/principal/audience policy, diagnostics and the accepted chain. |
| F-009 | Fixed: `QueueEngine::signal_capacity` exposes resident counts/bytes, limits, rejection count and an 80% alarm. `signal_capacity_alarm_uses_resident_statistics_across_restart` checks the threshold and recovery. See the capacity procedure below. |
| F-010 | Fixed: the ingress conformance README describes the current durable outbox path. |

The schema adds an explicit append position to authoritative event sources and
stores both eligibility cursors in the Flow journal. This is an unreleased
development-layout correction; interim stores reject schema mismatch, with no
migration or change to the AQ dependency or imported contracts.
Inventory dispositions explicitly retain the new cursor/capacity types and the
producer/recovery test modules within their existing foundation/runtime owners;
the catalog adds those references to its existing infrastructure family.

## Signal capacity procedure

AQ 0.3.0 retains signal identities and framed bytes for the lifetime of the store,
including retired signals and events with no waiter. Retirement does not recover
capacity. Default budgets are 100,000 identities and 16 MiB; the byte budget
usually binds first. The review measured about 544 bytes per representative WI
signal, or roughly 30,800 signals per default store. Actual envelope size and
other AQ producers determine the real budget; this estimate is not a quota.

Hosts must poll `QueueEngine::signal_capacity` alongside delivery status, alert
on its 80% alarm and any capacity rejection, and size both limits from aggregate
event rate times store lifetime, measured framed bytes and operating headroom.
Pending obligations and rejection codes remain durable while unrelated
scheduling proceeds. Deadlines still apply during backpressure.

Before exhaustion, stop admission, shut down the engine, and reopen the same
paired bundle with explicitly larger `SignalLimits` through
`bootstrap_with_signal_limits`. Drain the unchanged outbox and verify capacity
and pending diagnostics. Back up the paired bundle before maintenance. A fresh
complete WI/AQ bundle is an option only after existing work is settled and
producers are fenced and rerouted to new store-bound identities. Never replace
only AQ or replay old obligations into an unrelated store: waits and accepted
contexts bind the original queue store.

Per-tenant producer quotas can limit one tenant's growth but cannot reclaim
retained global capacity. No per-tenant AQ capacity isolation is claimed here;
the host must budget aggregate traffic and enforce its producer admission policy.
