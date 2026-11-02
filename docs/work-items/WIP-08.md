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
Callbacks additionally bind an existing dispatched effect. New callback revisions
must increase per effect and scoped route (tenant, audience, kind, correlation
and generation). Exact retries of any previously recorded revision remain
idempotent; changed content at that revision rejects. A stale unrecorded revision
rejects atomically without a source or signal obligation, including after rebuild
and reopen. Typed worker
observations retain provider/run/event identity and monotonic stream sequencing.
A tenant-scoped source index rejects the same provider/run/event under a new
sequence or route, including after journal replay and projection rebuild.
Process lifecycle integration and AB-WI-021 remain WIP-10 work.

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
journaled as bounded redacted codes and exposed to authenticated inspection,
including AQ's `not_found` classification for a host-scope mismatch. Delivery failure does not
prevent unrelated queue scheduling. Child-binding readiness remains signal-free.

`local-continuation-10` replaces the development `local-continuation-9` profile
to add the durable callback revision fence. The preceding profile added the
worker source uniqueness constraint. Older profiles reject
before writes; there is no migration. Replay reconstructs all
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

Review remediation evidence:

- F-023: `stale_callback_revision_rejects_after_handled_across_rebuild_and_reopen`
  resolves the first wait with revision 5, rejects revision 3 before and after
  replay, keeps the AfterHandled second wait unresolved on stale/duplicate
  callbacks, and completes exactly once with revision 6. The storage regression
  `callback_revisions_are_scoped_atomic_and_validated_against_history` checks
  effect/route isolation, source/outbox/fence rollback and projection tampering.
- F-026: all four conformance READMEs now name `local-continuation-10` as the
  current profile, matching StoreRoot. WIP-07 retains its historical item record.
- F-019: `resequenced_worker_event_rejects_after_handled_across_rebuild_and_reopen`
  establishes an AfterHandled wait, rebuilds and reopens, rejects re-sequenced
  retries without appending a source, and completes only on a distinct event.
- F-020: `scope_rejected_signal_is_journaled_without_blocking_scheduling`
  runs an unrelated AQ task while journaling `not_found`, replays the pending
  diagnostic, then admits the frozen request with corrected host authority.
- F-022: `provider_callback_before_and_after_wait_resumes_flow_exactly_once`
  exercises both callback orders and duplicates through real Flow execution;
  `one_signal_resumes_two_live_flow_waiters_exactly_once_each` establishes both
  Awaiting runs before publishing one shared notification.
- F-021: the static rule and its regression probes reject `crossbeam`,
  `AtomicWaker` and `event_listener`, including renamed imports. Semantic
  detection of inferred run-keyed registries remains a code-review obligation.

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
| F-011 | Fixed: ban Waker, JoinHandle, Semaphore, Timer APIs, yield/sleep/receive timeout calls and async_channel/kanal. Inspect containers, tuples, slices, fields and generic aliases holding wait/resume identities, with Result allowed for identity-returning helpers. Reject empty polling loops. Only the durable PlanOperation::Timer variant is allowed. Policy tests cover each reported form. |
| F-012 | Fixed: disposable scalar WI-journal/AQ-sequence watermarks skip unchanged idle reconciliation; mutations and reopen cause full validation, and transient errors leave retries enabled. Fully bound runs also avoid redundant admission transactions and database reloads. `idle_ticks_skip_unchanged_waiting_flows_and_reconcile_changes_after_restart` checks 100 waits, unrelated work, cancellation and restart wakeup. |
| F-013 | Fixed: EventAuthorization::authorize_run_read provides a separate run disclosure decision with the existing principal-only default. The inspection regression now covers an operator granted or denied the wait/accepted chain and enforces the tenant boundary even with a permissive policy. |
| F-014 | Fixed: the backlog regression checks the missing acknowledgment before handler execution and the repaired sequence immediately after acceptance, inside the executor before the next drain. Removing handler repair fails that assertion. |
| F-015 | Fixed: inspect containers/fields of WaitSpec, ResumeContext and WakeReason; follow type and import aliases across each target crate to a fixed point. Reject future::pending, smol channels, recv_deadline, wait_timeout and nonempty timed polling loops. Policy probes include cross-module alias chains and a separate-crate negative control, while preserving local/return tuple transport of durable checkpoints. |
| F-016 | Fixed: `journal_only_admission_failure_retries_on_an_otherwise_idle_live_engine` establishes an idle watermark, fails AQ admission after WI AdmitRun by temporarily removing host control context, fails reconciliation during another tick, then repairs host context. A tick must recover without resubmission, signals, other AQ changes or reopen. |
| F-017 | Fixed: the inspection regression uses permissive event reads without an authorize_run_read override. The principal sees the populated wait/acceptance chain and another principal sees neither, exercising the trait default independently of operator policy. |
| F-018 | Fixed: the scale regression now admits one unrelated event per measured tick with 100 waiting Flows, checks admission and all 200 AQ waits, and enforces active tick budgets. The supported integration workload and host admission limit are below. |

Targeted mutation verification for F-016 removes each watermark guard separately;
both variants fail at the missing-admission assertion. Replacing the F-017
principal-only default with unconditional disclosure fails the foreign-reader
assertion. The unmodified recovery suite passes all ten tests. The new test-only
OpenEventReads inventory item is explicitly retained in its existing runtime
infrastructure family; no production authority is added.

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

## Supported waiting workload and reconciliation budget

This integration profile supports up to **100 simultaneously pending waiting
Flows per engine**, qualified with one waiting node and its coordinator per Flow
(200 AQ Awaiting runs). Hosts must cap admission at that workload and at 200
Awaiting runs, including coordinators, and defer additional submissions. This is
a host operating limit; QueueEngine does not enforce a new admission quota.
Larger workloads or more complex graphs need separate scale qualification or
partitioning into independent complete WI/AQ bundles. No larger waiting workload
is supported by this proof.

The F-012 regression uses 100 unresolved Flows and an average idle-tick budget
of 250 ms over eight ticks in the test profile. This is a regression threshold,
not an end-to-end production latency guarantee. A local targeted run measured
about 1.85 ms per idle tick. No wait identities, handles or business decisions
are cached: the optimization retains only two scalar journal positions. Losing
the watermark repeats validation. Flow writes, AQ transitions (including
signals, controls and deadlines), bootstrap, and transient reconciliation
failures cause another full pass. The pre-scan watermark prevents concurrent
writes or reconciliation's own writes from being skipped.

A pass after a journal change still visits all pending Flows. The same regression
qualifies active traffic with eight ticks, each preceded by one new unrelated
worker event and its outbox obligation. Every event must be admitted while all
200 AQ waits remain unresolved and no executor is dispatched. The active budget
is **500 ms average and 2 seconds maximum per tick** in the test profile; intake
time and initial wait establishment are outside that measurement. Unrelated work,
cancellation and restart wakeup are then checked. These thresholds allow build
host variation and are regression budgets, not production deadline guarantees.
Hosts must monitor tick duration and reduce admitted workload or traffic if their
latency requirements are tighter. Bursty traffic above one new event per tick
and startup/recovery latency are not qualified by this scale test.
The targeted run measured 161 ms average and 164 ms maximum active tick time.

Active reconciliation retains linear cost; fully bound runs avoid redundant
transactions even on those active passes. Per-run change indexing remains future
performance work. Reopen retains complete reference/artifact validation.
