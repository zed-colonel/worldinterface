# WSP-04: supervisor conformance and WI-WORKER-G5

Accepted plan: WI-SUPERVISOR-PROOF-1-PLAN-1, WSP-04. WSP-03 owns WP-035–039;
WSP-04 owns WP-040 and G5. The original 55-case, ten-gate milestone stays fixed.
G5's criterion/cases come from the pinned worker execution profile, with the plan's
WSP-03 mapping and closed WI-WORKER-G1/G2 and WI-AQ-G1–G4 prerequisites. Closure
is effective only on reviewed merge with adopted checks; CraftingTable integrates.

## Carried regression measurements and fixes

Before changes, 100 waits under unrelated traffic reproduced the wall-clock
failure: average 579.78 ms, maximum 871.08 ms. Phase profiling found full
reconciliation dominating: 10 waits restored 30 plans/read 20 artifacts per pass;
100 waits restored 300/read 200. Representative 100-wait pass: 331.13 ms total,
251.27 ms validation, 35.48 ms eligibility preparation, including 102.83 ms plan
restoration and 158.45 ms artifact reads. Empty reconciliation was about 1–3 ms.
Holding 10 waits constant and adding 100 completed flows kept work at 30 plans/20
artifacts (38.26 ms representative pass). Counts, rather than load-sensitive timing,
establish history independence. Timings are diagnostics, not a raised threshold.

F-001 review found that the first bounded implementation moved the cost into
binding latency and durable AQ attempts: with 100 waits a new single-node Flow
needed 28 ticks and 14 coordinator/13 node attempts (reviewer probe), versus four
ticks and two/two attempts on the predecessor. Tenant-at-a-time scans also visited
history-only tenants. The remediation measures the original eight-row version at
0/10/100 waits and 100 completed runs in `scratch/verification/f001-before.log`;
100-wait reconciliation took 11–17 ms and still visited eight unchanged rows.

Reconciliation now prioritizes Flows named by AQ's indexed changed-task hints.
Each of the two calls per tick can validate at most four rows: up to three priority
rows and at least one background row when a pass is pending. The total remains
eight rows, two pages and at most 128 task-to-Flow lookups per tick. Hint discovery
is proportional to changed AQ tasks, with a deduplicated disposable queue; it does
not enumerate unchanged waits or recovered terminal history. A newly admitted Flow
and its changed children no longer wait for the background pass. All selected runs
still undergo complete plan, artifact, admission and ancestry validation. Hints
confer no authority. Continuing traffic cannot reset or starve the finite recovery
pass, whose admission cutoff excludes later runs. Errors retry; reopen validates
all plans, artifacts and AQ references and restarts the pass.

A global pending-run index skips completed rows across all tenants before the
page limit or deserialization. It is an optional derived schema-1 index, built
once at QueueEngine bootstrap for legacy stores after full validation. The schema
validator accepts its exact definition or its absence, and rejects a substituted
index. Source journal/projection formats and manifest versions stay unchanged.
F-004 operator decision (2026-11-14): accept `flow_pending_global`, keep Flow
schema version 1, and add no migration or in-memory pending set. WSP-03, merged
in `8f27b9a`, already added `effect_instances`, `effect_owner`, `effect_requests`
and `worker_reserved` without a migration. Full-schema validation therefore
already rejects stores across the alpha.1 / supervisor-line boundary in both
directions; this index removes no existing alpha.1 rollback path. The base
`8f27b9a` validator also rejects stores after index installation. This decision
accepts that restriction here rather than deferring it to WSP-05.
`flow_pending_global_page` returns a typed storage error identifying the missing
index and `flow_prepare_scheduling` prerequisite when called before preparation.
The non-ignored regression
`bootstrap_prepares_missing_scheduling_index_and_pending_page_survives_reopen`
checks that error, QueueEngine bootstrap from a store without the index, close,
reopen, schema version 1 and the retained pending page.
SQL VM-step tests cover 9,999 completed history tenants and a finite admission
cutoff, with no full scan or sort. Index construction and full reopen validation
are startup work, outside the per-tick bound.

The new non-ignored latency regression covers 0/100 waits crossed with 0/100
definition-only history tenants, both before and after QueueEngine restart. A
fresh single-node Flow must succeed within eight host ticks, yielding between
polls to the blocking executor, with at most two coordinator and two node attempts
in every configuration; this bounds scheduling under the fixture's single active
Flow workload, not arbitrary changing backlogs or handler execution time. The
first isolated result was four to five ticks and exactly two/two attempts in all
eight configurations. The existing unrelated-traffic test also limits the fresh
Flow to eight ticks. Active and idle elapsed times are diagnostics; invariant work
counts replace the shared-workstation-sensitive timing assertions without raising
their limits. F-002 removes the production environment-controlled stderr hook;
hosts retain explicit `tick_diagnostics` access.

F-008 distinguishes acquired descendants, departure and subreaper rediscovery.
A reparented child is reacquired from a fresh root scan, with stable birth/handle/
context validation. Live mismatches remain fatal. Deterministic acquisition
barriers force parent exit before/during acquisition and child departure; the
existing descendant cleanup, escalation and unrelated-process assertions remain.
The exact supervision pin/disposition changes only for this fix; no broader waiting
exception is granted. Intentional abort-point core suppression is preserved.

F-009 adds `IngressDeliveryHandler::with_clock`, with system time as its default.
One fixture clock now governs admission, dispatch, handler expiry and AQ time.
The fixture asserts admission before expiry and advances beyond expiry before
physical execution, preserving one-second retention, zero backoff, restart and
no-attempt/no-replacement assertions. Both targeted expiry probes pass. A broader preflight also exposed the same
setup-latency flaw in `inactive_binding_and_missing_registration_never_regenerate_aq_work`
at its one-second binding-expiry admission. Its isolated rerun passed, confirming
the load-sensitive precondition; the shared clock and explicit precondition now
also cover that sibling fixture, preserving all four modes and restart assertions. WSP-05 must
list this additive trusted-host API alongside the supervisor APIs in alpha.2 notes.
Also record `QueueEngine::tick_diagnostics`, `TickDiagnostics`, `ReconcileWork`,
`RECONCILE_ROWS_PER_TICK`, and storage `PendingFlow`/`flow_pending_page`/
`flow_next_tenant`/`flow_scan_bounds`, plus `flow_prepare_scheduling`,
`flow_pending_global_page` and `flow_pending_run`: additive trusted-host diagnostics
and scheduling reads. The runtime no longer uses tenant-at-a-time scheduling.
The explicit inventory/disposition additions are submitted with this item for review.

F-005 applies the same controlled delivery/AQ clock to
`expiration_during_ack_wait_completes_without_recovery_generation`, both short
token-invalidation tests, and all three interrupted-wake expiry cases. Each
fixture asserts unexpired admission; expiry advances only after the expected
wait (and reopen where applicable). The acknowledgment-expiry case crosses both
retention and retry deadlines, preserving token invalidation, one attempt, one AQ
run, generation zero and no regeneration after reopen. The invalidated-token
case advances past acknowledgment timeout while remaining inside retention and
backoff, so expiry cannot mask invalidation. No retention window is raised and
no production behavior changes. All six affected non-ignored tests pass together
in the focused run; committed-head adopted results remain external evidence.

## Evidence and consumer boundary

The external fixture retains operator-owned command, activation and terminal
records independently of temporary guest/WI state. Versioned bounded observations
bind all scenarios, original WI receipts, authenticated inspect and reconciliation
to exact campaign results. [G5 evidence](../evidence/WI-WORKER-G5.md) describes claims
and exclusions. Exoskeleton owns real Kata lifecycle and deployment qualification.
No alpha version change or publication occurs here; WSP-05 prepares alpha.2.

## Verification

The adopted runtime entry point independently runs `handoff verify` after campaign
construction, while the exact private source/build environment still exists. This
checks persisted receipt, observation, executable and publication bindings without
repeating physical scenarios or requiring another campaign.

Targeted checks and adopted committed-head checks are recorded in the implementation
run's external scratch/verification logs. Final results are recorded by CraftingTable
and the implementation handoff. Any F-007 recurrence is blocking and requires
retained-bundle diagnosis; a passing rerun cannot clear it.
