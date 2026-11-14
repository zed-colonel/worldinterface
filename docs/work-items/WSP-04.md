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

Reconciliation now reads indexed pages before deserialization, shares an eight-row
budget across both calls per tick, and walks tenants/runs with disposable cursors.
It reuses a verified plan within a pass. Changed journals trigger another full
bounded traversal; continuous traffic cannot reset traversal or starve later runs.
Each pass also captures finite run/tenant admission cutoffs, so continuing
admissions cannot extend the pass indefinitely and starve earlier changed rows.
Tenant traversal uses indexed rowid bounds; a regression appends 1000 new pending
rows and confirms the original pass still ends at its cutoff. Errors retry; bundle reopen still validates all plans, artifacts and AQ references.
No schema change or new continuation exception is needed. The existing timing
limits remain. A first complete continuation suite passed: active average 51.43 ms,
maximum 71.15 ms. Count assertions also vary 0/10/100 waits and completed history.

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
`flow_next_tenant`/`flow_scan_bounds`: additive trusted-host diagnostics and bounded scheduling reads.
The explicit inventory/disposition additions are submitted with this item for review.

## Evidence and consumer boundary

The external fixture retains operator-owned command, activation and terminal
records independently of temporary guest/WI state. Versioned bounded observations
bind all scenarios, original WI receipts, authenticated inspect and reconciliation
to exact campaign results. [G5 evidence](../evidence/WI-WORKER-G5.md) describes claims
and exclusions. Exoskeleton owns real Kata lifecycle and deployment qualification.
No alpha version change or publication occurs here; WSP-05 prepares alpha.2.

## Verification

Targeted checks and adopted committed-head checks are recorded in the implementation
run's external scratch/verification logs. Final results are recorded by CraftingTable
and the implementation handoff. Any F-007 recurrence is blocking and requires
retained-bundle diagnosis; a passing rerun cannot clear it.
