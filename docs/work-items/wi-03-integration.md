# WI-03 ingress integration and recovery bounds

HTTP and streaming transports commit authenticated observations, ingress receipts,
captured subscriber deliveries, and immutable AQ requests before dispatch. The
production adapter uses AQ `29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860`, tree
`1c0c85dd700bd9d0c75de223dada911ffeadbb5b`, conformance revision 16. Archive and
source-tree identities remain in `contracts/aq-integration-target.json`.
WI-AQ-005/006 map to retained durable signals, bounded waits and real-AQ restart
tests, rather than process-local matching. The four assigned baseline cases and
their source-record hashes are in `conformance/wi-fabric-2/integration/fixtures.json`.

Capture `IngressDeliveryHandler::subscriber_registry()` before moving the handler
into the AQ engine, then supply that clone through
`BoundAqIngress::with_subscriber_registry`. The registry shares the immutable
transport map and tenant with the handler. A second list of subscriber names no
longer determines recovery readiness. An absent registry or a registry for another
tenant denies recovery. Host configuration must supply the registry of the handler
actually bootstrapped into that engine.

Unavailable work completes without a polling wait, including on first execution.
The dispatcher rechecks delivery expiration, token validity, binding status,
disclosure and registration before recovering it. Each delivery permits at most
its captured `policy.max_attempts` replacement AQ generations, in addition to its
original generation. This bounds infrastructure retries separately from actual
subscriber claims, which retain their own limit using the same pinned value.
Neither owner suspend/resume cycles nor a misconfigured registry nor persistent AQ
wait-capacity exhaustion can extend the bound. At exhaustion `RecoverQueue`
journals `DeadLetter` and returns `QueueRecoveryExhausted`, without replacing the
current queue obligation. The command, current request and complete bounded prior
lineage retain the cause and recovery evidence. Current binding configuration cannot
increase a captured delivery budget. This implements ADR-013's bounded retry and
dead-letter policy without introducing another persistence authority.

Dead-letter stops automatic work; it does not retract an issued token or assert
application rejection. An authenticated delayed acknowledgment with a valid token
can still close that delivery before retention expiry. Expiration and invalidation
continue to deny acknowledgment and disclosure. Retention bounds initial delivery
waits and acknowledgment waits even when task admission commits but signal admission
does not. The AQ retained log is not assumed to compact or reclaim identity capacity.

Keep one `IngressRuntime` instance for repeated dispatch polls. Its bounded scan
rotates in memory even when every inspected delivery is inactive or already
waiting. Idle inspection writes neither a journal revision nor history rows.
Actual task/signal admission, recovery, expiration and acknowledgment signaling
for a queue entry commit the restart cursor in the same SQLite transaction as
that progress, using the existing journal commands. Non-queued deliveries admitted
through the foundation API may also be claimed and acknowledged through the
runtime. Their acknowledgment signals and expiration commit without changing the
queue cursor; mixed admission APIs cannot block queued dispatch. Reopening retains
the durable position; any idle
rotation since the last progress is repeated at most once. Acknowledgments still
take priority over the bounded delivery scan. No schema, replay vocabulary or
second persistence authority is introduced.

## Repair coverage

The following IDs preserve the handoff lineage. Candidate remediation requires
fresh independent review; this table is implementation coverage, not that review.

| Finding | Correction or retained check |
| --- | --- |
| SEC3-F001 | Shared transport registry, tenant guard and durable generation budget; `handler_registry_fails_closed_for_missing_transport_and_wrong_tenant` and `recovery_generation_budget_bounds_mismatch_binding_churn_and_capacity_after_restart`. |
| SEC3-F002 | The same generation budget bounds owner suspend/resume without claims; the latter real-AQ test also checks zero subscriber calls, bounded prior/signal/run counts and restart. |
| R1.WI03P-F001 | Durable expiration at dispatch, recovery and execution; expiry-before-dispatch, acknowledgment-wait and terminal-task expiration regressions in `ingress_recovery.rs`. |
| SEC2-F001 | Unavailable runs retire without interval polling; inactive-binding and quarantine/release tests verify flat attempts through pauses and restart. |
| SEC-F001 | Invalidation, expired/revoked/suspended binding, quarantine, registration and terminal delivery states are checked before recovery and execution. |
| SEC-F002 | Audience-filtered binding capture in both runtime and reducer; HTTP/stream partial-audience tests. |
| SEC-F003 | Principal-scoped receipt views and disclosure checks; subscriber receipt isolation tests. |
| SEC-F004 | Scoped exact-byte replay precedes capacity allocation; non-deduplicable replay-capacity test. |
| F-001 | Nested attestation scope, availability and quarantine checks on both transports. |
| F-002 | Occupied-offset conflicts precede content-window deduplication; conflict regression. |
| F-003 | Generated binding, deduplication/offset and delivery closure model/replay properties. |
| F-004 | Accepted ADR-012/013 pages preserve the approved proposal as historical and distinguish approval from testing. |
| F-005 | Required contract checks and contract CI rerun on the committed candidate. |
| WI03I-F001 | Fresh completion clocks and revision-checked claims; concurrent-journal-write redelivery test. |
| WI03I-F002 | Subscriber storage before acknowledgment and killed-process recovery tests. |
| WI03I-F003 | Journaled replacement generations recover terminal AQ wait-capacity failures; persistent failures now exhaust a finite budget. |
| WI03I-F004 | Generation/digest fencing of delayed old task/signal completions. |
| WI03I-F005 | Retention deadlines release initial waits after interrupted task/wake admission, including replacement generations and restart. |
| WI03I-F006 | In-memory idle rotation, durable progress cursor and acknowledgment-first dispatch; small-limit inactive-prefix progress tests. |
| WI03I-F007 | Ingress README documents the bounded authenticated exact-byte replay exception. |
| WI03I-F008 | Idle polls leave the full ingress projection and history count unchanged before/after restart; `idle_dispatch_polls_preserve_revision_and_history_across_restart` covers three deliveries, limits 1/10, suspended/revoked/expired bindings, quarantine, invalidation, active waits, missing registration and expired deliveries, before/after initial admission. Storage fault tests roll back progress and cursor together and verify duplicate/stale completion no-ops and replay. |
| WI03I-F009 | Queue membership conditions only the cursor write, preserving non-queued progress. `nonqueued_acknowledgment_preserves_queued_dispatch_progress_across_restart` covers both admission APIs on one store, acknowledgment priority at limits 1/10, restart before/after signaling, queued progress and duplicate no-ops. `nonqueued_dispatch_progress_preserves_cursor_and_replays_atomically` covers acknowledgment and expiration, rollback, an unrelated cursor, duplicate no-ops and replay. |
| WI03I-F010 | Deferred host-loop integration nit: retain one runtime per tenant across polls as documented above. There is no production poller in this slice; adding one or a process-wide cursor holder is unselected follow-up. Existing small-limit restart tests verify the retained-runtime contract. |

The audit covers every producer of replacement work (`dispatch_pending`), its
journal authority (`RecoverQueue`), and retirement paths in the handler before
initial wait, after resumed execution, after claim races and after subscriber I/O.
The reducer boundary test covers policy limits 1, 2 and 100, pinned-policy isolation,
unchanged lineage at exhaustion and rejection of further recovery. Real-AQ tests
exercise missing transports, deliberately mismatched registry configuration,
tenant mismatch, repeated lifecycle churn, persistent wait-capacity failure and
valid delayed acknowledgment. Existing replay/property, crash, expiry, token,
audience, disclosure and dispatch-progress tests remain required.

The journal-growth audit covers all dispatch write sites: acknowledgment signals,
task completions, wake completions, terminal recovery and expiration. Only a
mutating progress command can persist a cursor. Unchanged readiness checks,
unregistered terminal work, established waits, duplicate completions and fenced
stale generations cannot append cursor-only history. The older standalone cursor
command remains readable for historical replay; the dispatcher no longer emits
it for inspection. Real progress still consumes append-only audit capacity under
the accepted retention/backpressure policy.

The mixed-admission audit covers all five commands accepted by the dispatch
transaction. Task admission, wake admission and recovery still require a queue
entry in the reducer. Acknowledgment signaling and expiration are also valid for
non-queued deliveries and preserve the current cursor. The transaction observes
membership under the same SQLite write lock as the progress command. No public
claim or acknowledgment authority is narrowed, and the reducer continues to reject
unknown standalone cursor entries. Queued rollback/no-op/replay and idle-poll tests
remain required alongside the non-queued regressions.

Use the supplied Cargo launcher for native runtime build/test and the AQ-free
supplementary domain/contract checks. Run all four adopted foundation checks with
`ct-check --declared`, the integration/domain/contract scripts through `ct-check`,
and their scoped jobs through `ct-act`. Rust 1.89.0 and the supplied explicit CI
Cargo config remain required. Logs and digest-indexed results belong outside source.

Repository policy revision 1 and branch-settings revision 1 specify controller-local
exact-commit review and controller-mediated integration. Remote protection is due
before first publication and final release approval. G2/G3 attestation, independent
review of case results on this candidate, a distinct security review, controller
integration, merged-target verification and parent acceptance remain separate gates.
This owning slice changes no plan, acceptance criteria, scheduling authority or
other work item's implementation.
