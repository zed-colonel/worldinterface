# WIP-03 — WI-03 ingress recovery takeover

This item takes over all 23 parked commits from `fce7def9d64e69633f6a20a78479dcdc8101d41b`
through `26114a778a584eca13d052e1927066e740b4cdbe` onto the worker-proof base
`8d25c1fdc35c1da98f66808455b87b59e0a4c6c1`. Each replayed commit retains its
original SHA in a cherry-pick trailer. Inventory conflicts were reconciled by
entry identity, preserving every WIP-01/WIP-02 disposition and the parked ingress
additions. Crates.io AQ `=0.2.0`, the runtime lockfile, source acceptance matrices,
ADR approvals, adopted check definitions and open gate records are preserved.

## Behavior and decisions

The parked repair is retained: reconcile expiration before dispatch, recovery and
execution; bound initial and acknowledgment waits by retention; deny recovery for
invalidated tokens, unavailable required references, inactive bindings and absent
subscriber registration. The AQ-neutral journal fences completions by generation
and digest, caps replacements at the captured `policy.max_attempts`, and reuses
the persisted wake. Exhaustion and retired wakes close automatic work as dead-letter.
Expiration invalidates the token and denies disclosure and acknowledgment;
dead-letter still permits a valid delayed acknowledgment before expiration.

ADR-012/013 remain the accepted policies. Provider identity, authenticated exact
bytes and safe contiguous offsets stay separate from application semantics.
Both transports filter captured fanout by allowed audience; subscriber inspection
returns only that principal's binding authorization through `SubscriberReceiptView`.
Claims and acknowledgments require trusted tenant/principal context, with tokens
issued only over the protected subscriber channel. Transport success is not an
application acknowledgment.

Hosts capture `IngressDeliveryHandler::subscriber_registry()` before moving the
handler into AQ, then pass it to `BoundAqIngress::with_subscriber_registry`.
Missing or wrong-tenant registry configuration denies replacement work. The new
regressions use this composition through reopen so a missing registry cannot hide
an unwanted recovery generation.

**WI03I-F010 is the expressly permitted follow-up:** hosts must retain one
`IngressRuntime` per tenant across dispatch polls for fair idle scanning. Actual
progress and its cursor commit atomically; idle scans rotate in memory without
journal writes. No new host poller is introduced here.

## WI03P-F001 permanent evidence

`wi03p_f001_expired_pending_delivery_stops_recovery` reproduces the preserved
probe with 1,000 ms retention, 100 ms acknowledgment timeout, zero policy backoff,
fixed zero AQ backoff and a 1 ms tick. Retention elapses before the first dispatch.
Dispatch and a timeout-bounded run to idle, followed by repeated dispatch and
bundle/AQ reopen, assert `Expired`, invalidated token, no subscriber calls or
domain attempts, generation zero, unchanged empty lineage, no AQ task, and no
additional resident signal identity or bytes. Disclosure and claim remain denied.

`wi03p_f001_admitted_delivery_completes_after_expiry` uses the same settings but
admits the task/wake before expiration and executes afterward. The original run
completes in one physical attempt with zero failure attempts. Repeated dispatch
and reopen preserve one run, one wake and its bytes, with no domain attempt or
replacement generation. This distinguishes prevention from handler convergence.

The original acknowledgment-wait, terminal-task, interrupted-admission, generation
budget, retired-wake, delayed-acknowledgment and old-completion regressions remain
non-ignored and execute with the adopted runtime test check.

## WI-03 source required tests

Foundation references below use manifest `crates/worldinterface-foundation/Cargo.toml`
and package `worldinterface-ingress`. Runtime references use manifest
`crates/worldinterface-runtime/Cargo.toml` and package `worldinterface-runtime`.
All are non-ignored tests; scope coverage remains authoritative in
`conformance/wi-fabric-2/ingress/coverage.json`.

| Source WI-03 requirement | Foundation test | Runtime test |
| --- | --- | --- |
| Duplicate provider delivery yields one observation and one logical delivery per captured subscriber | `concurrent_http_and_stream_duplicates_capture_one_atomic_fanout` | `ab_wi_007_commit_replay_identity_and_subscriber_ack`, `captured_fanout_routes_each_delivery_to_its_trusted_subscriber` |
| Observation commits before wake; crash recovers delivery | `restart_commit_before_wake_and_subscriber_commit_before_ack_are_safe` | `process_death_after_wi_commit_recovers_aq_obligation` |
| Subscriber stores before acknowledgment; crash redelivers safely | `restart_commit_before_wake_and_subscriber_commit_before_ack_are_safe` | `process_death_after_subscriber_storage_redelivers_same_id` |
| Forged source, stale binding, wrong tenant and invalid signature reject before admission | `transport_rejects_forgery_substitution_bounds_and_malformed_frames`, `binding_admission_is_scoped_idempotent_and_version_checked`, `exact_byte_http_and_session_signatures_match_independent_vectors` | `partial_audience_http_isolates_subscribers`, `partial_audience_stream_isolates_subscribers` supplement audience/tenant rejection |
| Ordering gaps remain evidence rather than false contiguous order | `gaps_reconnects_and_out_of_order_preserve_safe_committed_prefix`, `cumulative_ack_never_advances_on_conflicting_offset_or_failed_admission` | `ab_wi_006_009_one_wait_and_strict_signal_cursor_across_restart` supplements AQ cursor separation |
| Receipt claims no truth, urgency, conversation membership or authority | `quarantine_and_typed_lifecycle_evidence_do_not_create_authority` | Captured fanout and partial-audience tests supplement principal isolation |

The receipt test now explicitly checks the no-semantic-authority validation marker,
the observation digest and preserved asserted sender, absence of truth, urgency,
conversation-membership and authority claims, and rejection of those claims by
the ingress receipt wire schema. Typed role and commissioning evidence remains
observational; it creates no platform lifecycle operation.

HTTP and streaming ingress use the shared binding/observation/delivery path.
The foundation mock wake test proves store-before-wake; runtime tests prove it
against AQ and killed processes. AQ work is isolated behind `AqIngressPort` and
`BoundAqIngress`, leaving foundation AQ-free.

## AB cases and inventory

`integration/fixtures.json` and `proof/cases.json` register the same exact runtime
test names without changing source-record digests. The conformance manifest's
`integration_fixture_sha256` is refreshed too: the existing checker requires it
to bind the extended fixture bytes. This additional pin maintenance is required
by the code and preserves the manifest contract.

| Case | Evidence |
| --- | --- |
| AB-WI-006 / AB-WI-009 | `ab_wi_006_009_one_wait_and_strict_signal_cursor_across_restart`: one exact wait, typed signal cursor distinct from WAL, strict later matching through restart |
| AB-WI-007 | Commit/replay/conflict and subscriber acknowledgment; captured fanout; HTTP/stream audience isolation; both process-death boundaries |
| AB-WI-022 | `ab_wi_022_retirement_keeps_resident_capacity_and_alarm`; both WI03P-F001 tests; acknowledgment-wait expiration; generation-budget, owner-churn capacity, wait-capacity recovery, historical wake reuse and retired-wake tests |

The existing WI-AQ-001/002 ingress task admission mapping is retained in
`contracts/aq-consumer-reconciliation.json`: full request bytes precede AQ;
Created/AlreadyExists replies must match task ID, key, canonical version/digest
and positive sequence. Conflicts leave both journals unchanged. No new admission
version or private AQ override is introduced.

The explicit structural inventory dispositions adopted from the parked repair
cover `SubscriberReceiptView`, `DeliveryReadiness`, `SubscriberRegistry`, the
admission reply test and the interrupted/retirement/old-task completion helpers.
They stay in the existing ingress domain/runtime families and destinations.
The imported `RetireDuringWake` rationale records durable retirement cause testing.
No predecessor entry, connector family ownership, destination or treatment is
reassigned. The catalog scanner also requires this note as a shared `delay`
surface because it documents delayed acknowledgment. Its explicit treatment is
to retain WI-03 evidence and review shared references before later selective
connector removal. Hash refresh changes only source digests after that structural
addition and the source/docs edits.

## Verification and review handoff

Targeted runtime ingress/recovery and foundation ingress/storage checks precede
the mandated `hashes refresh`. Every adopted check, including `contract-test` and
`runtime-aq-graph`, runs on the committed candidate with Rust 1.89.0, offline in a
private clone through the run-provided `ct-check --declared`. Cargo verification
uses this run's external build cache and scratch directories. Logs and a candidate
digest/result index live under `scratch/verification`; implementation security
and inventory assessments live under `scratch/review` in the CraftingTable run.

The implementation security pass examines transport authentication/replay,
audience/receipt isolation, token secrecy, quarantine/revocation, recovery/capacity
bounds, stale completion fencing, journal compatibility and restart. It supplies
review evidence; independent CraftingTable review of this exact candidate,
including its security pass, remains required for WI-03 acceptance. Only
CraftingTable integrates reviewed work. **WI-AQ-G3 stays open for WIP-08**; this
item contributes AB-WI-006/007/009/022 without claiming that gate's closure.

The design's open choices use the accepted simplest resolutions above: replay all
23 commits, port both expiry boundaries, retain the bounded policies and record
WI03I-F010 as a follow-up. There is no design deviation or operator decision left
open.
