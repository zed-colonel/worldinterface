# WIP-04 — WI-05 completion integration

This candidate selectively takes over `cb64b65`, `d64e63f`, `c7da12f` and
`c507031ba9163f8f3bbe6a2c70d0580f25d87882` from parked WI-05 integration on
baseline `71e6a523fcb54b5db31aa3784a8283c2ad574016`. WIP-02 admission and WIP-03
ingress recovery are retained. Obsolete launcher/CI changes were excluded.
The parked range supplied implementation input, not review or passing evidence.

The target runtime now owns `EffectExecutionPort`, `EffectAttemptHandler`,
receipt-bound `CompletionLocator` and authenticated `ReceiptInspector`.
Production Start requires acknowledged admission, an observed run and current
exact authorization. Its durable record fences provider reinvocation. Required
artifacts are verified and published before the atomic Fabric outcome, receipt
and outbox transaction. AQ success references only a published receipt-bound
locator. Lost outcomes become explicit Uncertain receipts; cross-store completion
without a receipt or locator rejects reopen. Exhausted AQ interruption budgets
retain inspectable convergence faults and the original provider-call identity.

## Approved decisions and scope

Decision cycle `dd3d1012-2c57-4307-bec7-f51a41b02c69` (2026-10-01) changes new
effect admissions to `max_attempts=3` and `SafetyLevel::Idempotent`. Persisted
requests keep their exact bytes, artifact hash, canonical digest, admission key
and task identity. The old-policy regression persists a 1/Transactional request,
admits it with a lost WI acknowledgment, restarts, and recovers AlreadyExists
without rewriting. New-policy conflict tests mutate 3 to 4 and Idempotent to
Transactional. Public CanonicalAdmissionV2 vectors and the approved AB-WI-003
tenant-scope interpretation remain covered. WIP-03 mappings are preserved in
`contracts/aq-consumer-reconciliation.json`.

Operator cycle `10a42bb3-5c5d-4300-8ec2-afde9c238211`, recorded in this run's
`plan/craftingtable-operator-decisions.json`, approves exactly `crash-points = []`
on storage through the runtime storage dev-dependency and adoption of amended
checks at merge. Checkpoint code and calls remain conditional. The isolation
checks reject defaults, forwarding, aliases and normal/build activation and run
negative checker cases. They inspect Cargo compiler-artifact feature records
from actual offline normal foundation/runtime builds. Foundation verification
resolves only its independent AQ-free workspace. All-features tests remain.
The approved check change adds helper definition paths; check IDs and commands
are retained. Approval supplies authority, not passing evidence.

The source WI-05 exit gate applies to the active target model. Legacy receipt
deletion remains deferred with `contracts/milestones.json` at WI-01. WI-12 owns
signing providers/key lifecycle; detached proofs here record provenance only.
WIP-04 contributes completion evidence to WI-AQ-G4; WIP-07 closes that gate.

## Whole WI-05 obligation matrix

Exact manifest/package/test references are in
`conformance/wi-fabric-2/completion/coverage.json`. `contract-check` discovers the
adopted harnesses and rejects missing, ignored, ambiguous or disabled references
for every obligation, alongside the case registry. Tests below run in runtime
unless a foundation package is named.

| Source required test | Executable evidence |
|---|---|
| Canonical bytes/digest stable across replay | `canonical_receipt_survives_replay_rebuild_backup_restore`; protocol `receipt_import_requires_exact_canonical_bytes_and_supported_profile` |
| Authoritative intent/observation and canonical resource | egress `receipt_outcome_and_outbox_commit_after_scoped_publication`, `receipt_execution_evidence_is_bounded_by_authoritative_outcome`; ingress `process_death_after_wi_commit_recovers_aq_obligation` reconstructs the immutable indexed ingress receipt |
| Completion never exposes unpublished output | `aq_completion_exposes_only_published_locator`, `completed_aq_run_without_fabric_receipt_or_locator_rejects_reopen` |
| Every publication crash converges to one receipt or explicit uncertainty | `completion_kill_matrix`, `aq_disposition_process_kills_replay_the_original_locator`, `overlapping_delivery_preserves_uncertainty_and_single_port_call`, `three_expired_deliveries_record_convergence_fault_without_second_port_call`; storage receipt/publication kill tests |
| Redaction retains hashes/links without protected content | `authenticated_redaction_and_cross_tenant_principal_denials`, `proof_bundles_bind_receipts_and_authorize_every_member`, `worker_report_identity_mismatch_is_not_disclosed_as_bound_evidence` |
| Valid signature is provenance, never truth | `ab_wi_002_003_004_persist_retry_ack_and_discover` verifies real Ed25519 signing input while the receipt remains Uncertain; `detached_proofs_are_restricted_provenance_and_leave_receipts_immutable` |
| Campaign/arm cannot change authority, approval, connector, budget or priority | `runtime_campaign_inspection_preserves_authority_and_material_confounds`; domain `campaign_labels_are_attribution_and_material_differences_remain_visible` |
| Succeeded receipt has no winning-arm, binding-constraint, saturation, quality, continuity or acceptance fields | `wp016_runtime_receipt_retains_requested_advertised_applied_observed_missing_unknown` checks succeeded bytes and rejects extra claims |
| Material resource/policy/connector/environment/artifact/timing/uncertainty differences remain visible | `runtime_campaign_inspection_preserves_authority_and_material_confounds`; domain comparison test |
| WP-015: enclosing provenance separate from enforcement | `wp015_runtime_receipt_separates_enclosing_provenance_from_worker_enforcement`; worker `wp015_host_provenance_cannot_certify_worker_enforcement` |
| WP-016: requested/advertised/applied/observed/missing/unknown retained | `wp016_runtime_receipt_retains_requested_advertised_applied_observed_missing_unknown`; worker `wp016_missing_control_is_recorded_and_never_promoted` |

WP-A06 is resolved at receipt inspection: a canonical producer report is linked
through an authoritative receipt, checked against the exact source binding, and
preserves incomplete control evidence. An enclosing attestation cannot certify
worker enforcement. Inspection does not turn producer assertions into verified
enforcement or task acceptance. Real worker launch remains WIP-09/WIP-10.

## Crash matrix

| Boundary | Cuts and recovery assertions |
|---|---|
| Artifact/publication | Identified, durable bytes, before availability. A reader opened before the child cannot read incomplete outputs; recovery never infers provider success from bytes. |
| Receipt | After history, index, transaction commit. Recover all-or-none; stable one terminal receipt, exact retries, rebuild and backup/restore. |
| Domain update/outbox | Distinct `fabric-receipt-state` cut between effect-state and outbox writes, plus `fabric-receipt-outbox`; rollback or coherent commit. |
| Signal outbox | `process_death_after_wi_commit_recovers_aq_obligation` kills after Fabric commit, real AQ signal admission and local acknowledgment. Restart retains receipt, exact task/signal requests, one signal sequence and no replacement generation. |
| AQ attempt commit | Before/after disposition, overlap and three lease expiries retain receipt identity and provider-call count. The local synchronization channel is not signal evidence. |

Runtime and foundation helpers kill and reap children on timeout and assertion
failure. WIP-03 expiration, retired wake and subscriber recovery tests remain.

## Review and verification

Implementation correctness and a separate security self-review cover source
binding/canonicalization, publication ordering, authenticated disclosure,
per-member proof authorization and production crash-hook exclusion. Review and
verification artifacts are under run
`c4adeef1-d645-4497-8275-c3ba9d21dd7e/scratch/review` and `scratch/verification`.
These are implementation evidence; CraftingTable supplies qualifying independent
review of the exact candidate and target, including its security pass.

The selective port needed disclosure hardening beyond the parked code: receipt
ownership, requested-receipt membership, every bundle member's disclosure,
restricted detached-proof reads and exact worker report source checks. Additional
bundle evidence references require a separate exact disclosure decision; the new
host hook denies by default even when every receipt member permits Full disclosure. No
provider/signer authority is inferred from a proof or artifact.

The inventory additions were reviewed by source family: completion/inspection,
AQ routing/convergence, detached proof storage, conditional crashpoints and test
helpers. Existing dispositions remain; structural additions have explicit
dispositions rather than a hash-only refresh. Source hashes are refreshed after
edits. Rust 1.89.0, the supplied external build cache, crates.io AQ `=0.2.0` and
published checksum checks are retained.

WI-05, WI-WORKER-G2 and WI-AQ-G2 closure records become effective only on
reviewed integration with passing adopted checks. The controller performs that
integration; this implementation branch is neither pushed nor merged.

## Remediation of review 362d95d3

The clean inherited head was `9ff4ce8`; its independent review and separate
security pass reported no blocking or major findings. The recovery journal did
not supply reusable verification artifacts, so this remediation reruns checks.
The dispositions below are implementation claims for the next reviewer to verify.

| Finding | Disposition and review evidence |
|---|---|
| S-001 | Fixed: failures recording a post-Start observation fall back inline to an Uncertain receipt with a bounded reason artifact. Recording failures have a distinct possible-commit AQ code. `invalid_port_observations_commit_inline_uncertainty_without_reinvocation` covers contradictory, incomplete, oversized and missing evidence; `completion_clock_failure_records_inferred_uncertainty_with_effect_phase_times` covers clock failure. Unavailable storage can still prevent the fallback; the handler then reports `wi.effect.uncertain_recording_error` and reopen retains uncertainty convergence. |
| R-001 | Fixed: pre-Start errors use AQ's retryable disposition and durable, reference-only dispatch faults. Reopen records exhausted pre-Start runs without inventing an outcome or receipt. `expired_dispatch_authorization_retries_and_keeps_a_durable_diagnostic` exercises all three real AQ attempts and terminal reopen. |
| S-010 | Fixed: immediately before invocation, re-check Running, the original attempt, and absence of that attempt's outcome. Supersession cancels the handler's existing token. `invocation_fence_rechecks_after_supersession` pauses a process after durable Start, records uncertainty in a second delivery, then resumes it and proves no invocation. Cancellation remains cooperative; this check does not make the external port call atomic with a Fabric transaction. |
| S-012 | Fixed: terminal disposition requires the current attempt and immutable receipt source dispatch, AQ store, task, request digest, execution binding and observed run. Outcome writes re-check the delivering run against the current dispatch; historical convergence diagnostics bind their immutable receipt source revision across later retries. Authorized retries prepare a new dispatch while preserving the previous admission bytes. `retry_dispatch_does_not_reuse_the_previous_attempt_disposition` completes a later permitted dispatch and rejects an old run. |
| S-002 | Fixed: storage proof inspection now requires a separate evidence authorizer callback for every additional evidence reference, as well as receipt ownership. `proof_bundles_bind_receipts_and_authorize_every_member` tests storage and runtime denial and the exact grant. |
| S-004 | Fixed: proof-bundle, trace and telemetry identities are unique within `(tenant,id)`; proof lookup includes the tenant. Unknown and foreign-principal receipts both return unavailable. `proof_and_diagnostic_ids_are_unique_within_each_tenant` and `authenticated_redaction_and_cross_tenant_principal_denials` exercise these boundaries. |
| S-008 | Fixed: receipt admission and Start timestamps come from the effect journal, with submission as receipt arrival. Commit time is present only for ConfirmedCommitted. Inferred recovery observations carry an inferred assurance and a capped unknown-uncertainty sentinel, never a fabricated precise commit time. `successful_completion_preserves_distinct_journal_phase_times` and the clock-failure regression verify distinct phases. |
| S-003 | Fixed: the storage crate explicitly documents every public API as trusted-host persistence; snapshot, enumeration, append and ReceiptAccess construction require host authentication and authorization before disclosure. The runtime inspector remains the remote disclosure boundary. |
| S-005 | Fixed: cross-direction receipt identities reject writes and reopen. Plain effect transactions reject outcomes that bypass the atomic receipt transaction; every historical outcome must have exactly one authoritative receipt on reopen. `plain_effect_transaction_cannot_persist_a_receiptless_outcome` and `reopen_rejects_outcomes_without_receipts_and_cross_direction_receipt_ids` cover these invariants. |
| S-006 | Fixed: crash-feature validation scans every repository Cargo manifest, including examples, workspace dependencies, patch and replace tables, while excluding build output. Negative checker fixtures cover foreign normal/dev/workspace activation and storage overrides. |
| S-007 | Fixed: enabling crash-points without debug assertions fails compilation, including non-test shipping artifacts. All-features verification artifacts retain hooks; normal production feature exclusion is independently checked from Cargo compiler artifacts. |
| S-009 | Deferred: explicit routing metadata belongs to host composition. Current strict effect locators and text ingress payloads are disjoint, and both handlers validate durable bindings. No broader router change is included here. |
| S-011 | Fixed by operator documentation: older feature profiles and interim incompatible SQLite layouts fail closed without mutation. The `local-receipt-3` profile remains explicit and no migration is introduced. Persisted old-policy admission requests in a compatible store remain byte-identical. |
| S-013 | Fixed: completion lookups are fallible. A journal-derived, replay-validated run index selects one tenant/effect instead of enumerating every tenant and effect; observation conflicts reject transactionally. Rebuild, restore and process-kill tests validate the index along with other projections. |

Schema additions and scoped uniqueness are explicit reviewed source changes. The
store's exact-schema validation rejects incompatible interim stores rather than
silently upgrading them. Operators should preserve such stores for an explicit
future migration; this work does not rewrite them. Inventory additions cover the
run index diagnostic types, temporal lookup and regression helpers by source
family, with explicit dispositions and canonical source hash refresh.

## Remediation of review 2c045b50

The inherited head was `bfc80fc`, with a clean worktree and no untracked artifacts.
The source-run handoff confirms its ten adopted checks passed, but the separate
security pass identified S-014 as a major completion race. Those historical
results do not verify this remediation. The following are implementation claims
for the next independent reviewer, preserving the finding IDs.

| Finding | Disposition and executable evidence |
|---|---|
| S-014 | Fixed: publish outputs/evidence once, then retry journal conflicts or SQLite writer contention at most 32 times. Each retry takes a fresh tenant snapshot and clock, checks Running, the original attempt, no outcome for that attempt, and the delivering run in the current dispatch, and rebuilds the receipt transaction from the original published references. Journal phase times are scanned once before the retry window and bound to the same Start. `contended_completion_preserves_provider_outcome_and_published_artifacts` commits another same-tenant effect after the snapshot and advances the clock; both success and proven noncommit retain the original outcome, receipt and evidence without reinvocation. `completion_contention_budget_is_bounded_without_reinvocation` exhausts 32 conflicts and checks the uncertainty fallback. `contended_completion_preserves_a_superseding_uncertain_receipt` proves a second delivery's receipt cannot be overwritten by a retried success. |
| S-015 | Fixed: `wi.effect.pre_start_error` requires a positively read DispatchPending phase with no attempt. Failed binding, missing effect or unavailable phase returns terminal `wi.effect.phase_unknown`, which preserves possible commit. `unreadable_binding_never_claims_retry_safe_pre_start_failure` removes the input artifact before delivery both with and without an earlier durable Start; neither case retries or calls the provider. The existing expired-authority regression still proves three retries when the safe phase is known. |
| S-016 | Fixed: canceled pre-Start AQ runs append `wi.effect.pre_start_canceled`, distinct from failure exhaustion, and this code is validated on replay. Failed AQ runs with `wi.effect.uncertain_recording_error` or `wi.effect.phase_unknown` agree with a Fabric Uncertain outcome; other codes or queue states still record convergence faults. `canceled_pre_start_run_retains_cancellation_diagnostic_on_reopen` and `possible_commit_aq_codes_converge_without_spurious_faults` use real AQ terminal states and repeated reopen, including an unexpected-code negative control. |
| S-017 | Fixed: inferred completion/recovery time is only `observed_at`; `reconciled_at` remains absent because no Reconcile action occurred. The inferred assurance and capped uncertainty sentinel remain explicit. Both the clock-failure regression and terminal-run recovery regression assert this distinction. |
| S-009 | Deferred as accepted by review: explicit router metadata remains host-composition work. The current strict payload forms are disjoint, and durable binding validation fails closed. No optional router change is included. |

The deterministic interleaving clock has an explicit test-evidence inventory and
catalog disposition. All new regressions are non-ignored runtime tests mapped to
the existing WI-05 obligations in `completion/coverage.json`. No admission bytes,
queue policy, receipt schema or check definition changes are needed here.

Verification and separate correctness/security self-review evidence for this run
live under `a79fc357-c82e-4e8a-a29d-6101a32c9455/scratch/verification` and
`scratch/review`. Qualifying independent review, including its separate security
pass, remains with CraftingTable before WI-05 and G2 closure takes effect.


## Remediation of review 8f44c4c6

The inherited head is `3b1e3e3`. The initial status contained only a foundation
lockfile change removing unused AQ patch entries. The preceding implementation
journal ended clean; the following review journal and its preserved verification
script show supplemental Cargo runs. That generated lockfile change was saved to
this run's verification evidence and restored, without changing dependencies.
There were no untracked source files or test artifacts to reconcile.

These dispositions are claims for independent reviewer verification, superseding
the earlier optimistic-retry description above.

| Finding | Disposition and executable evidence |
|---|---|
| S-018 | Fixed: the new trusted-host `effect_transact_receipt_with` seam takes `BEGIN IMMEDIATE` before loading the tenant state, checking the original attempt/run fence, scanning phase times, sampling the clock and validating the Outcome command. Concurrent writes cannot stale that snapshot before receipt commit. Publication and the complete locked transaction retry transient SQLite contention up to eight times; SQLite's existing busy timeout supplies the wait, with no provider reinvocation. `sustained_tenant_writes_preserve_confirmed_completion_without_reinvocation` queues 40 same-tenant writes, beyond the old retry budget, and proves a competing writer is excluded during preparation. The success/noncommit artifact regression also probes that lock. `completion_retries_publication_contention_without_losing_provider_observation` holds an exclusive context lock beyond the 5-second busy timeout and retains the confirmed outcome and output. Supersession still preserves the winning uncertainty receipt. |
| S-019 | Fixed: validated commands retain their base state digest; storage emits the typed `EffectJournalConflict` marker on a mismatch before domain replay. Both Egress and completion use the typed/SQLite classifier, without matching error text. `effect::tests::effect_journal_faults_roll_back_every_projection` verifies actual stale transactions carry the marker and a text-only lookalike does not count as contention. Receipt exact-retry checking still precedes conflict checking. |
| S-021 | Fixed: terminal-run recovery uses the same locked receipt seam and bounded contention retries, including publication. Reopen passes the observed original attempt and terminal run into recovery; the lock protects their revalidation, so a superseded attempt cannot acquire a receipt. `terminal_recovery_waits_for_writers_and_preserves_original_attempt` overlaps real AQ terminal recovery with 40 same-tenant writes, retains one original-attempt Uncertain receipt across reopen, and proves a lost fence cannot invoke even the preparation callback. |
| S-020 | Deferred: phase_unknown diagnostics still combine unreadable storage with binding failures, and a failed pre-Start run can retain the exhausted label. This is diagnostic only; it neither grants authority nor invokes a provider. Keep a distinct terminal-code/allowlist cleanup as a follow-up. |
| S-009 | Deferred as accepted in prior reviews: explicit router metadata belongs to host composition. Payload forms remain strict and disjoint, with durable binding validation failing closed. |

The three added inventory types are explicitly dispositioned: the trusted-host
receipt preparation and conflict types remain in the storage family; the port
interleaving helper is test evidence. All added/renamed tests map to WI-05 crash
convergence in `completion/coverage.json`. No request bytes, queue constraints,
receipt wire format, SQLite layout or adopted check definitions change here.
Clock implementations must return promptly without storage reentry or provider
I/O because completion now samples the clock while holding the writer lock.
Permanent I/O failures or exhaustion of SQLite's contention waits can still
prevent recording; the existing explicit uncertainty behavior remains in force.

This run records a separate security self-review under `scratch/review`, covering
publication ordering, canonicalization, disclosure, tenant/attempt/run fences and
recovery authority. It does not replace CraftingTable's independent review and
separate security pass. Verification logs belong to this run's
`scratch/verification`; the controller alone integrates reviewed work.

## Remediation of review 80c831a5

The inherited head was `f6e4585`. Initial status, including untracked files, was
clean. The source review journal records completed verification, superseding the
older interrupted runs; no generated artifacts or intended new files needed
reconciliation. Historical checks do not verify this candidate.

These are implementation claims for the next independent reviewer.

| Finding | Disposition and executable evidence |
|---|---|
| S-022 | Fixed: reopen selects the journal's original Start run and attempt, checks that run's membership in the current dispatch and AQ terminal state, and skips other runs. The locked receipt seam returns no result on a Start/run mismatch before invoking preparation. `terminal_recovery_selects_start_among_multiple_terminal_runs` exercises two real terminal AQ runs mapped into the model's permitted multi-run shape and selects the lower-sorting Start. `terminal_recovery_uses_start_run_and_skips_other_observed_terminal_runs` leaves Start without terminal evidence while a higher-sorting observed run is terminal, and verifies repeated reopen remains available without fabricating a receipt. The existing contended recovery test still proves one original-attempt receipt when Start is terminal. |
| S-023 | Fixed: an expression index on tenant, the journal action's intent ID and revision lets phase-time queries seek only that intent's ordered history. `write_receipt` reuses the snapshot already held by the transaction rather than deserializing the tenant state twice. `effect::tests::phase_times_seek_one_intent_and_preserve_dispatch_and_tenant_boundaries` puts 2,000 unrelated entries in the same tenant, preserves the latest dispatch's phase times across preparation, separates same-ID foreign-tenant history and verifies the query plan uses the two-key index without a scan or temporary sort. Existing contention and crash/rebuild/restore tests continue to verify transactional ordering. |
| S-024 | Fixed: a bounded-retry read-only attempt/dispatch fence precedes output, evidence and boundary publication. A superseding receipt supplies the disposition immediately. The locked seam retains the final fence after publication; redundant checks inside its callbacks are removed. `contended_completion_preserves_a_superseding_uncertain_receipt` now also queries publication identities and asserts no stale port output, evidence or boundary publication exists. |
| S-020 | Deferred as accepted in review: diagnostic distinctions for phase_unknown remain a follow-up. Existing codes neither grant authority nor invoke providers. |
| S-009 | Deferred as accepted in review: explicit router metadata belongs to host composition; strict payloads remain disjoint and durable binding checks fail closed. |

The phase index is an explicit SQLite layout change within the interim
`local-receipt-3` profile. Exact-schema validation rejects pre-index interim
stores without modification; there is no automatic index creation, migration or
admission-request rewrite. Preserve such stores for a separately designed
migration. `effect::tests::missing_phase_index_rejects_interim_schema_without_mutation`
asserts repeated rejection leaves the database bytes unchanged. Newly created
compatible stores retain persisted old-policy admission bytes unchanged, as
covered by the existing admission regression.

This reduces work under the database-wide Fabric writer lock; it does not bound
tenant aggregate size or eliminate domain replay and artifact validation there.
Clock implementations must still return promptly without storage reentry. A
fence can also be lost during publication, so the final transactional check
remains necessary and that interleaving can still leave unreferenced artifacts.
The new preflight avoids publication when supersession is already observable.

New tests are non-ignored and registered in `completion/coverage.json`. Source
hashes are refreshed without changing inventory treatments or adopted check
definitions. Correctness and a separate security self-review are recorded under
this run's `scratch/review`; verification is under `scratch/verification`.
CraftingTable's independent review with its separate security pass remains due
before the controller integrates WI-05 and G2 closure.

## Remediation of review c9342461

The inherited head was `f3c78e5`. Initial status, including untracked files, was
clean. The source review journal and its preserved verification summary record
all ten adopted v6 checks passing and the reviewer restoring its generated
lockfile rewrite. There were no artifacts to remove or untracked sources to
stage. The operator subsequently required S-026 to be fixed in WIP-04; the
provisional interrupted reviews do not authorize integration.

These dispositions are implementation claims for the next independent reviewer.

| Finding | Disposition and executable evidence |
|---|---|
| S-026 | Fixed: `effect_receipt_source` seeks the existing `(tenant,intent,revision)` journal index, materializes only that intent's entries up to the receipt revision, and releases the read statement before canonicalization or interpretation. It reads the source dispatch ordinal, exact preparation snapshot/admission, original Start run/attempt and Outcome, without applying commands or digesting/cloning tenant state at each revision. Completion compares the source outcome, Start run, latest dispatch ordinal and immutable admission fields. The domain's unchanged deterministic request derivation is shared with completion, so the exact request and its execution digest still bind the original preparation snapshot. `receipt_source_seeks_one_intent_and_fences_revision_tenant_dispatch_and_start` excludes 2,000 unrelated same-tenant entries, tests foreign tenants and historical dispatches, rejects absent source revisions and damaged digests, and requires an indexed range query without a scan/sort. `retry_dispatch_does_not_reuse_the_previous_attempt_disposition` compares the new source facts against full replay for both dispatches. `committed_disposition_rejects_request_admission_and_outcome_binding_drift` rejects projection drift while retaining one receipt and one invocation. `completion_and_redelivery_bound_latency_and_writer_delay_with_long_history` batches 1,000 genuine domain-validated discovery entries, bounds completion and three redeliveries, and probes a concurrent other-tenant writer. |
| S-027 | Fixed: before the first port publication, the runtime checks the entire observation against the same domain bounds used for Outcome references: at most 64 outputs, 64 evidence blobs, and 16 MiB total output/evidence/boundary bytes. Checked addition rejects overflow. `oversized_observations_publish_no_port_artifacts` tests excess outputs/evidence, each oversized blob class and a combined overflow of the byte budget; every case leaves zero port publication obligations, records one bounded Uncertain receipt and never reinvokes. `outcome_artifact_bounds_include_boundary_bytes_and_checked_totals` covers exact bounds and arithmetic overflow. The total-byte budget is an explicit local completion-profile bound in this remediation, including boundary bytes and counting each reference; it is stricter than the previous individual-artifact-only bound. |
| S-025 | Fixed: the read-only pre-publication fence checks both the attempt and the original Start run. The locked completion seam retains its final recheck. `sibling_run_cannot_publish_or_reuse_the_start_runs_receipt` proves that an observed sibling cannot publish recovery artifacts, fabricate an outcome, invoke the port or use the original run's receipt. |
| S-020 | Deferred as accepted by review: distinct phase_unknown diagnostic labels remain a follow-up. Existing labels do not authorize or invoke providers. |
| S-009 | Deferred as accepted by review: explicit TaskRouter metadata remains host-composition work. Current strict payload forms remain disjoint and durable binding validation fails closed. |

The lookup relies on the domain-validated append-only journal and its validation
at store open; it is a trusted-host fact reader, not a command or authorization
constructor. Full journal replay remains available for offline validation,
rebuild and restore. Current-aggregate loading and ordinary domain command
validation still scale with tenant state; this change removes repeated tenant
history replay from committed completion and redelivery. The liveness regression
uses 1,000 journal entries, not a claim that every possible tenant size meets a
universal completion deadline.

The new storage binding type is explicitly retained in `effect-storage` and in
the existing catalog family. The latency test's reference to delay is an explicit
shared review surface for the legacy delay family; its test evidence is retained.
All six new regressions map to WI-05 obligations in `completion/coverage.json`;
WP-015/016 and the AB-WI-002/003/004 admission mappings remain in force. This
remediation changes no SQLite layout, receipt envelope, admission request bytes
or adopted check definition.

Verification and separate correctness/security self-review evidence for this run
are under `da04db16-14c6-4d48-b007-a0bd876211e1/scratch/verification` and
`scratch/review`. All ten adopted v6 checks must pass on the committed candidate.
CraftingTable's independent review, including its separate security pass, remains
due before integration; only the controller merges the reviewed result.

## S-028 operating envelope for the local proof runtime

Completion still deserializes, hashes and writes the tenant's aggregate state.
This implementation supports a bounded local proof workload. For this profile,
keep each tenant's serialized `effect_state.state` at or below **1 MiB**, each
intent's journal at or below **1,000 entries**, and effect attempt timeouts at
**30 seconds or longer** (`submission.budget.timeout_millis >= 30000`). Reserve
at least **10 seconds** of that timeout for Fabric completion, beyond the measured
provider execution time. The shared Fabric writer also requires bounded write
contention; do not schedule sustained concurrent mutation on the same store.
These are host operating limits, not new admission rules or automatic pruning.
The accepted 3/Idempotent request policy and old persisted requests are unchanged.

The host must inspect size before adding work to a tenant and monitor completion
latency on its deployment hardware. A trusted local diagnostic, against the
Fabric SQLite database, is:

```sql
SELECT tenant, length(state) AS state_bytes FROM effect_state;
SELECT tenant, json_extract(CAST(entry AS TEXT), '$.action.id') AS intent,
       count(*) AS journal_entries
FROM effect_history GROUP BY tenant, intent;
```

Stop admission to an over-limit tenant and retain its store for inspection; do
not delete journal rows, rewrite admission requests or fabricate a replacement
identity to recover capacity. Keep existing AQ work and recovery enabled. For
larger tenants or shorter deadlines, qualify a separate operating envelope on
the deployment hardware or implement per-intent/incremental state persistence
before expanding the supported workload. Retry/Start fencing preserves evidence
on a timeout, but is not a latency guarantee.

The previous independent review measured `e9adec1` at 2,282,418 state bytes:
1.15 seconds completion and 330 ms maximum writer wait; at 4,554,418 bytes:
2.03 seconds and 531 ms. Those measurements justify rejecting the 1-second
minimum as an operating assumption, not extrapolating the parser's 16 MiB cap
into a supported workload. Evidence is in review run
`63b6b3d7-ae8b-4b3f-938d-e88b3084e06b/scratch/verification/bench-s026-fill-e9adec1-quiet.txt`.
The limits above leave margin below those measured state sizes and within the
30-second fixture timeout; they are a conservative operating policy, not a
benchmark claim for arbitrary providers, hardware, artifacts or contention.

## S-030 compatibility of interim stores

The 16 MiB outcome limit counts **all** durable output, evidence and boundary
references together, including repeated references. It applies to journal replay
at open, rebuild and restore as well as new completions. An interim store whose
outcomes exceeded that total, even when every individual artifact was under
16 MiB, will fail validation. Such stores can share `local-receipt-3` with current
stores; that profile alone does not promise compatibility across this development
branch. No released store format is being migrated. Preserve incompatible stores
for inspection with their matching revision; there is no in-place migration,
outcome truncation or automatic rewriting of receipt or admission bytes.

The S-029 discovery rules likewise reject old journal histories that appended
unchanged discovery cursors or advanced discovery after Start. Compatible
histories keep their exact bytes. Back up any interim proof store before moving
between revisions; create new proof data on the new revision when its history is
incompatible.

## Remediation of the review on e9adec1

These dispositions are implementation claims for the next independent reviewer.

| Finding | Disposition and evidence |
|---|---|
| S-028 | Fixed by the bounded operating profile above, as the review recommended. Whole-tenant state cost remains explicit; no scalability claim is made. |
| S-029 | Fixed: unchanged cursors return the original state without a revision; discovery is allowed only in DispatchPending. `execution_times` materializes indexed rows and drops its statement before parsing. `discovery_noops_preserve_history_and_start_freezes_the_cursor` repeats real AQ discovery and direct cursor retries, rejects stale cursors and post-Start/post-outcome changes, and checks exact state/history counts and reopen. |
| S-030 | Fixed by the compatibility notice above, including open/rebuild/restore and same-profile interim data. |
| S-031 | Fixed: shared domain structural validation checks content-derived references before the first provider publication. The locked transaction repeats validation against the current manifest and attempt. `structurally_invalid_observations_publish_no_port_artifacts` covers contradictory claims, missing/empty evidence, missing/empty boundaries and wrong output hashes; all leave zero port publications and one Uncertain receipt without reinvocation. |
| S-032 | Fixed: the shared receipt writer requires exactly one newly appended Outcome. `receipt_transaction_rejects_a_command_without_a_new_outcome` rejects a valid Reconcile command through the receipt API without changing projections/receipts, then applies reconciliation normally and reopens successfully. Existing exact-retry tests preserve lost-response behavior. |
| S-020 | Deferred as accepted by review: distinct diagnostic labels for phase_unknown remain a follow-up; current labels do not authorize or invoke a provider. |
| S-009 | Deferred as accepted by review: explicit router metadata belongs to host composition; strict payloads and durable bindings continue to fail closed. |

The three new regression references are registered in completion coverage. Source
hash refresh preserves the reviewed inventory structure and dispositions. This
remediation changes no check definitions, SQLite layout, admission request format,
receipt format or crash cuts. Implementation verification and separate security
self-review are recorded under run
`e45f6541-e1f8-4d5f-ae3d-3fbe19726237/scratch/verification` and `scratch/review`.
CraftingTable's qualifying independent review and separate security pass remain
required before integration.
