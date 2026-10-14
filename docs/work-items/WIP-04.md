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
