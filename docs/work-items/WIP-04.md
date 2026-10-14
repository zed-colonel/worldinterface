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
