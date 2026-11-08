# WI-EMBEDDED-WORKER-PROOF-1 producer evidence and consumer handoff

This is the integration-only, WI-owned proof for `process-managed-proof-1`,
execution profile `WI-WORKER-EXECUTION-1`. The containing committed revision is
the producer source identity. Resolve its exact commit and tree in the external
execution bundle; this document cannot contain its own commit hash.

The proof covers authenticated admission, durable execution through embedded WI
and real crates.io ActionQueue **0.3.0**, lifecycle and crash convergence, scoped
artifacts and an asynchronous **seam simulation**. Foundation cases provide
AQ-free definition/domain evidence; runtime cases provide execution evidence;
checker cases provide independent discovery and negative validation. The campaign
runs every case, without reclassifying all prerequisite tests as embedded runs.

The ten gate records are prepared as closed, effective only on
**reviewed-merge-with-adopted-checks**. A build, worker exit, WI receipt, campaign
pass or generated handoff cannot grant that reviewed closure. Fresh execution
and all ten adopted checks on the unchanged candidate, independent review and
CraftingTable integration remain required. This document does not assert that a
future campaign has passed. Its machine-readable counterpart is
[`proof/handoff.json`](../../conformance/wi-fabric-2/proof/handoff.json).

No consumer task acceptance, ordinary-use admission, parent acceptance, G5,
release, remote parity, two-host qualification, strong isolation or hostile-code
qualification is supplied. `contracts/milestones.json` retains its historical
WI-01 state; the off-map proof does not advance full-parent/release obligations.
The legacy root workspace and daemon/CLI are historical and are outside this
embedded proof. There is no supported daemon, remote, Python or Platform worker
surface in this handoff.

## Identity and evidence binding

The descriptor contains SHA-256 identities for all three **active** lockfiles
(foundation, runtime and contract tool), Rust toolchain configuration, profile
YAML and prose, worker and AQ acceptance matrices, assumption ledger, adapter
sources, fixture/helper sources, coding repository fixture, build recipe and seam
implementation. `handoff check` recomputes these and compares the complete
case/test and gate records with their authoritative registry. `all check` also
compiles and lists the exact registered tests through `proof check`; discovery
establishes eligibility, never passage. `handoff generate` only renders the
current descriptor and cannot repair missing implementation or close a gate.

Rust is **1.89.0**; builds use locked offline Cargo and Linux x86_64 with pidfd,
close_range, sealed memfd, procfs, subreaper support and private writable scratch.
`contracts/aq-integration-target.json` separates two identities:

- Historical AQ 0.2.0: source commit
  `29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860`, conformance revision **16**,
  tree/archive/source digests and all eleven published crate checksums.
- Executed AQ 0.3.0: all six resolved runtime crates and published checksums,
  under the WIP-07 operator-authorized migration. No 0.2.0 execution is claimed;
  the historical source commit is not attributed to 0.3.0.

Both full records are copied into the descriptor and checked against the
contract. The runtime graph check requires crates.io 0.3.0 and its checksums;
path/patch overrides are forbidden. The external receipt resolves clean
commit/tree before and after execution, rustc/Cargo versions, lock/contract
hashes, compiled test binaries, individual exact-test logs and outcomes, all 55
case results, additional runtime outcomes, OS/kernel/architecture, execution
identity, exact approval, fixture/helper build hashes and three observations.
The generated external `handoff.json` binds the descriptor digest, receipt and
producer evidence. `handoff verify DIRECTORY` verifies these against current
source and compiled binaries. Preserve the source revision, external cache and
all evidence files for reproduction; scratch storage may expire. Same-user
mutation of source, binaries or evidence is outside an adversarial trust model;
these checks detect inconsistent bytes, not truthful authorship by themselves.

The hash graph is acyclic: the packet hashes this document and descriptor;
the descriptor names packet/approval/anchor paths without their hashes. Concrete
packet, approval, executable, environment and commit pins are external. Neither
WIP-15's historical candidate nor a descriptor alone substitutes for fresh
WIP-16 execution.

## Reproduction and acceptance

Run at the repository root with the supplied external `CARGO_TARGET_DIR` and
`TMPDIR`; evidence belongs in `$TMPDIR/verification`, outside source/build output.
Fetch first if dependencies are absent (`cargo fetch --locked --manifest-path`
for each of the three active manifests). Do not resolve runtime to satisfy a
foundation check. Before presenting a changed packet:

```sh
cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh
cargo run --locked --offline --manifest-path tools/wi-contract-check/Cargo.toml -- handoff generate > "$TMPDIR/verification/handoff-descriptor.json"
cp "$TMPDIR/verification/handoff-descriptor.json" conformance/wi-fabric-2/proof/handoff.json
python3 scripts/refresh-wi-worker-evaluation.py
scripts/build-wi-worker-evaluation.sh
```

The builder compiles/tests source-bound artifact publication without launching
worker fixtures. It records canonical packet and compiler-attested fixture/helper
hashes in `$TMPDIR/verification/revised-packet/worker-build.json`. Present that
exact packet digest and build evidence to the operator, with a finite expiry no
more than seven days after actual acceptance. Record only the actual decision
in `evaluation-approval.json` and its separate trusted host anchor. Those trust
records cannot widen sources, tasks, cases, operations, limits or environment.
Changed packet bytes invalidate inherited acceptance even before expiry.

WIP-15's receipt expires **2026-11-11T00:00:00Z** and authorizes its old packet
only. WIP-16 requires renewed acceptance after non-live validation. Positive
execution and subsequent adopted `runtime-test` fail closed on missing, drifting,
revoked or expired approval, using system wall time. Complete integration and
post-merge live verification before the accepted expiry or renew exact acceptance
first. An earlier passing campaign is no authorization for a later run.

Commit the complete accepted candidate. Run each of the ten adopted checks on
that unchanged committed head through CraftingTable's `ct-check --declared ID`:

```sh
for check in foundation-fmt foundation-clippy foundation-test foundation-isolation \
  runtime-fmt runtime-clippy runtime-test runtime-aq-graph contract-check contract-test
do
  ct-check --declared "$check" || exit
done
scripts/worker-proof-evidence.sh "$TMPDIR/verification/wip-16-campaign"
timeout --kill-after=15s 600s cargo run --locked --offline \
  --manifest-path tools/wi-contract-check/Cargo.toml -- handoff verify \
  "$TMPDIR/verification/wip-16-campaign"
```

The adopted definitions in `.craftingtable/checks.json` govern; direct equivalents
and this helper are supplemental. The adopted runtime test runs the same full
campaign once, with a 1785-second inner timeout plus 15-second kill grace under
its unchanged 1800-second outer limit. Diagnose a slow/failing exact test alone
under a hard timeout before repeating suites. The helper requires an empty
external directory and additionally verifies the generated publication bundle.
Logs, observations, receipt, handoff and timings stay external, never committed.

## Embedded consumer guide

The supported composition is `crates/worldinterface-foundation/Cargo.toml`
(AQ-free domain, protocol, storage, worker, capability and ingress) plus
`crates/worldinterface-runtime/Cargo.toml` (real AQ). The contract tool is an
independent maintenance workspace, never a runtime dependency. See
`crates/worldinterface-runtime/tests/support/embedded_consumer.rs` for a consumer
using only the application facade, and `embedded_setup.rs` for trusted host
provisioning. The boundary test prevents consumers from reaching internal stores,
provider handles, helper internals or raw queue authority.

1. The trusted host creates/opens a compatible `StoreBundle`, installs explicit
   authentication, evidence, resolver, clock, configuration and credential-custody
   ports through `EmbeddedPorts`, and calls `EmbeddedHost::install`. The host
   owns bootstrap administration, private workspace, reviewed fixture/helper,
   evaluation verifier and finite slots. Exclusive host ownership and store
   recovery precede `application()`. `WorkerInstallation::Recovery` can inspect
   and reconcile existing work without launching an executable or minting approval.
2. The authenticated application calls `submit(session, EffectSubmission,
   DirectOperationSelection)` or `submit_flow(session, definition, submission)`.
   Caller scope comes from the installed authentication port; request tenant or
   principal fields confer no authority. `control` uses the same authorization
   boundary for configuration and administration. Persist the admission request
   before AQ dispatch. `advance(session).await` drains authoritative WI
   observations/outbox and advances the single AQ engine; wakeups cause state to
   be reloaded and authorized, not acceptance inferred from a signal.
3. Import nonsensitive bytes with `import_artifact`; reference them by content
   digest and scope. Reads use `read_artifact(session, ObjectRef)` and are scoped
   and authorized. Bind reviewed executable, helper, configuration, input and
   export manifests by digest, not user-supplied paths. Publication validates
   staged content/size and traversal, symlink, hardlink and replacement races;
   collection is durably committed before completion. An export failure or
   unknown publication stays unresolved/quarantined until reconciliation.
4. Use `inspect_flow`, `inspect_worker`, `inspect_worker_evidence` and `receipt`
   for scoped status, sanitized controls/cleanup and provenance. Credentials use
   exact run/audience/operation scope, short-lived custody-safe references and
   the application credential preparation/reconciliation APIs. No secret bytes
   belong in logs, receipts, Flow documents or exports. `reconcile_effect` and
   `reconcile_credential` reload authoritative durable state; uncertain external
   delivery is never blindly repeated.
5. Supported operations are **provision, start, inspect, terminate,
   collect_artifacts and release**. Start-acknowledgement loss is uncertainty;
   reconcile before replacement. Duplicate and early/reordered observations must
   retain exact route/identity and stable replay semantics. Cancel is a request,
   not proof of termination. Release capacity only after confirmed exit and
   cleanup. Keep concurrency-key reservations through cancellation, export
   failure, quarantine and uncertainty. Worker success and WI receipts are
   execution evidence; the consumer must check its own outputs and task outcome.

The current process `Task` enum accepts reviewed Coding, Transform, Probe,
Sleep, Flood, Tree and Churn fixtures with typed finite parameters. The evaluation
packet further allowlists exact task digests and cases. Supplying an FFmpeg path
is not a media adapter: arbitrary executable arguments, inputs and workload
support require further consumer integration, exact source/configuration/fixture
review, appropriate approval and consumer validation. `process-managed-proof-1`
permits reviewed executables and reviewed nonsensitive inputs, never hostile code.
Evaluation-only registration/qualification uses trusted operator acceptance;
ordinary use needs a separate exact WIP-12 catalog registration, qualification
and approval record from trusted operator/evidence ports. Evaluation cannot
self-promote or reuse unrelated approval; drift/revocation fences new admissions.

Flow uses at most **64 explicit nodes**, with bounded static expansion, at most
32 ports per plan element and no unbounded dynamic fan-out. AQ limits are 64
initial runs, dependencies and children, 32 emitted signals per disposition,
hierarchy depth 8, one active wait per run and 64 KiB payloads. WI's own canonical
plan/envelope limits can reject a large boundary-node graph before AQ's node
ceiling. The coordinator uses a bounded 1 MiB disposition budget. Persist WI
state/outbox before wake, retain correlated early events, and use inclusive AQ
deadlines. Backup/restore must include both stores, WAL/artifacts and control
history; AQ automatic snapshot replay trusts the skipped WAL prefix. Full-history
integrity audits require the WIP-07 procedure, not just ordinary startup.

## Controls, limitations and residual risks

Requested controls, provider advertisements, applied controls and observed
measurements remain separate in the execution evidence. Declarations alone
cannot establish enforcement. The process adapter checks source-bound executable
and input identity, clears the environment, restricts descriptors and arguments,
applies a finite wall timer and bounds retained output. Tests measure applied
and observed wall/output limits separately and record TERM/KILL counts,
descendants, retained bytes and confirmed cleanup. Evaluation limits are
**15000 ms** and **65536 bytes**. Profile artifact bounds are one input up to
1 MiB and four exports up to 4 MiB aggregate; evaluation restricts retained
output/exports further. Export manifests are limited to 64 KiB.

CPU, memory, process count, filesystem, network and independent VM containment
are **unavailable**. A private scratch directory, cleared environment, sealed
inputs, process groups and reaping do not prevent same-OS-identity access,
co-residency interference, host filesystem/network access or resource exhaustion.
No user namespace, cgroup, container/Kata or VM boundary is proven. The supervisor
exception remains the reviewed exact-source disposition, not a general-purpose
process/continuation permission. No production credentials or sensitive inputs
are permitted by this evaluation.

The seam is a WI-owned asynchronous **seam simulation**, exchanging bounded
messages with no PID, workspace or store handle. Its process controls remain
**null/unknown**, not inherited from the local provider. Recovery-only inspection
without live observations also returns unknown controls/cleanup. This supplies
portable routing, acknowledgement and artifact evidence, not remote execution,
Mac inference gateway qualification or isolation.

## Slopinator P1 handoff

| Consumer obligation | Producer evidence and retained consumer work |
|---|---|
| P1-01 | Pin containing WI commit/tree, three locks, Rust/AQ identities, profile, adapter, fixture/helper and recipe digests; verify G1–G4 and execution bundle. Independently review/admit the media executable, configuration and nonsensitive input policy. |
| P1-02 | WP-029 and WP-027/028 show authorized embedded work and scoped artifacts/receipts. Run a real consumer media work order and verify its input/output identities and business result. |
| P1-03 | WP-017–028 cover uncertain start, callbacks, interruption, cancellation, export failure, reconciliation and cleanup. Reproduce the applicable failures in the consumer integration. |
| P1-04 | WIP-06/07/08 and AB-WI-005–011 cover bounded Flow/AQ scheduling, waits and outbox durability. Compile and validate the actual render graph within those limits. |
| P1-05 | WP-033/034 keep completion nonsemantic; AB-WI-021 retains capacity until cleanup. Independently accept task/media results and hold accelerator capacity until confirmed cleanup. |

## Review provenance

The accepted WIP-16 design is CraftingTable run
`11e3aabb-001b-4e23-a675-1450f334de68`, recovering
`76beb8b6-2fa6-4fe2-b42f-30369f23d633`; implementation preparation is run
`5f81f4d2-2cf6-4cb6-9e0f-8ea8f5dee8ab`. These are provenance, not fabricated
reviewer approvals or passing execution. The independent exact-head review and
reviewed-merge verification are CraftingTable records. WIP-15's historical
results, receipts, cold timing and finding dispositions are preserved in
`docs/work-items/WIP-15.md` and `WIP-15-verification.md`, not relabelled as WIP-16.

## Gate, case and exact test mapping

The following publication snapshot reproduces gate pass criteria and required
cases. The descriptor additionally includes complete prerequisites, bindings,
ownership and effectiveness. The exact test table below uses F =
`crates/worldinterface-foundation/Cargo.toml`, R =
`crates/worldinterface-runtime/Cargo.toml`, C =
`tools/wi-contract-check/Cargo.toml`; each entry is **manifest · package · exact
harness test name**. The registry is authoritative; edits require regenerating
both this publication snapshot and the descriptor, then renewed packet acceptance.

| Gate | Pass criteria | Required cases |
|---|---|---|
| WI-AQ-G1 | Target reference, data/artifact and causal mappings reconciled.<br>Fresh persistence and rejection/replay checks pass.<br>wi reviews its own mappings and applicable assumptions; a similarly named gate for another consumer is not an automatic pass.<br>Historical AQ-NN references identify retained capability/evidence obligations of the completed source; they are not runnable work. Review actual consumer candidate-branch fixtures on the exact accepted AQ build; independent gate attestation is still required and is not inferred from source presence. | AB-WI-001, AB-WI-015, AB-WI-016 |
| WI-AQ-G2 | Created/AlreadyExists preserve durable identities; mismatched digest conflicts.<br>Admission and local outbox retry behavior pass race/crash tests.<br>wi reviews its own mappings and applicable assumptions; a similarly named gate for another consumer is not an automatic pass.<br>Historical AQ-NN references identify retained capability/evidence obligations of the completed source; they are not runnable work. Review actual consumer candidate-branch fixtures on the exact accepted AQ build; independent gate attestation is still required and is not inferred from source presence. | AB-WI-002, AB-WI-003, AB-WI-004 |
| WI-AQ-G3 | Early/late/duplicate signals, wait establishment, deadline/cancel races and restart converge.<br>Checkpoint and matched-signal references survive; no process-local fallback.<br>wi reviews its own mappings and applicable assumptions; a similarly named gate for another consumer is not an automatic pass.<br>Historical AQ-NN references identify retained capability/evidence obligations of the completed source; they are not runnable work. Review actual consumer candidate-branch fixtures on the exact accepted AQ build; independent gate attestation is still required and is not inferred from source presence. | AB-WI-006, AB-WI-007, AB-WI-008, AB-WI-009, AB-WI-010, AB-WI-011, AB-WI-022 |
| WI-AQ-G4 | Lease-fenced dispositions and atomic child-admission/parent-wait behavior pass.<br>Completion, cancellation, and output publication recover without semantic reinterpretation.<br>wi reviews its own mappings and applicable assumptions; a similarly named gate for another consumer is not an automatic pass.<br>Historical AQ-NN references identify retained capability/evidence obligations of the completed source; they are not runnable work. Review actual consumer candidate-branch fixtures on the exact accepted AQ build; independent gate attestation is still required and is not inferred from source presence. | AB-WI-005, AB-WI-012, AB-WI-013, AB-WI-014, AB-WI-020, AB-WI-023 |
| WI-EMBEDDED-CORE-1 | Real embedded protocol/store/ingress/intent/receipt/executor/Flow/continuation conformance on exact AQ/WI implementation and fixture pins; no mock substitutes.<br>Consumer assumption reconciliation covers the actual embedded surfaces. This is a newly scoped evidence barrier, never an alias for EXO-WI-G3 or accepted WI-06/07/08 parents.<br>Parent-owned worker and supervisor cases remain mandatory at full parent acceptance; production activation needs ordinary authority and qualified profiles. | AB-WI-001, AB-WI-002, AB-WI-003, AB-WI-004, AB-WI-005, AB-WI-006, AB-WI-007, AB-WI-008, AB-WI-009, AB-WI-010, AB-WI-011, AB-WI-012, AB-WI-013, AB-WI-014, AB-WI-015, AB-WI-016, AB-WI-020, AB-WI-022, AB-WI-023 |
| WI-EMBEDDED-WORKER-PROOF-1 | All named checkpoint evidence is reviewed and valid on one compatible bound generation.<br>This partial milestone does not accept parent work items, publish a release, or qualify unimplemented platform/external/hostile-code profiles. | AB-WI-001, AB-WI-002, AB-WI-003, AB-WI-004, AB-WI-005, AB-WI-006, AB-WI-007, AB-WI-008, AB-WI-009, AB-WI-010, AB-WI-011, AB-WI-012, AB-WI-013, AB-WI-014, AB-WI-015, AB-WI-016, AB-WI-019, AB-WI-020, AB-WI-021, AB-WI-022, AB-WI-023, WP-001, WP-002, WP-003, WP-004, WP-005, WP-006, WP-007, WP-008, WP-009, WP-010, WP-011, WP-012, WP-013, WP-014, WP-015, WP-016, WP-017, WP-018, WP-019, WP-020, WP-021, WP-022, WP-023, WP-024, WP-025, WP-026, WP-027, WP-028, WP-029, WP-030, WP-031, WP-032, WP-033, WP-034 |
| WI-WORKER-G1 | WI-ADR-016 and referenced authorization/evidence ADR clauses reviewed; limitations, field mapping and operation capability contract frozen.<br>Source-owned evidence cases are bound in acceptance_coverage; owner PR IDs attribute work and are not automatic accepted-parent prerequisites. | WP-001, WP-002, WP-003, WP-004, WP-005, WP-006, WP-007, WP-008 |
| WI-WORKER-G2 | Exact-profile admission, unsupported-control denials and worker/instance evidence separation proven in producer fixtures.<br>Source-owned evidence cases are bound in acceptance_coverage; owner PR IDs attribute work and are not automatic accepted-parent prerequisites. | WP-009, WP-010, WP-011, WP-012, WP-013, WP-014, WP-015, WP-016 |
| WI-WORKER-G3 | Reviewed processes supervised via real WI/AQ; idempotent start, process ownership, event durability and termination/recovery proved.<br>Source-owned evidence cases are bound in acceptance_coverage; owner PR IDs attribute work and are not automatic accepted-parent prerequisites. | AB-WI-021, WP-017, WP-018, WP-019, WP-020, WP-021, WP-022, WP-023, WP-024, WP-025, WP-026 |
| WI-WORKER-G4 | WI-owned embedded harness, artifacts and external-provider seam pass without EXO/platform/Kata dependency or inflated assurance claims.<br>Source-owned evidence cases are bound in acceptance_coverage; owner PR IDs attribute work and are not automatic accepted-parent prerequisites. | AB-WI-019, WP-027, WP-028, WP-029, WP-030, WP-031, WP-032, WP-033, WP-034 |

| Case | Exact executable references |
|---|---|
| WP-001 | `F · worldinterface-worker · wp_001_profile_and_binding_drift` |
| WP-002 | `F · worldinterface-protocol · wp002_process_image_and_enclosing_evidence_are_distinct`<br>`F · worldinterface-protocol · wp002_existing_seams_bind_process_configuration_and_control_artifacts`<br>`F · worldinterface-protocol · wp002_rejects_mismatched_profile_and_resolver_bindings` |
| WP-003 | `F · worldinterface-worker · wp_003_independent_vm_cannot_inherit_enclosing_kata` |
| WP-004 | `F · worldinterface-worker · wp_004_opaque_consumer_artifacts_and_controls` |
| WP-005 | `F · worldinterface-worker · wp_005_lifecycle_and_unavailable_operations` |
| WP-006 | `F · worldinterface-worker · wp_006_aq_free_portable_contract_and_strict_codec` |
| WP-007 | `R · worldinterface-runtime · catalog_ordinary_records_require_provenance_and_never_enable_or_self_promote`<br>`R · worldinterface-runtime · catalog_registration_is_exact_bounded_and_inspection_grants_no_authority`<br>`R · worldinterface-runtime · wp_007_exact_operator_packet_runs_only_supported_bounded_evaluation_through_wi_aq`<br>`C · wi-contract-check · qualification_drift_and_revocation`<br>`C · wi-contract-check · quarantine_and_discovery`<br>`C · wi-contract-check · wp007_unqualified_and_evaluation_table` |
| WP-008 | `F · worldinterface-worker · wp_008_mediation_does_not_elevate_containment` |
| WP-009 | `F · worldinterface-egress · wp009_exact_fixture_durable_intent_and_admission`<br>`F · worldinterface-egress · ordinary_provider_cannot_reuse_fixture_proof`<br>`F · worldinterface-egress · resolution_failures_are_durable_without_running`<br>`F · worldinterface-egress · canonical_resource_grammar_and_namespace_specific_bounds`<br>`F · worldinterface-egress · identical_request_recovery_conflicts_and_scoped_inspection`<br>`F · worldinterface-egress · lost_ack_reopens_identical_request_without_extra_use`<br>`F · worldinterface-egress · forged_acknowledgments_cannot_complete_outbox`<br>`F · worldinterface-egress · concurrent_submissions_and_logical_use_races`<br>`F · worldinterface-egress · commit_evidence_rejects_false_success_failure_and_missing_publication`<br>`F · worldinterface-egress · successful_boundary_requires_durable_output_evidence`<br>`F · worldinterface-egress · uncertainty_reconciliation_and_retry_require_noncommit_policy_and_authority`<br>`F · worldinterface-egress · reconciliation_cannot_change_confirmed_commit_or_skip_evidence`<br>`F · worldinterface-egress · cancellation_before_dispatch_and_after_handoff_are_distinct`<br>`F · worldinterface-egress · compensation_is_a_separate_intent_without_inherited_authority`<br>`F · worldinterface-egress · backup_rebuild_replay_and_generic_bypass_integrity`<br>`F · worldinterface-egress · unsupported_store_profile_is_rejected_without_upgrade`<br>`F · worldinterface-egress · removing_authority_never_increases_allowed_demands`<br>`F · worldinterface-egress · deterministic_command_replay_preserves_state_and_rejects_stale_commands`<br>`F · worldinterface-egress · resolution_attests_symlinks_dns_redirects_and_exact_audience`<br>`F · worldinterface-egress · unpublished_output_cannot_be_marked_successful_or_survive_reopen`<br>`F · worldinterface-egress · concurrent_conflicting_requests_commit_only_one_intent`<br>`F · worldinterface-storage · effect::tests::effect_journal_faults_roll_back_every_projection`<br>`F · worldinterface-egress · ordinary_qualification_is_exact_and_revocable`<br>`R · worldinterface-runtime · wp009_runtime_exact_qualification_admits_once_without_execution_success`<br>`R · worldinterface-runtime · self_initiated_and_external_requests_follow_the_same_authorization_path` |
| WP-010 | `F · worldinterface-egress · wp010_unreviewed_executable_cannot_be_waived`<br>`F · worldinterface-egress · approval_and_separation_of_duties_are_bound_to_exact_inputs`<br>`F · worldinterface-egress · purpose_candidate_and_lifecycle_labels_never_supply_authority`<br>`R · worldinterface-runtime · wp010_runtime_generated_executable_denied_despite_exact_approval_and_purpose` |
| WP-011 | `F · worldinterface-egress · wp011_protected_credentials_and_cross_run_denied`<br>`F · worldinterface-egress · wp011_test_credential_is_exact_and_short_lived`<br>`F · worldinterface-egress · agency_forgery_ambiguity_expiry_revocation_and_attenuation`<br>`F · worldinterface-egress · delegation_endpoints_cycles_and_widening_fail_closed`<br>`F · worldinterface-egress · inspection_rejects_missing_and_contradictory_evidence`<br>`R · worldinterface-runtime · wp011_runtime_protected_input_and_credentials_cannot_admit`<br>`R · worldinterface-runtime · wp011_runtime_test_credential_is_exact_short_lived_and_inspection_is_scoped`<br>`R · worldinterface-runtime · caller_claims_cannot_override_authenticated_agency_or_grants`<br>`R · worldinterface-runtime · production_facade_authenticates_verifies_and_rechecks_live_target` |
| WP-012 | `F · worldinterface-egress · wp012_each_unmet_control_names_requirement_and_never_dispatches`<br>`F · worldinterface-egress · unknown_control_and_revoked_qualification_fail_closed`<br>`R · worldinterface-runtime · wp012_runtime_each_mandatory_control_denies_without_downgrade` |
| WP-013 | `F · worldinterface-egress · wp013_unsupported_operation_is_unavailable`<br>`F · worldinterface-egress · ordinary_unsupported_operations_are_unavailable_without_substitution`<br>`R · worldinterface-runtime · wp013_runtime_ordinary_unsupported_operations_are_unavailable` |
| WP-014 | `F · worldinterface-egress · wp014_every_snapshot_binding_drift_requires_reevaluation`<br>`F · worldinterface-egress · profile_change_requires_explicit_exact_reevaluation`<br>`R · worldinterface-runtime · wp014_runtime_drift_blocks_initial_and_prepared_delivery`<br>`R · worldinterface-runtime · wp014_runtime_explicit_reevaluation_cannot_rebind_persisted_request` |
| WP-015 | `F · worldinterface-worker · wp015_host_provenance_cannot_certify_worker_enforcement`<br>`R · worldinterface-runtime · wp015_runtime_receipt_separates_enclosing_provenance_from_worker_enforcement` |
| WP-016 | `F · worldinterface-worker · wp016_missing_control_is_recorded_and_never_promoted`<br>`R · worldinterface-runtime · wp016_runtime_receipt_retains_requested_advertised_applied_observed_missing_unknown` |
| WP-017 | `R · worldinterface-runtime · wp_017_admitted_worker_intent_precedes_bounded_process_and_wakes_are_durable` |
| WP-018 | `R · worldinterface-runtime · campaign_embedded_cancellation_and_output_bounds_keep_capacity_until_cleanup`<br>`R · worldinterface-runtime · wp_018_duplicate_admission_and_start_preserve_one_immutable_run` |
| WP-019 | `R · worldinterface-runtime · campaign_embedded_cancellation_and_output_bounds_keep_capacity_until_cleanup`<br>`R · worldinterface-runtime · wp_019_and_wp_024_killed_lifecycle_boundaries_quarantine_without_replacement`<br>`R · worldinterface-runtime · wp_019_helper_death_retains_capacity_without_a_cleanup_or_success_claim` |
| WP-020 | `R · worldinterface-runtime · wp_020_mismatched_and_damaged_private_ownership_never_adopts_an_unrelated_run`<br>`R · worldinterface-runtime · worker_process::ownership::tests::recycled_pid_simulation_never_adopts_or_signals_unrelated_child` |
| WP-021 | `R · worldinterface-runtime · wp_021_stop_obligation_precedes_bounded_descendant_cleanup` |
| WP-022 | `R · worldinterface-runtime · wp_022_wall_and_output_limits_are_enforced_after_real_admission` |
| WP-023 | `R · worldinterface-runtime · wp_023_helper_and_fixture_exclude_deliberate_host_descriptors_and_environment` |
| WP-024 | `R · worldinterface-runtime · wp_024_worker_state_and_event_are_authoritative_before_aq_wake`<br>`R · worldinterface-runtime · wp_019_and_wp_024_killed_lifecycle_boundaries_quarantine_without_replacement` |
| WP-025 | `R · worldinterface-runtime · wp_025_reordered_duplicate_late_and_cancel_racing_events_cannot_revive_work`<br>`R · worldinterface-runtime · wp_025_real_worker_events_preserve_aq_deadline_cancel_and_restart_winners`<br>`F · worldinterface-storage · concurrent_worker_observations_allocate_revisions_with_operation_acknowledgement` |
| WP-026 | `R · worldinterface-runtime · wp_026_unavailable_operations_are_not_registered_or_decoded_as_available_effects` |
| WP-027 | `R · worldinterface-runtime · wp_027_scoped_transform_import_and_durable_export_through_real_wi_aq`<br>`R · worldinterface-runtime · wp_027_coding_patch_and_schema_report_publish_before_completion`<br>`R · worldinterface-runtime · wp_027_publication_requires_exact_binding_and_precedes_completion_across_reopen`<br>`R · worldinterface-runtime · wp_027_empty_exports_still_require_a_validated_published_collection` |
| WP-028 | `R · worldinterface-runtime · wp_028_invalid_batches_quarantine_before_any_publication_and_survive_reopen`<br>`R · worldinterface-runtime · wp_028_unknown_and_failed_trusted_verifiers_ignore_provider_pass_claims`<br>`R · worldinterface-runtime · wp_028_cancelled_prepared_collection_cannot_publish_and_cleanup_keeps_report`<br>`R · worldinterface-runtime · wp_028_snapshot_and_publication_crash_cuts_recover_exactly_or_quarantine`<br>`R · worldinterface-runtime · wp_028_missing_or_corrupt_prepared_snapshot_quarantines_without_recollection`<br>`R · worldinterface-runtime · wp_028_live_collection_ack_receipt_and_aq_crashes_preserve_publication_order`<br>`R · worldinterface-runtime · worker_process::artifact::tests::export_links_special_files_and_replacement_races_reject`<br>`R · worldinterface-runtime · artifact_integrity_symlink_hardlink_and_special_files_are_quarantined`<br>`R · worldinterface-runtime · wp_028_unsafe_live_exports_quarantine_and_report_through_real_wi_aq`<br>`R · worldinterface-runtime · wp_028_unavailable_seam_export_quarantines_after_provider_ack_through_real_wi_aq` |
| WP-029 | `R · worldinterface-runtime · wp_029_embedded_coding_and_transform_use_one_engine_and_application_services`<br>`R · worldinterface-runtime · wp_029_restored_publication_is_readable_without_original_workspace_and_keeps_capacity`<br>`R · worldinterface-runtime · wp_029_embedded_crash_recovery_quarantines_without_replacement` |
| WP-030 | `R · worldinterface-runtime · wp_030_consumer_and_helper_imports_stay_inside_application_boundary`<br>`R · worldinterface-runtime · wp_030_embedded_forgery_scope_and_unauthorized_controls_fail_closed`<br>`R · worldinterface-runtime · wp_030_recovery_only_facade_enforces_disclosure_revocation_and_exclusive_ownership`<br>`R · worldinterface-runtime · wp_030_catalog_quarantine_and_revocation_fence_queued_execution`<br>`R · worldinterface-runtime · wp_030_recovery_reconstructs_scoped_metadata_before_returning_application` |
| WP-031 | `R · worldinterface-runtime · wp_031_seam_simulation_delayed_reordered_observations_and_uncertain_ack_use_real_wi_aq`<br>`R · worldinterface-runtime · wp_034_embedded_seam_handoff_retains_nonsemantic_completion_and_unknown_controls`<br>`R · worldinterface-runtime · campaign_unknown_controls_publish_once_with_exact_observation_digest` |
| WP-032 | `R · worldinterface-runtime · catalog_invalid_recovery_policy_denies_before_any_provider_read_or_stop`<br>`R · worldinterface-runtime · catalog_quarantine_fences_stale_admission_and_survives_cleanup_replacement_rebuild_and_reopen`<br>`R · worldinterface-runtime · catalog_quarantine_serializes_with_dispatch_and_fences_every_later_dispatch`<br>`R · worldinterface-runtime · catalog_recovery_is_exact_to_caller_binding_operation_and_current_policy`<br>`R · worldinterface-runtime · catalog_recovery_only_construction_requires_no_approval_or_executable_files`<br>`R · worldinterface-runtime · catalog_registration_is_exact_bounded_and_inspection_grants_no_authority`<br>`R · worldinterface-runtime · wp_032_drift_denies_new_admissions_before_allocation_through_real_wi_aq`<br>`R · worldinterface-runtime · wp_032_quarantine_revocation_and_recovery_only_reopen_keep_inspect_terminate_and_aq_reconcile`<br>`R · worldinterface-runtime · catalog_poll_invalid_approval_quarantines_only_the_run_and_preserves_provider_admission`<br>`R · worldinterface-runtime · wp_032_ordinary_terminate_under_provider_quarantine_acknowledges_and_reconciles` |
| WP-033 | `F · worldinterface-storage · control_distinct_fresh_issuances_wait_for_delivery_ownership`<br>`F · worldinterface-storage · control_plane::tests::shared_effect_and_credential_reservations_have_one_atomic_last_use`<br>`R · worldinterface-runtime · ab_wi_019_invalid_restore_never_publishes_partial_destination`<br>`R · worldinterface-runtime · ab_wi_019_two_independent_stores_restore_complete_wal_artifacts_and_control_history`<br>`R · worldinterface-runtime · f007_crash_cuts_reconcile_exact_ownership_without_redelivery`<br>`R · worldinterface-runtime · f007_expiry_rotation_and_recovery_preserve_the_original_credential_generation`<br>`R · worldinterface-runtime · f007_live_delivery_excludes_recovery_and_concurrent_retry`<br>`R · worldinterface-runtime · f007_restored_pending_attempt_requires_custody_fencing_and_uncertainty_stays_unresolved`<br>`R · worldinterface-runtime · f007_restored_unattempted_intent_requires_global_fencing_of_late_original_claims`<br>`R · worldinterface-runtime · wp_029_embedded_coding_and_transform_use_one_engine_and_application_services`<br>`R · worldinterface-runtime · wp_029_embedded_crash_recovery_quarantines_without_replacement`<br>`R · worldinterface-runtime · wp_029_restored_publication_is_readable_without_original_workspace_and_keeps_capacity`<br>`R · worldinterface-runtime · wp_030_catalog_quarantine_and_revocation_fence_queued_execution`<br>`R · worldinterface-runtime · wp_030_consumer_and_helper_imports_stay_inside_application_boundary`<br>`R · worldinterface-runtime · wp_030_embedded_forgery_scope_and_unauthorized_controls_fail_closed`<br>`R · worldinterface-runtime · wp_030_recovery_only_facade_enforces_disclosure_revocation_and_exclusive_ownership`<br>`R · worldinterface-runtime · wp_030_recovery_reconstructs_scoped_metadata_before_returning_application`<br>`R · worldinterface-runtime · wp_033_embedded_completion_is_producer_evidence_for_partial_milestone_only`<br>`C · wi-contract-check · campaign_coverage_requires_every_embedded_obligation_and_preserves_scope_tests`<br>`C · wi-contract-check · campaign_dirty_source_changing_or_stale_generation_is_rejected`<br>`C · wi-contract-check · campaign_missing_ignored_ambiguous_and_recursive_references_are_rejected`<br>`C · wi-contract-check · campaign_real_child_failure_zero_ignored_missing_and_altered_logs_fail_closed`<br>`C · wi-contract-check · wp_033_inflated_claims_cannot_convert_worker_success_to_consumer_acceptance`<br>`C · wi-contract-check · campaign_parallel_harnesses_retain_failures_and_reject_substituted_binary_pins` |
| WP-034 | `R · worldinterface-runtime · wp_034_embedded_seam_handoff_retains_nonsemantic_completion_and_unknown_controls`<br>`C · wi-contract-check · wp_034_handoff_rejects_substituted_pins_missing_evidence_and_semantic_completion`<br>`R · worldinterface-runtime · campaign_unknown_controls_publish_once_with_exact_observation_digest`<br>`C · wi-contract-check · wp_034_publication_rejects_registry_gate_mapping_and_claim_drift`<br>`C · wi-contract-check · wp_034_publication_rejects_stale_profile_fixture_lock_and_recipe_pins`<br>`C · wi-contract-check · wp_034_publication_requires_complete_gate_state_and_valid_descriptor`<br>`C · wi-contract-check · wp_034_publication_binds_external_evidence_and_never_grants_reviewed_closure` |
| AB-WI-001 | `R · worldinterface-runtime · ab_wi_001_store_formats_fresh_lineage_and_profile_rejection`<br>`R · worldinterface-runtime · ab_wi_001_published_identity_rejects_substitutions` |
| AB-WI-002 | `R · worldinterface-runtime · ab_wi_002_003_004_persist_retry_ack_and_discover`<br>`R · worldinterface-runtime · ab_wi_002_subprocess_crash_cuts`<br>`R · worldinterface-runtime · production_facade_authenticates_verifies_and_rechecks_live_target`<br>`R · worldinterface-runtime · expiry_during_refresh_blocks_initial_duplicate_and_later_batch_admission`<br>`R · worldinterface-runtime · live_ambiguity_after_intent_never_reaches_aq`<br>`R · worldinterface-runtime · production_facade_rejects_resource_boundaries_before_admission`<br>`R · worldinterface-runtime · caller_claims_cannot_override_authenticated_agency_or_grants`<br>`R · worldinterface-runtime · self_initiated_and_external_requests_follow_the_same_authorization_path`<br>`R · worldinterface-runtime · persisted_old_policy_request_is_retried_without_rewriting` |
| AB-WI-003 | `R · worldinterface-runtime · ab_wi_002_003_004_persist_retry_ack_and_discover`<br>`R · worldinterface-runtime · ab_wi_003_public_v2_vectors_and_digest_field_boundaries`<br>`R · worldinterface-runtime · ab_wi_003_required_and_detached_same_key_conflict` |
| AB-WI-004 | `R · worldinterface-runtime · ab_wi_002_003_004_persist_retry_ack_and_discover`<br>`R · worldinterface-runtime · run_discovery::tests::stale_cursor_restarts_and_partial_page_is_not_complete`<br>`R · worldinterface-runtime · wrong_queue_store_cannot_acknowledge_or_discover` |
| AB-WI-005 | `R · worldinterface-runtime · ab_wi_005_bounds_are_enforced_by_aq_admission_and_disposition`<br>`R · worldinterface-runtime · ab_wi_005_compound_commit_replay_is_all_or_none_with_stable_children`<br>`R · worldinterface-runtime · child_transaction_native_wal_process_death_cuts_replay_all_or_none`<br>`R · worldinterface-runtime · bounded_64_node_plan_commits_flat_children_and_rejects_65_before_admission`<br>`R · worldinterface-runtime · maximum_length_flow_identities_have_bounded_aq_and_effect_keys`<br>`R · worldinterface-runtime · wi_proposal_aq_commit_and_binding_process_death_recover_stable_children`<br>`R · worldinterface-runtime · sixty_four_boundary_nodes_reject_at_wi_plan_byte_bound_before_aq_admission`<br>`R · worldinterface-runtime · checkpoint_tail_and_wal_only_recovery_preserve_pending_flow_and_branch_receipts`<br>`R · worldinterface-runtime · flow_retained_count_rollback_replay_and_tampering`<br>`R · worldinterface-runtime · concurrent_flow_admissions_serialize_at_retained_capacity` |
| AB-WI-006 | `R · worldinterface-runtime · ab_wi_006_009_one_wait_and_strict_signal_cursor_across_restart` |
| AB-WI-007 | `R · worldinterface-runtime · ab_wi_007_commit_replay_identity_and_subscriber_ack`<br>`R · worldinterface-runtime · captured_fanout_routes_each_delivery_to_its_trusted_subscriber`<br>`R · worldinterface-runtime · process_death_after_wi_commit_recovers_aq_obligation`<br>`R · worldinterface-runtime · partial_audience_http_isolates_subscribers`<br>`R · worldinterface-runtime · partial_audience_stream_isolates_subscribers`<br>`R · worldinterface-runtime · process_death_after_subscriber_storage_redelivers_same_id` |
| AB-WI-008 | `R · worldinterface-runtime · ab_wi_008_early_late_duplicate_and_fanout_use_durable_records`<br>`R · worldinterface-runtime · continuation_process_death_replays_source_signal_wait_and_resume_commits`<br>`R · worldinterface-runtime · outbox_backpressure_retains_identity_and_pending_obligations`<br>`R · worldinterface-runtime · provider_callback_commits_an_authoritative_record_and_outbox_without_waiter`<br>`R · worldinterface-runtime · worker_intake_conflicts_scope_and_sequence_are_durable`<br>`R · worldinterface-runtime · ingress_ready_and_acknowledged_commit_source_outbox_atomically_and_wake_flows`<br>`R · worldinterface-runtime · reconciliation_commits_source_outbox_atomically_and_wakes_flow`<br>`R · worldinterface-runtime · flow_child_commits_source_outbox_atomically_and_wakes_flow`<br>`R · worldinterface-runtime · signal_capacity_alarm_uses_resident_statistics_across_restart`<br>`R · worldinterface-runtime · provider_callback_before_and_after_wait_resumes_flow_exactly_once`<br>`R · worldinterface-runtime · one_signal_resumes_two_live_flow_waiters_exactly_once_each`<br>`R · worldinterface-runtime · resequenced_worker_event_rejects_after_handled_across_rebuild_and_reopen`<br>`R · worldinterface-runtime · scope_rejected_signal_is_journaled_without_blocking_scheduling`<br>`R · worldinterface-runtime · stale_callback_revision_rejects_after_handled_across_rebuild_and_reopen` |
| AB-WI-009 | `R · worldinterface-runtime · ab_wi_006_009_one_wait_and_strict_signal_cursor_across_restart` |
| AB-WI-010 | `R · worldinterface-runtime · ab_wi_010_deadline_signal_orders_and_timeout_policies_survive_replay` |
| AB-WI-011 | `R · worldinterface-runtime · ab_wi_011_awaiting_releases_attempt_and_unrelated_work_progresses`<br>`R · worldinterface-runtime · administrative_resume_reestablishes_wait_and_all_wake_forms_redeliver_idempotently`<br>`R · worldinterface-runtime · altered_checkpoint_cannot_authorize_wait_progress`<br>`R · worldinterface-runtime · continuation_process_death_replays_source_signal_wait_and_resume_commits`<br>`R · worldinterface-runtime · current_authority_and_scoped_routes_are_required_at_every_wake`<br>`R · worldinterface-runtime · early_control_wakes_reestablish_child_and_parent_waits`<br>`R · worldinterface-runtime · eligibility_uses_signal_high_water_and_after_handled_advances_actual_sequence`<br>`R · worldinterface-runtime · fresh_generation_is_scoped_and_inherited_through_after_handled`<br>`R · worldinterface-runtime · resume_redelivery_reproduces_result_and_conflicting_context_rejects`<br>`R · worldinterface-runtime · signal_deadline_cancel_and_late_event_orders_preserve_committed_outcomes`<br>`R · worldinterface-runtime · non_early_wait_rejects_preexisting_outbox_events_after_backpressure_and_rebuild`<br>`R · worldinterface-runtime · admitted_unacknowledged_wake_with_backlog_recovers_independently_of_scan`<br>`R · worldinterface-runtime · accepted_child_and_coordinator_replay_after_authority_revocation`<br>`R · worldinterface-runtime · signal_cancel_acceptance_and_deadline_winners_replay_across_paired_reopen`<br>`R · worldinterface-runtime · continuation_inspection_authorizes_and_reports_pending_and_accepted_chain`<br>`R · worldinterface-runtime · accept_resume_validates_exports_even_with_other_artifacts`<br>`R · worldinterface-runtime · unaccepted_coordinator_finish_requires_authority_after_child_acceptance`<br>`R · worldinterface-runtime · idle_ticks_skip_unchanged_waiting_flows_and_reconcile_changes_after_restart`<br>`R · worldinterface-runtime · journal_only_admission_failure_retries_on_an_otherwise_idle_live_engine` |
| AB-WI-012 | `R · worldinterface-runtime · ab_wi_012_disposition_algebra_and_minimal_invalid_failure` |
| AB-WI-013 | `R · worldinterface-runtime · ab_wi_013_required_detached_closure_cancel_and_mixed_runs`<br>`R · worldinterface-runtime · aq_success_without_wi_acceptance_cannot_complete_flow`<br>`R · worldinterface-runtime · flow_cancellation_after_start_preserves_external_uncertainty`<br>`R · worldinterface-runtime · cancellation_before_start_and_failed_node_redelivery_cannot_invoke`<br>`R · worldinterface-runtime · publication_failure_retries_durable_success_without_reinvocation`<br>`R · worldinterface-runtime · rejected_aq_cancel_does_not_mutate_flow_and_authorized_execution_continues`<br>`R · worldinterface-runtime · temporary_binding_artifact_io_failure_retries_without_quarantine`<br>`R · worldinterface-runtime · quarantine_control_failure_surfaces_and_retries_after_host_repair` |
| AB-WI-014 | `R · worldinterface-runtime · ab_wi_014_capacity_remote_fence_stale_and_uncertain` |
| AB-WI-015 | `R · worldinterface-runtime · ab_wi_015_bounds_and_unsupported_overrides_reject_without_truncation`<br>`R · worldinterface-runtime · ab_wi_015_016_real_child_execution_replay_and_foreign_provenance` |
| AB-WI-016 | `R · worldinterface-runtime · ab_wi_016_authorization_and_integrity`<br>`R · worldinterface-runtime · ab_wi_015_016_real_child_execution_replay_and_foreign_provenance`<br>`R · worldinterface-runtime · backup_retains_both_store_locks_and_rejects_missing_references`<br>`R · worldinterface-runtime · ab_wi_016_nested_destinations_leave_source_unchanged`<br>`R · worldinterface-runtime · ab_wi_016_single_component_relative_destinations` |
| AB-WI-019 | `R · worldinterface-runtime · ab_wi_019_two_independent_stores_restore_complete_wal_artifacts_and_control_history`<br>`R · worldinterface-runtime · ab_wi_019_invalid_restore_never_publishes_partial_destination` |
| AB-WI-020 | `R · worldinterface-runtime · ab_wi_020_terminal_cancel_is_append_free_and_has_no_late_cascade`<br>`R · worldinterface-runtime · ab_wi_020_023_provider_ack_loss_is_uncertain_until_reconciled`<br>`R · worldinterface-runtime · ab_wi_020_queue_cancel_keeps_started_external_effect_uncertain`<br>`R · worldinterface-runtime · spi_cancel_phase_records_observation_and_preserves_commit_truth` |
| AB-WI-021 | `R · worldinterface-runtime · ab_wi_021_awaiting_release_hold_and_resume_keep_durable_worker_capacity` |
| AB-WI-022 | `R · worldinterface-runtime · ab_wi_022_retirement_keeps_resident_capacity_and_alarm`<br>`R · worldinterface-runtime · wi03p_f001_expired_pending_delivery_stops_recovery`<br>`R · worldinterface-runtime · wi03p_f001_admitted_delivery_completes_after_expiry`<br>`R · worldinterface-runtime · expiration_during_ack_wait_completes_without_recovery_generation`<br>`R · worldinterface-runtime · recovery_generation_budget_bounds_mismatch_binding_churn_and_capacity_after_restart`<br>`R · worldinterface-runtime · owner_churn_reuses_wake_capacity_and_other_owner_can_deliver_and_ack_after_restart`<br>`R · worldinterface-runtime · terminal_wait_capacity_recovers_from_journaled_generation`<br>`R · worldinterface-runtime · previously_journaled_generation_wake_is_reused_without_migration_after_restart`<br>`R · worldinterface-runtime · retired_wake_deadletters_without_replacement_after_restart` |
| AB-WI-023 | `R · worldinterface-runtime · ab_wi_023_actual_uncertain_aq_mutation_recovers_stable_identity`<br>`R · worldinterface-runtime · ab_wi_020_023_provider_ack_loss_is_uncertain_until_reconciled`<br>`R · worldinterface-runtime · spi_provider_and_reconciliation_crash_cuts_never_duplicate_commit` |
