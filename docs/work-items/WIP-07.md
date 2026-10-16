# WIP-07: Flow coordinator on AQ transactional children

New Direct and Flow production submissions use PlanService, the sealed ExecutionPlan,
and one AQ coordinator/node handler. EffectService::submit_direct delegates to that
path. Its former standalone submit entry is removed; persisted top-level admissions
retain their original request bytes and the recovery/authorization/completion APIs.
The existing recovery tests explicitly seed pre-WIP-07 effect records through a
local fixture. No production fixture entry is added.

The new `local-flow-5` fresh-store profile initializes an AQ-free Flow journal in
StoreKind::Flow. Unsupported profiles reject before modification; no migration is
implied. Immutable definitions, predecessor and activation checks, pinned plan bytes,
hash, registry/schema and authority snapshots, complete original submissions and
accountable agency, scoped request deduplication, proposals, child bindings, node
results and exports have append-only history and rebuildable projections. An
independent admission digest protects attribution and queue identity. Recovery
recompiles against the persisted expected hash and snapshots. Backup/restore checks
both WI publication closure and AQ admission/disposition ancestry.

Every explicit node is one flat, required, Once AQ child. One Awaiting disposition
commits children, dependency edges, checkpoint and parent AllTerminal wait. WI's
prepared proposal is never commitment evidence. Host reconciliation surrounds each
QueueEngine tick; it verifies accepted children using AQ's public projection,
producing disposition, scoped-child-key derivation and inherited causation. Binding
is persisted before dispatch. Missing bindings yield to an immediately eligible AQ
deadline wait, then reload the durable binding without a connector call or failed
attempt. This allocates no readiness signal identities. Pending coordinator requests
and bindings recover from durable WI records; the old per-child wake outbox is no
longer used.

Boundary nodes materialize pinned inputs/resources and preserve submitting agency.
Structural ceilings never supply authority. Current evidence, target comparison,
registry/implementation checks, authorization-use reservation, StartProduction and
the existing effect completion/receipt machinery remain mandatory. Opaque artifacts
require an installed connector's explicit pinned-schema validator; its default
rejects unsupported contracts. Pure nodes evaluate with the sealed evaluator and
journal hashes, program pins and selected branches, producing no boundary receipts.
AQ closure only triggers an independent WI outcome/output/receipt evaluation.
Uncertainty after Start survives queue cancellation.

Code-forced choices within the accepted design:

- AQ's derived child key uses `child/v1/<hash>`, which WI's OpaqueRef grammar cannot
  represent. Versioned child provenance stores this exact validated AQ key as text,
  alongside the existing WI effect admission key. The accepted request artifact and
  canonical digest are preserved; no top-level acknowledgment is fabricated.
- The queue config uses a bounded 1 MiB disposition budget, within AQ's published
  hard ceiling. Its default 128 KiB budget cannot hold the complete accepted
  64-child transaction with its duplicated admission plans. Payload, initial-run,
  dependency, child, wait-target, signal and hierarchy hard limits remain AQ-owned.
- Native WAL fault injection lives in `tests/flow_crash.rs`; AQ contract fixtures
  extend `tests/executor_aq_contract.rs` to reuse its published mutation seams.
- Flow submission uses the original submission as the agency/policy template. A
  multi-node boundary uses a bounded, domain-separated hash of the original client request ID and
  node ID for its client key. AQ local child keys use the persisted task UUID. Fresh authorization must cover the complete materialized demand.
- Production Wait/Timer lowering rejects before coordinator admission. WIP-08 owns
  those nodes; AB-WI-010 is proven separately through actual AQ continuation APIs.

The pure `flow/coverage.json` is preserved. `flow/integration-coverage.json` separately
binds all runtime obligations and AB-WI-005/010/013 to exact non-ignored tests;
contract-check rejects absent, ignored, duplicate, foreign and incomplete references.
WI-AQ-G4 is drafted closed with the existing WIP-04 completion and WIP-05 cases.
Its closure becomes effective only through the reviewed merge and adopted checks.
WI-AQ-G3 stays open for WIP-08. No upstream conformance, legacy removal or release
claim is made.

Verification uses Rust 1.89.0, the supplied external target cache and scratch logs.
The runtime test profile uses optimization level 1 for the sustained 10,000-admission
and recovery fixture. Debug assertions, overflow checks and debug-only crash
injection remain enabled; no cases, bounds or adopted check definitions change.
Coverage includes real Direct/Flow boundary receipts; branch/skip/join replay;
readiness before reconciliation and restart; authority/registry/schema rejection;
native AQ process death before commit, during partial WAL publication and after
durable commit before acknowledgment; real WI coordinator process death after
PrepareChildren, after AQ commit before binding, and after binding; AQ bounds, deadline
orders and all timeout policies; required/detached lifecycle and mixed-run task
success; projection rebuild/tampering, immutable versions and artifact backup/restore.
All adopted checks must run successfully on the committed candidate. Results are
in the run's `scratch/verification`, outside Git.

Repository policy settings version 1, observed 2026-10-15T18:53:08.036Z, reserves
integration to CraftingTable after qualifying review. This branch is committed for
review and never merged or pushed by the implementation agent. Independent review
and its blocking/major-finding disposition remain controller responsibilities.

Structural review adds 44 inventory declarations/modules to the existing retained
infrastructure family; no inventory item or existing treatment is removed. Seven
new source/test surfaces and the crash fixture's selective filesystem-write
surface are dispositioned explicitly. Source hashes are then refreshed with the
contract tool. No adopted check definition or dependency pin changes.

The full 64-node execution fixture exposed concurrent content-publication races.
Publication now reloads each transition under its SQLite writer transaction and
accepts identical progress already committed by another writer. It preserves the
strict append-only replay algebra and publishes no duplicate transitions. An
8-writer regression and real 64-child Flow execution cover the repair. The
Flow-specific process-death-after-Start fixture also reopens the paired stores and
proves the original child binding and external commit survive without reinvocation.

Final boundary review made host artifact authorization mandatory: PlanConfiguration
implements the existing ArtifactAuthorization contract, and FlowAttemptHandler
requires a host authorizer. Submission, materialized inputs/resources, boundary
outputs and inspection use the authenticated submitting principal with the existing
ArtifactResolver. Journal/admission evidence remains host-internal validation.
A denied same-tenant artifact prevents admission or connector invocation; persisted
attribution and artifact scope alone do not supply read authority.

The maximum-length identity regression executes two boundary nodes with 256-byte
node, definition and original client request IDs. Derived effect keys hash a
canonical tuple, avoiding concatenation collisions and overflow; AQ local keys
use already-persisted UUIDs. Stable identities never depend on truncating user IDs.


## Review remediation (F-001 through F-019)

These are implementation dispositions for independent review, not reviewer closure.
All new tests below are non-ignored in `crates/worldinterface-runtime/tests/flow.rs`
unless another file is named. The coverage matrix preserves the authoritative AB
case IDs and exact manifest/package/test references.

| Finding | Disposition and evidence to verify |
| --- | --- |
| F-001 | Fixed: Start and Finish share a root-bound dispatch guard. AQ authorizes cancellation before any WI mutation; its control and WI cancel record share the Start guard. Start reloads Flow closure and node disposition, and invocation checks again. Finish waits for child terminality. Committed effects missing a successful node escalate to Uncertain. Node writes after Finish reject. `cancellation_before_start_and_failed_node_redelivery_cannot_invoke` and the existing after-Start cancellation test cover both sides; `rejected_aq_cancel_does_not_mutate_flow_and_authorized_execution_continues` verifies rejected control leaves WI unchanged. |
| F-002 | Fixed: effect ownership is part of the initial intent journal entry, preceding resolution/authorization. Legacy advance, prepare and delivery reject owned effects; dispatch_pending skips them. `flow_owned_authorized_effect_cannot_enter_legacy_dispatch`. |
| F-003 | Fixed: transient pre-Start and publication failures yield retryable AQ dispositions. Durable effect outcomes are recovered before current authorization is requested again. Publication retry never invokes a committed boundary twice. `pre_start_journal_failure_is_retryable_and_then_succeeds` and `publication_failure_retries_durable_success_without_reinvocation`. |
| F-004 | Fixed: effect dedup uses the typed Flow owner (run and node), a namespace distinct from client request dedup. Ownership is verified before use and preparation requires matching child provenance. `node_request_collision_uses_distinct_owned_effects_and_queue_keeps_progressing`. |
| F-005 | Fixed: readiness uses deadline waits and durable binding reloads, allocating zero signals. `full_signal_capacity_does_not_block_binding_or_flow_execution` fills AQ's configured identity budget, asserts backpressure on the next signal, then completes a Flow. |
| F-006 | Fixed: oversized Artifact outputs reject explicitly; JSON ports validate actual JSON bytes individually. The current descriptor profile rejects mixed Artifact/JSON boundary ports at compilation because it permits exactly one output. `artifact_output_size_and_json_input_schema_are_enforced_on_actual_bytes`. |
| F-007 | Fixed: `wi_proposal_aq_commit_and_binding_process_death_recover_stable_children` kills the real WI coordinator after PrepareChildren, after AQ commit before BindChild, and after BindChild, then uses production bootstrap and checks stable identities/proposal, bindings, zero readiness signals and exactly one invocation/commit per boundary. The obsolete before-WI-reconciliation mode was removed from the AQ-only fixture. There is no WakeDelivered cut in the new design because readiness no longer has a signal outbox. |
| F-008 | Fixed: the hash test recomputes admission_hash after tampering and asserts the precise plan digest mismatch, including the durable quarantine diagnostic and zero commits. |
| F-009 | Fixed: boundary child timeouts round their materialized millisecond budget up to AQ seconds, minimum one; coordinator/pure nodes retain 60 seconds. `boundary_attempt_timeout_tracks_budget_in_seconds`. |
| F-010 | Fixed: reconciliation validates each unfinished run independently and journals a quarantine diagnostic bounded to 1024 UTF-8 bytes before canceling that run's AQ task. Typed I/O, AQ, capacity and conflicting writes retry; completed runs are skipped, including completion races. Store-level snapshot/journal failures still surface. `damaged_run_is_quarantined_while_another_tenant_completes`. |
| F-011 | Superseded by F-020: the original 256-run envelope regressed Direct capacity. Runs now occupy individual rows with a 10,000-run tenant budget; definition versions retain their independent 256-version budget. Routine admission, mutation and scheduling use indexed subsets. See the production capacity and recovery evidence below. |
| F-012 | Fixed: artifact existence, integrity and tenant scope are checked before Flow history writes for admissions, bindings, results and exports. `flow_journal_rejects_invalid_artifacts_and_preserves_finished_admission` and `journal_finish_rejects_substituted_exports_and_stale_definition_admission`. |
| F-013 | Fixed: AdmitRun idempotency compares verified immutable admission identity after progress; activation retries retain the exact previous precondition and reject zero; admission checks activity inside the transaction; finished node/binding/wake mutations reject; exports equal recorded selected producer artifacts; child run IDs are unique. The two journal regression tests above and foundation `flow_versions_predecessors_revision_rollback_and_rebuild` cover these rules. |
| F-014 | Fixed: production EffectService::submit_direct and PlanService::submit_flow have forged-session, invalid/revoked-evidence, caller identity/capacity and conflict regressions. Grant/approval claims are admitted as requests, then denied by authorization with zero invocations. `production_direct_and_flow_reject_forged_sessions_invalid_evidence_and_claims`, `forged_grant_and_approval_claims_do_not_authorize_production_direct_or_flow`. |
| F-015 | Fixed: resource, operation and connector enlargement assert CompileError::Authority; a Transform requesting hidden I/O asserts HiddenIo; all produce zero commits. `runtime_compiler_rejects_connector_and_operation_authority_enlargement` and the strengthened existing authority/purity test. |
| F-016 | Fixed: native torn-tail test asserts production AQ bootstrap fails specifically during WAL replay under Strict. TruncatePartial inspection proves the valid prefix only; it is not automatic production repair. See operator procedure below. |
| F-017 | Fixed with the existing WI size envelope preserved: AQ limit+1 assertions name TooLarge, HierarchyDepth, DispositionError or WaitSpecError; parent mappings have cardinality 64 and exactly one Awaiting disposition contains all children. The 64-boundary-node WI case asserts its stricter 64 KiB plan rejection before AQ admission. AQ compound and deadline fixtures replay native framed WAL; deadline races assert the exact winning signal identity. |
| F-018 | Fixed: Running/outcome-less boundary errors record NodeDisposition::Uncertain; AQ cancels dependent work and Flow outcome folding preserves the uncertainty. JSON schemas parse connector input/output bytes and exported artifact bytes; reference metadata never substitutes for JSON. `outcome_recording_error_marks_node_uncertain_and_blocks_dependents` and the artifact/JSON and `exported_json_schema_validates_artifact_bytes_instead_of_reference_metadata` regressions. |
| F-019 | Fixed: Direct admission destructures the boundary once; branch replay includes a guarded boundary whose skipped result cannot create an effect or receipt; the compound-test comment describes AQ only; reference validation propagates digest errors. |

## Review remediation (F-020 through F-023)

These are implementation dispositions for independent review, not finding closure.

| Finding | Disposition and evidence to verify |
| --- | --- |
| F-020 | Fixed: `flow_runs` holds one projection per run; indexed request and task identities preserve idempotency and cancellation. The definition catalog and tenant revision remain separate. Mutation and replay load only the affected run; ticks load unfinished runs. Complete append-only history remains authoritative and recovery compares projections one row at a time. Immutable plans compile at admission, before execution and during recovery; later journal commands verify the admission digest and cannot modify plan bytes. Row encoding enforces its size bound once inside the writer transaction. `flow_capacity_is_enforced_without_losing_existing_history` uses production `EffectService::submit_direct` for all 10,000 admissions in one bundle, mixing host cancellations with six real boundary completions (including requests 256, 257 and 10,000). It reopens after 257 and with request 10,000 pending, verifies receipts, rejects request 10,001 without a revision, preserves dedup/conflict behavior at capacity, rebuilds projections and reopens the paired stores with historical identities intact. This tests sustained admission and late execution/recovery; it is not a throughput benchmark or 10,000 successful external commits. |
| F-021 | Fixed: `flow-dispatch.lock` fences Start, cancellation, node mutations, quarantine and Finish across separate StoreRoot/StoreBundle handles and processes. The root lock is correctly described as a shared maintenance exclusion lock. Foundation `separate_store_handles_share_the_dispatch_fence` proves a second handle blocks until release. Runtime `cancellation_before_start_and_failed_node_redelivery_cannot_invoke` now wires its handler to a separately opened bundle and exercises both parent and single-child cancellation. |
| F-022 | Fixed: `FlowRejected`, domain `CompileError` and storage `FlowIntegrity` identify deterministic validation failures. `FlowRevisionConflict` and `FlowCapacity` are distinct; I/O, SQLite and AQ service errors remain retryable irrespective of message text. Quarantine cancellation errors surface to the host and remain pending for the next tick. `pre_start_journal_failure_is_retryable_and_then_succeeds` injects a temporary SQLite error containing the former terminal keywords; `temporary_binding_artifact_io_failure_retries_without_quarantine` removes and restores accepted-request bytes; `quarantine_control_failure_surfaces_and_retries_after_host_repair` restores AQ control authority after a visible failed cancel. |
| F-023 | Fixed: the independent outcome fold and terminal-parent fallback honor persisted cancel_requested; single-child cancellation ends Canceled unless stronger boundary evidence requires uncertainty. The separate-bundle cancellation regression asserts the exact outcome and zero invocations. |

The fresh `local-flow-5` profile adds the row projections and bundle dispatch lock.
Earlier development profiles reject before modification; no migration is supplied.
The per-tenant 10,000 retained-run limit matches the existing effect capacity;
definition versions have a separate 256-version limit. Each run and catalog
encoding is bounded separately by 16 MiB. Full host inspection can enumerate retained runs,
but production scheduling and mutations do not serialize that whole collection.
No online pruning or identity reuse is introduced. At the existing 10,000-run
limit, archive the stopped bundle before initializing a separate bundle.

Availability failures do not create quarantine. Restore the unavailable artifact,
store or AQ service and continue ticks; accepted identities and outcomes are reused.
Integrity quarantine retains its bounded diagnostic and immutable evidence and
prevents new boundary dispatch. A failed AQ cancel is surfaced and retried on the
next tick after restoring host control authority. Quarantine is not an editable
projection flag: for damaged authoritative evidence, stop writers, preserve the
damaged bundle, and restore a known-good paired WI/AQ backup through StoreBundle's
offline restore API. Strict reopen verifies it before recovery. Preserve and
reconcile any post-backup external outcomes with the WIP-04 procedure before new
submissions; clearing quarantine or reusing a canceled coordinator is unsupported.

Production bootstrap keeps AQ 0.2.0's Strict WAL policy. For an incomplete trailing
record, stop every writer, retain a byte-for-byte copy, inspect the native WAL with
AQ's published recovery API and TruncatePartial policy, and obtain the validated
last-record boundary. An operator may then use AQ's `truncate_to_last_valid` API
on that tail, sync it, and re-open with Strict validation. Never truncate a complete
record or interior corruption. This maintenance procedure is explicit; tests do
not claim automatic repair by QueueEngine.

Remediation adds only retained ownership/guard and test-helper declarations to the
existing fabric-support disposition family; no check definitions, AQ features,
versions, dependencies or legacy-removal dispositions change. The remote actor
mutation-only fixture retains its original test writer because this adopted binary
profile does not enable AQ's actor wire feature; AB-WI-005 and AB-WI-010 use native
WAL encoding and replay.

This remediation explicitly dispositions five new error/test declarations in the
existing retained fabric-support family. Inventory source hashes are refreshed
without changing adopted checks, AQ versions, dependency features or removal gates.
