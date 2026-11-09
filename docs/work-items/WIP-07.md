# WIP-07: Flow coordinator on AQ transactional children

New Direct and Flow production submissions use PlanService, the sealed ExecutionPlan,
and one AQ coordinator/node handler. EffectService::submit_direct delegates to that
path. Its former standalone submit entry is removed; persisted top-level admissions
retain their original request bytes and the recovery/authorization/completion APIs.
The existing recovery tests explicitly seed pre-WIP-07 effect records through a
local fixture. No production fixture entry is added.

The current `local-flow-7` fresh-store profile initializes an AQ-free Flow journal in
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
- AQ 0.3.0 automatic snapshots use the default 10,000-event threshold and normal
  engine bootstrap. Read-only AQ preflight remains before WI recovery writes;
  engine bootstrap remains separate. Ordinary startup validates the checkpoint
  and WAL boundary frames, then replays only the WAL tail. It trusts the
  checkpoint's commitment to the covered WAL prefix without rereading or rehashing
  that prefix. Corruption or modification wholly inside the skipped prefix can
  therefore go undetected if the checked boundary frames remain unchanged.
  The complete WAL is retained and Strict tail handling is unchanged. Integrity
  audits require the explicit full-history verification procedure below.
  Physical checkpoint damage falls back to WAL recovery; incompatible checkpoints
  and corrupt tails still follow AQ's fail-closed compatibility/integrity policy.
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
The runtime test profile uses optimization level 3 and 16 codegen units, without LTO
for the sustained 10,000-admission and recovery fixture. Debug assertions,
overflow checks and debug-only crash
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

Structural review adds 57 inventory declarations/modules to the existing retained
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
| F-009 | Fixed: boundary child timeouts round their materialized millisecond budget up to AQ seconds, minimum one; pure nodes retain 60 seconds; new coordinators have no invented boundary timeout (AQ leases, retries and cancellation still apply). `boundary_attempt_timeout_tracks_budget_in_seconds`. |
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
| F-020 | Fixed: `flow_runs` holds one projection per run; indexed request and task identities preserve idempotency and cancellation. The definition catalog and tenant revision remain separate. Mutation and replay load only the affected run; ticks load unfinished runs. Complete append-only history remains authoritative and recovery compares projections one row at a time. Immutable plans compile at admission, before execution and during recovery; later journal commands verify the admission digest and cannot modify plan bytes. Row encoding enforces its size bound once inside the writer transaction. `flow_capacity_is_enforced_without_losing_existing_history` uses production `EffectService::submit_direct` for all 10,000 admissions in one bundle, mixing host cancellations with six real boundary completions (including requests 256, 257 and 10,000). It reopens after 257 and with request 10,000 pending, verifies receipts, rejects request 10,001 without a revision, preserves dedup/conflict behavior at capacity, rebuilds WI projections while the queue is quiescent, then verifies production resubmission and historical identities against the retained live AQ projection. The full 10,000-run paired reopen occurs with the final request pending. This tests sustained admission and late execution/recovery; it is not a throughput benchmark or 10,000 successful external commits. |
| F-021 | Fixed: `flow-dispatch.lock` fences Start, cancellation, node mutations, quarantine and Finish across separate StoreRoot/StoreBundle handles and processes. The root lock is correctly described as a shared maintenance exclusion lock. Foundation `separate_store_handles_share_the_dispatch_fence` proves a second handle blocks until release. Runtime `cancellation_before_start_and_failed_node_redelivery_cannot_invoke` now wires its handler to a separately opened bundle and exercises both parent and single-child cancellation. |
| F-022 | Fixed: `FlowRejected`, domain `CompileError` and storage `FlowIntegrity` identify deterministic validation failures. `FlowRevisionConflict` and `FlowCapacity` are distinct; I/O, SQLite and AQ service errors remain retryable irrespective of message text. Quarantine cancellation errors surface to the host and remain pending for the next tick. `pre_start_journal_failure_is_retryable_and_then_succeeds` injects a temporary SQLite error containing the former terminal keywords; `temporary_binding_artifact_io_failure_retries_without_quarantine` removes and restores accepted-request bytes; `quarantine_control_failure_surfaces_and_retries_after_host_repair` restores AQ control authority after a visible failed cancel. |
| F-023 | Fixed: the independent outcome fold and terminal-parent fallback honor persisted cancel_requested; single-child cancellation ends Canceled unless stronger boundary evidence requires uncertainty. The separate-bundle cancellation regression asserts the exact outcome and zero invocations. |

The historical `local-flow-6` profile added the row projections, bundle dispatch lock,
and compact storage of exact UTF-8 plan/request bytes as JSON strings.
This avoids large intermediate JSON integer arrays on routine reads and replay.
It preserves the original bytes and hashes. The current `local-flow-7` profile adds
a retained-run count to the tenant catalog. The admission reducer increments it
only for a new successful admission, in the same immediate transaction as history
and run projection writes. Retry, conflict, completion and cancellation do not
consume another slot. Replay reconstructs and compares the count with the persisted
catalog. `local-flow-6` and earlier profiles reject before recovery writes, with no
implicit migration.

Repeated plan restoration reuses at most 16 sealed compiler results with at most
8 MiB of encoded plan/pin inputs. A hit compares the exact bytes, expected hash
and complete FlowPins; any difference takes the full compiler path. Admission
digests and per-run identity checks still run on every restore, including cache
hits. The cache holds no execution, authorization or branch state, and boundary
authorization remains fresh. The tamper regression populates the cache, then
recomputes admission digests while changing bytes, hash, registry/schema/authority
pins and attribution, so these failures reach the relevant validation checks.
Canonical encoding writes object keys and scalar values directly into its output
buffer, avoiding a temporary allocation for each byte in an admission digest's
request array. Sorting, scalar validation, escaping, size limits and hash bytes
are unchanged; the independent canonical golden vectors cover this path. The
capacity fixture reports cumulative submission, cancellation and reconciliation
times, each 1,000-run batch, and the paired reopen/rebuild phases. Optional
`WI_RECOVERY_TIMINGS=1` diagnostics separate AQ read-only preflight (including
checkpoint sequence and AQ recovery phases), WI database replay/comparison,
plan/artifact validation, cross-store references/convergence and AQ engine
bootstrap/scheduling. AQ 0.3.0 shares projection histories and recovers through
checkpoints. WI no longer scans retained runs to count each admission. The full
10,000-run proof and every existing assertion remain non-ignored.
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

For an integrity audit (F-026), stop all bundle writers and close the bundle, then
run AQ 0.3.0's `actionqueue store verify --data-dir /path/to/bundle/aq --json`.
This explicitly replays the complete AQ WAL and compares it with the checkpoint
and tail. Ordinary startup or `actionqueue store inspect` does not provide this
full-history verification. WI's offline `StoreBundle::backup(source, destination)`
and `StoreBundle::restore(backup, destination)` also invoke AQ full-history
verification and validate the paired WI/AQ bundle before publishing a separate
destination; use these bundle APIs for paired backups and restores. WI journal
replay, plan/artifact validation and cross-store checks still run on every open.

Production bootstrap keeps AQ 0.3.0's Strict WAL policy. For an incomplete trailing
record, stop every writer, retain a byte-for-byte copy, inspect the native WAL with
AQ's published recovery API and TruncatePartial policy, and obtain the validated
last-record boundary. An operator may then use AQ's `truncate_to_last_valid` API
on that tail, sync it, and re-open with Strict validation. Never truncate a complete
record or interior corruption. This maintenance procedure is explicit; tests do
not claim automatic repair by QueueEngine.

Remediation adds only retained ownership/guard and test-helper declarations to the
existing fabric-support disposition family. That historical remediation did not
change AQ versions or dependencies. The subsequent operator-authorized AQ 0.3.0
migration updates the runtime graph binding; legacy pins and legacy-removal
dispositions remain unchanged. The remote actor
mutation-only fixture retains its original test writer because this adopted binary
profile does not enable AQ's actor wire feature; AB-WI-005 and AB-WI-010 use native
WAL encoding and replay.

This remediation explicitly dispositions five new error/test declarations in the
existing retained fabric-support family. Inventory source hashes are refreshed
without changing adopted checks, AQ versions, dependency features or removal gates.

## AQ 0.3.0 performance repair (F-024/F-025)

Operator cycle `47b9f34c-4ffd-4ef6-8849-55aae36a8999` explicitly supersedes
the older runtime 0.2.0 requirement. Four direct exact dependencies and the
runtime's own lockfile now bind crates.io 0.3.0; legacy root pins are untouched.
`contracts/aq-integration-target.json::runtime_release` records the six resolved
packages and registry checksums separately from the unchanged imported `target`,
`published_release`, baseline lock and source-record digests. The graph check and
runtime receipt validator reject stale 0.2.0 packages, substituted sources, wrong
checksums and wrong features. No new source commit is attributed to the old baseline.

The fresh WI profile advances to `local-flow-7` because the catalog encoding
changes. This is the explicit format disposition; no existing store is upgraded.
No new inventory type declarations are introduced by the count/timing helpers.
Borrowed history iteration replaces AQ's old slice getters without copying history.
AQ manifests now require snapshot schema 10 and projection version 9.

New non-ignored tests cover count rollback, retry/conflict, cancellation/completion,
rebuild and tampering; a synthetic final-slot fixture isolates concurrent transaction
serialization while the production test retains all 10,000 real admissions.
Recovery coverage compares checkpoint-plus-tail and complete WAL projections,
retains pending child bindings, verifies branch decisions and boundary receipts,
exercises damaged-checkpoint fallback, and rejects partial WAL tails before WI writes.
The completed Flow backup/restore case now includes a checkpoint.

The upstream release-note URL was unavailable during this run (web fetch failed;
direct network access was blocked). API/recovery behavior was reconciled against
the fetched 0.3.0 crate source and its packaged release/compatibility documentation.
Fresh measured results are recorded in the run's scratch verification report.
F-024 requires a complete adopted runtime-test pass; F-025 requires measured
scaling and reopen evidence. F-001–F-023 protections remain required. G4's effective
closure remains conditional on CraftingTable's qualifying review and integration.

Supplemental measurement before committed-head adopted checks (Rust 1.89.0):

| Phase | Duration |
|---|---:|
| Initial all-target optimized test compilation (LTO disabled) | 271.64 s |
| Complete 10,000-run capacity case, 600 s timeout | 285.92 s |
| First / last 1,000 submission totals | 8.183 / 7.998 s |
| First / last 1,000 cancellation totals | 5.599 / 5.433 s |
| Full-capacity AQ read-only preflight, checkpoint sequence 20,095 | 0.145 s |
| WI database replay and projection comparison | 16.764 s |
| WI plan verification and artifact closure | 7.071 s |
| WI open total | 23.836 s |
| Cross-store references and convergence | 2.675 s |
| AQ engine bootstrap and scheduling reconstruction | 0.171 s |
| WI full-history rebuild | 16.526 s |

This includes both paired reopens and the final pending request, receipts,
capacity rejection, deduplication/conflict and rebuild assertions. Some supplemental
compilation overlapped admission batches; the per-batch data show no monotonic
retained-history growth. These measurements support F-025 remediation and do not
substitute for the complete adopted runtime-test required by F-024.
