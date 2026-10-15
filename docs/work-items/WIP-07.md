# WIP-07: Flow coordinator on AQ transactional children

New Direct and Flow production submissions use PlanService, the sealed ExecutionPlan,
and one AQ coordinator/node handler. EffectService::submit_direct delegates to that
path. Its former standalone submit entry is removed; persisted top-level admissions
retain their original request bytes and the recovery/authorization/completion APIs.
The existing recovery tests explicitly seed pre-WIP-07 effect records through a
local fixture. No production fixture entry is added.

The new `local-flow-4` fresh-store profile initializes an AQ-free Flow journal in
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
and its exact readiness-signal obligation commit together. Missing bindings yield
to a correlated retained-signal wait without a connector call or failed attempt.
Pending coordinator requests and wakes recover from durable WI records.

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
  multi-node boundary uses a deterministic client key derived from that request and
  node ID. Fresh authorization must cover the complete materialized demand.
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
Coverage includes real Direct/Flow boundary receipts; branch/skip/join replay;
readiness before reconciliation and restart; authority/registry/schema rejection;
native process death before commit, during partial WAL publication, after durable
commit before acknowledgment and before WI reconciliation; AQ bounds, deadline
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
