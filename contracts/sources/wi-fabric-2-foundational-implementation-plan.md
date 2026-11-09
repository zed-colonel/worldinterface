# WI-FABRIC-2 Foundational Implementation Plan

**Status:** Repository-level development plan with an additive developmental-experiment evidence profile; detailed now, dependency-gated at ActionQueue integration boundaries  
**Target repository:** `zed-colonel/worldinterface`  
**Source baseline:** public `main` at commit `f48df82e98203fa94c86eecae60425205c3e00e9`, June 9, 2026  
**Normative architecture:** `WorldInterface v2: Accountable Boundary Fabric, Portable Agency, and Deterministic Flow`  
**Contract label:** `WI-FABRIC-2`  
**Stack contract:** `STACK-2026-07-25-FOUNDATIONAL-2`  
**Upstream implementation contract:** `AQ-CONT-1`  
**Downstream consumer:** `EXO-V3`  
**Planning revision:** `WI-PLAN-5`  
**Date:** September 12, 2026  
**Implementation posture:** Clean break; one target authority path; no legacy readers, adapters, dual writes, or live shadow runtime

This document turns the finalized WorldInterface architecture into an executable repository plan. It specifies development order, crate boundaries, persistence transactions, ActionQueue gates, connector disposition, conformance ownership, crash testing, security work, developmental-experiment evidence, and release criteria. It is intentionally more concrete than the architecture document while remaining subordinate to that document, the canonical stack contract, and `STACK-DEVELOPMENTAL-DIAGNOSTICS-1`. The profile improves evidence for Exoskeleton-owned developmental campaigns; it does not add a WorldInterface evaluator or performance ontology.

The target is not a compatibility migration from a deployed system. The current repository is a frozen experimental baseline. Its strongest properties, failure cases, fixtures, and benchmarks are retained as evidence. Its obsolete ontology and serialized formats are not carried into the active runtime.

> **Initial worker-provider amendment:** [WI-WORKER-EXECUTION-1](wi-fabric-2-worker-execution-profile.md) owns `process-managed-proof-1`. It stages bounded embedded delegation proof before broader platform/worker-isolation work. All original parent exit gates and AQ gates remain; the narrower milestone cannot mark them complete. Kata/containerd belongs to the EXO instance deployment, not WI core.

---

## 0. Purpose and decision summary

### 0.1 Purpose

`WI-FABRIC-2` must provide one coherent accountable boundary fabric for:

- direct external observations and effects;
- deterministic boundary workflows;
- embedded single-principal operation;
- authenticated multi-principal operation;
- communication ingress and egress;
- managed worker and sandbox boundary control;
- optional portable-identity and proof integration;
- reliable operation over `AQ-CONT-1` durable execution;
- comparable, privacy-preserving boundary evidence for Exoskeleton-owned developmental campaigns using existing causal, receipt, uncertainty, provenance, and lifecycle seams.

The repository should emerge with a small foundational vocabulary:

```text
principal and actor
verified agency
canonical resource or audience
capability and authority
submission and intent
observation and effect
execution plan
receipt and evidence
uncertainty and reconciliation
flow trace
worker-control boundary
```

It should not emerge as a universal catalog of specialist techniques.

### 0.2 Decisions fixed by this plan

1. **ActionQueue is the sole durable task executor.** WorldInterface never creates a second scheduler, lease system, retry engine, or external-wait registry.
2. **Ingress and egress are both first-class boundary directions.** They share agency, resource, evidence, and protocol foundations but retain distinct lifecycles.
3. **The caller submits a request, not authoritative identity or authorization.** WorldInterface authenticates the caller, resolves operational agency, canonicalizes the resource, and creates the authoritative records.
4. **Intent is durable before consequential dispatch.** A crash before external execution leaves an inspectable `EffectIntent`, never an unaccounted operation.
5. **Resource and audience resolution precede final authorization.** Policy evaluates the exact canonical target, not an unresolved string selector.
6. **Unknown consequential effects fail closed.** Every executable capability has an admitted effect class, resource grammar, idempotency declaration, and reconciliation policy.
7. **Ambiguous provider outcome becomes `Uncertain`.** It does not become an ordinary retryable queue failure.
8. **Direct invocation and Flow compile to one `ExecutionPlan`.** Flow is an optional deterministic frontend, not a second execution fabric.
9. **Boundary receipts, workflow traces, evidence artifacts, and telemetry are distinct.** High-volume implementation details do not pollute durable causal history.
10. **Specialist coding technique leaves Fabric core.** Repository search, semantic code analysis, local Git work, compiler use, and test iteration belong in specialist sandboxes or verifier services.
11. **Managed workers are controlled through semantic boundary operations.** Work-order meaning and completion acceptance remain outside WorldInterface.
12. **Portable identity is optional and protocol-neutral.** EntityID, KERI, controlled identifiers, certificates, or future systems integrate through SPI boundaries; none becomes a Fabric core dependency.
13. **The target starts with fresh persistence.** Pre-contract WorldInterface stores, receipts, FlowSpecs, and runtime state are not read by the target runtime.
14. **The release pins an exact `AQ-CONT-1` implementation and conformance revision.** No fallback to old ActionQueue events or fire-and-forget submission exists.
15. **One target authority path exists.** Offline fixture comparison is permitted; live old/new authorization, receipt, or execution paths are forbidden.
16. **Developmental correlation is bounded attribution.** Campaign, intervention-arm, task-distribution, evaluation-plan, and component-fingerprint references cannot widen authority, alter priority, choose a connector, or accept a candidate.
17. **Boundary evidence is not performance judgment.** Receipts and observations preserve execution facts and known uncertainty; Exoskeleton owns task quality, binding-constraint diagnosis, saturation, and improvement acceptance.
18. **Experimental work uses the ordinary boundary path.** Research, benchmark, trial, maintenance, or self-improvement labels do not weaken resource resolution, acting-capacity, information-use, approval, authorization, idempotency, or reconciliation.
19. **Comparative evidence preserves material differences.** Resource, policy, connector, environment, artifact, timing, resource-use, and uncertainty differences are pinned or exposed as confounds rather than normalized into a generic success result.

### 0.2.1 Foundational completeness additions

This revision keeps the fourteen-PR delivery shape and adds protocol-neutral support for:

- external commissioning after internal Vessel genesis;
- explicit acting capacity and role/stewardship attestations;
- information-use constraints distinct from access permission;
- candidate, provenance, trial, activation, rollback, quarantine, restore, migration, succession, and fork operations;
- lifecycle-supervisor boundaries and protected activation authority;
- temporal provenance and schema/policy dependency versioning;
- bounded developmental campaign causality and an evidence-completeness profile over existing receipts, observations, uncertainty, reconciliation, component provenance, and lifecycle effects.

These additions do not make WorldInterface the owner of Vessel identity, Purpose Charter, mission admission, beliefs, task distributions, metric semantics, binding-constraint diagnosis, saturation, improvement acceptance, continuity assessment, or narrative. They add no new public developmental protocol type.

### 0.3 What this plan deliberately does not do

This plan does not:

- define Exoskeleton cognition, commitments, relationships, attention, initiative, or narrative;
- make WorldInterface a general business-process engine unrelated to accountable boundary operations;
- treat a signature as proof of semantic truth;
- infer unique personhood from a cryptographic identifier;
- require EntityID, a blockchain, a token, or a global reputation system;
- preserve the current connector names as aliases;
- preserve the current receipt schema or FlowSpec wire format;
- migrate current experimental SQLite data or ActionQueue WAL state;
- allow a workflow, connector, worker, or caller to mint its own authority;
- put a delegated specialist’s internal tool loop into Fabric ontology;
- block WorldInterface development until every ActionQueue PR is complete;
- compute benchmark scores, select a winning intervention, diagnose intelligence/representation/harness bottlenecks, or declare saturation;
- treat a campaign or trial label as authorization, scheduler priority, approval, or permission to expose protected data;
- embed legacy accounting, ERP, software-maintenance, or other specialized domain ontology in Fabric core;
- add a free-form developmental metadata map to submissions or receipts.

Independent and assumption-bound work proceeds now. Upstream-facing runtime integration freezes only at the explicit gates in Section 3.

---

## 1. Baseline source map

### 1.1 Current workspace and target impact

The pinned repository contains ten workspace crates. Their target disposition is:

| Current crate | Current role | Target disposition |
|---|---|---|
| `worldinterface-core` | FlowSpec, IDs, descriptors, receipts, metrics, streaming types | Split into `protocol`, `capability`, identity/proof envelopes, and `flow-model`; delete flow-centric ownership from the foundation. |
| `worldinterface-flowspec` | Flow configuration, IDs, payloads, compile path | Rebuild as `worldinterface-flow-model` and `worldinterface-flow-compiler`. |
| `worldinterface-contextstore` | Durable step outputs and context | Retain with fresh schema as write-once artifact/output publication and completion support. |
| `worldinterface-connector` | Connector SPI, registry, context, receipt generation, transforms, native connector catalog | Split contracts from runtime execution; delete signal registry and specialist coding catalog; rebuild selected general and semantic connectors. |
| `worldinterface-coordinator` | Flow resolution, branch evaluation, handler, coordination | Rebuild as `worldinterface-flow-coordinator` over `AQ-CONT-1`; no blocked executor waits. |
| `worldinterface-host` | Embedded composition and host lifecycle | Become `worldinterface-embedded` plus runtime composition. |
| `worldinterface-http-trigger` | Webhook registration, handler, trigger receipt | Rebuild into `worldinterface-ingress` with authenticated bindings, provider deduplication, observation receipts, delivery outboxes, and acknowledgment. |
| `worldinterface-daemon` | HTTP server, routes, metrics, state | Rebuild as authenticated Fabric/Flow daemon; optional Platform control-plane modules remain separable. |
| `worldinterface-cli` | Current daemon/client commands | Replace with target principal, ingress, effect, flow, worker, identity, receipt, and reconciliation commands. |
| `worldinterface-wasm` | WASM component runtime and policy | Retain; align to target capability, transform, verifier, resource, artifact, and receipt contracts. |

The first implementation may stage some target boundaries as modules before splitting every logical boundary into a crate. The dependency direction is normative even when a boundary is temporarily intra-crate.

### 1.2 Current surgical points

The implementation plan is driven by these concrete seams:

1. **Flow-centric foundational types.** Current IDs, invocation context, and receipts assume flow run, node, and step identities. Target agency and effect semantics must make flow references optional causal context.
2. **Connector/runtime compression.** The current connector crate combines SPI, registry, transforms, signal coordination, execution context, receipt generation, and a large native catalog. These concerns require separate ownership.
3. **Ephemeral signal correctness.** `SignalRegistry` provides in-memory one-shot coordination for `signal.await` and `signal.emit`. It cannot survive restart or solve early-arriving signals. The entire path is deleted.
4. **Blocking delay/wait behavior.** Waiting must compile to `AQ-CONT-1` `Awaiting`, deadlines, and durable signals rather than occupying an executor or connector call.
5. **Coding-specific expansion.** The latest baseline added repository search, symbol, reference, semantic Rust, editing, testing, and repository-context operations to the global boundary catalog. These move to specialist execution environments or verifier profiles.
6. **Ingress as callback glue.** HTTP trigger and streaming mechanisms prove bidirectional operation, but they do not yet define a shared authoritative observation, binding, deduplication, downstream delivery, and acknowledgment lifecycle.
7. **Intent/receipt ordering.** Target execution must store intent before resolution and dispatch, then publish authoritative outcome and receipt after execution or reconciliation.
8. **Trusted-network daemon assumptions.** Multi-principal operation requires explicit authentication, tenant boundaries, policy administration, secret brokerage, quotas, and receipt access controls.
9. **Run identity used too broadly.** An ActionQueue `RunId` may support queue replay identity, but it is not the actor, effect identity, authorization, provider idempotency key, or proof identity.
10. **Direct and Flow paths need one executor representation.** Direct invocation must not remain a convenient bypass around the target intent, authorization, and receipt path.

### 1.3 Existing behavior classification

| Existing behavior | Classification | Target treatment |
|---|---|---|
| ActionQueue as sole durable executor | Retain | Re-prove against `AQ-CONT-1`. |
| DAG validation and deterministic branch evaluation | Retain and refine | Compile target Flow into shared `ExecutionPlan`; pure transforms stay I/O-free. |
| Write-once outputs | Retain | Reframe as artifact/output publication before completion. |
| Connector descriptors and registry | Retain and strengthen | Add effect class, resource grammar, idempotency, reconciliation, isolation, and evidence declarations. |
| Direct invocation | Replace | Direct submission compiles to a one-operation `ExecutionPlan`. |
| Existing receipt schema | Replace | Fresh direction-aware receipt and evidence model. |
| Webhook and streaming ingress | Replace around proven mechanisms | Port transports into authoritative ingress lifecycle. |
| WASM deny-by-default policy | Retain and align | Use target capability and artifact contracts; do not make WASM the delegated-agent lifecycle. |
| `signal.await`, `signal.emit`, `SignalRegistry` | Reject | Delete. |
| Blocking `delay` connector | Reject | Delete; use queue scheduling or wait deadlines. |
| Coding-specific connectors | Reject from Fabric ontology | Move to sandbox, specialist backend, verifier, or delete. |
| Trusted-network daemon defaults | Reject for Platform | Keep an explicitly bounded local-development profile only. |
| Compatibility principal or caller-asserted identity | Reject | Verified server-side principal resolution only. |

### 1.4 Current connector inventory and target disposition

The implementation begins with a complete machine-checked inventory. The following disposition is normative:

| Current connector or family | Target disposition |
|---|---|
| `signal_await`, `signal_emit`, `signal_registry` | Delete. |
| `delay` | Delete as connector; compile time conditions to ActionQueue eligibility, deadline, or wait. |
| `code_grep`, `code_fuzzy`, `code_glob`, `code_ls`, `code_read` | Specialist sandbox/toolchain. |
| `code_symbol`, `code_read_symbol`, `code_references`, `code_impls`, semantic Rust analysis | Specialist backend or optional verifier/analysis service. |
| `repo_context`, `repo_locate` | Specialist backend or evidence generator outside Fabric core. |
| `code_test`, `code_git_diff`, edit/write/apply-patch helpers | Sandbox-local technique; independent exported-result verification where useful. |
| `gitignore_check` | Worker-local check or verifier. |
| `fs_read`, `fs_write` | Rebuild as canonical `filesystem.read` and `filesystem.write` capabilities for accountable trust-boundary access. |
| `http_request` | Rebuild as canonical `http.request` with destination resolution, egress policy, credential binding, size/deadline limits, redirect rules, and explicit idempotency. |
| `shell_exec` | Replace by tightly scoped `process.execute` for fully mediated workloads; otherwise sandbox-local. |
| `sandbox_exec` | Replace by `sandbox.provision`, `sandbox.inspect`, `sandbox.release`, and `worker.*` lifecycle operations. |
| webhook and streaming sources | Port into `worldinterface-ingress`. |
| pure transforms | Retain under an I/O-free deterministic transform profile. |
| high-value semantic connectors | Retain only after capability admission review proves governance, reconciliation, verification, or explanatory value. |

There is no legacy-only connector category.

### 1.5 Evidence archive

Before code-bearing changes, tag `worldinterface/pre-wi-fabric-2` and archive:

- representative FlowSpecs and compiled DAG fixtures;
- current connector descriptors and complete catalog inventory;
- receipt and ContextStore examples;
- webhook and streaming fixtures;
- WASM manifests and policy tests;
- crash/recovery scenarios worth retaining;
- coding benchmark cases that exposed the abstraction leak;
- `signal.await`/`signal.emit` early/late ordering cases;
- direct-invocation and flow-output timing cases;
- baseline build, test, startup, throughput, and storage measurements.

Archived artifacts are inert evidence. No target crate imports an old reader.

---

## 2. Delivery model and repository workflow

### 2.1 Branch model

```text
main
    frozen experimental baseline after the archive tag

wi-fabric-2
    single target implementation branch
    one authority path
    fresh persistence lineage
    no compatibility feature flag
```

Short-lived feature branches merge into `wi-fabric-2` through the PR sequence in Section 7. The integration branch becomes the release source only after `WI-13` passes.

### 2.2 Clean target store

The target store begins with:

```text
contract: WI-FABRIC-2
fabric_schema: 1
context_schema: 1
flow_schema: 1
platform_schema: 1   # only when Platform profile is enabled
```

Startup behavior is precise:

- empty directory: initialize target stores;
- valid target manifest: open normally;
- nonempty pre-contract directory: reject without modification;
- unknown target schema: reject without modification;
- explicit future supported schema: run a documented target-to-target migration only after such a release exists.

Current receipts, FlowSpecs, SQLite files, and ActionQueue state are not upgraded.

### 2.3 Review policy

Every PR must:

- name the invariants it implements;
- identify whether each work item is Independent, Assumption-bound, Contract-blocked, or Integration-only;
- include property or state-machine tests for pure domain changes;
- include storage replay tests for durable changes;
- include crash-point tests for cross-store or external-effect changes;
- update the connector disposition manifest when touching a connector;
- update the ActionQueue assumption ledger when an upstream dependency is clarified;
- avoid introducing Exoskeleton types into WorldInterface;
- avoid accepting caller-supplied authoritative identity or authorization;
- avoid old-format readers or aliases.

### 2.4 Dependency posture

Development may use a path dependency or immutable Git revision of the `aq-cont-1` branch. The integration pin is updated only at the gates in Section 3.

WorldInterface must not pressure ActionQueue to preserve speculative API shapes. If downstream planning uncovers a missing generic primitive, the WorldInterface team files a structured upstream requirement describing the semantic need, failure scenario, and conformance case. ActionQueue remains free to choose the generic API that satisfies it.

### 2.5 Work maturity labels

| Label | Meaning | Merge rule |
|---|---|---|
| **Independent** | Does not depend on an unstable upstream runtime contract. | May merge after local tests. |
| **Assumption-bound** | Built against a named `AQ-CONT-1` design assumption. | May merge behind an internal interface; must reconcile at the named gate. |
| **Contract-blocked** | Correctness depends on a specific upstream conformance guarantee. | Do not merge the real adapter before the gate passes. Mocks and pure code may merge. |
| **Integration-only** | Cannot be proven without the upstream implementation and crash harness. | Complete only after the required gate. |

### 2.6 Release posture

`WI-FABRIC-2` publishes only after:

- `AQ-CONT-1` has a release candidate with passing pinned conformance;
- WorldInterface’s own embedded and daemon conformance pass against that exact revision;
- all forbidden legacy symbols are absent;
- fresh-store backup, restore, replay, and future-version rejection are proven;
- the Exoskeleton handoff fixtures in Section 19 are published.

---

## 3. AQ-CONT-1 assumptions and reconciliation gates

### 3.1 Why downstream planning happens now

WorldInterface is the first substantial consumer of `AQ-CONT-1`. Planning it before ActionQueue is complete validates that durable admission, signals, waits, checkpoints, resume context, and compound attempt commits are sufficient for real boundary lifecycles.

The WorldInterface plan therefore has two obligations:

1. avoid freezing speculative ActionQueue APIs into Fabric domain types; and
2. expose missing generic semantics early enough for ActionQueue to correct them before release.

### 3.2 Reconciliation gates

| Gate | Required ActionQueue evidence | WorldInterface work unlocked or frozen |
|---|---|---|
| **WI-AQ-G1 — Core lineage** | `AQ-02` and `AQ-03`: target IDs/state vocabulary and fresh persistence lineage stable | Freeze WorldInterface queue-reference wrappers, data-reference profile, store rejection fixtures, and causal-reference mapping. |
| **WI-AQ-G2 — Idempotent admission** | `AQ-04`: `ensure_task`/compound admission conformance passes | Merge real effect-dispatch and Flow task-admission adapters; freeze `ClientRequestId`/`EffectIntentId` to `AdmissionKey` mapping. |
| **WI-AQ-G3 — Continuations** | `AQ-05` through `AQ-07`: durable signals, `Awaiting`, deadlines, checkpoints, resume context pass | Merge ingress wake, callback/reconciliation waits, Flow wait/timer compilation, and worker-event resumption. |
| **WI-AQ-G4 — Attempt and workflow** | `AQ-08` and `AQ-09`: `AttemptDisposition` and transactional child coordination pass | Merge connector executor, Flow coordinator, compound child emission, and output/lease convergence. |
| **WI-AQ-G5 — Release candidate** | `AQ-13` conformance package passes and exact AQ RC is published | Freeze all queue-facing APIs, run full cross-stack crash suite, and permit `WI-13` release hardening. |

### 3.3 High-priority upstream assumptions

The machine-readable assumption ledger is a companion artifact. The most load-bearing assumptions are:

- repeated `ensure_task` with the same admission key and digest returns the same durable task identity;
- a conflicting digest under the same key fails loudly;
- signal admission is durable and deduplicated;
- a signal admitted before a wait can satisfy that later wait exactly once;
- wait establishment, checkpoint reference, run transition, and lease release are atomic;
- resume context includes the matched signal reference and checkpoint reference;
- cancellation and deadline races have deterministic, replayable winners;
- an attempt can atomically commit child admissions and a parent wait;
- ActionQueue propagates opaque causal references without interpreting them;
- executor traits affect routing only and never external-resource authority.

### 3.4 Upstream feedback protocol

A proposed ActionQueue change from WorldInterface must include:

```text
requirement ID
concrete WorldInterface lifecycle
failure or race if absent
minimum generic semantic guarantee
proposed conformance fixture
non-goals and domain-neutrality check
which WI PR is blocked
```

No request may mention a desired ActionQueue type such as `EffectIntent` or `Receipt`. The upstream requirement must be expressible in task, run, attempt, wait, signal, checkpoint, admission, cancellation, deadline, or opaque-reference terms.

---

## 4. Architectural decisions to lock

The following ADRs must be accepted by the PR shown. An ADR may choose a different concrete encoding than an example here, but it may not weaken the architecture invariants.

| ADR | Decision | Due |
|---|---|---|
| `WI-ADR-001` | Protocol version envelope, canonical serialization, hash input, unknown-field rules | WI-02 |
| `WI-ADR-002` | Identifier generation and stable client-request idempotency mapping | WI-02 |
| `WI-ADR-003` | Fresh store topology, SQLite transaction boundaries, backup/restore contract | WI-02 |
| `WI-ADR-004` | Principal authentication, actor binding, accountable-principal derivation | WI-04 |
| `WI-ADR-005` | Canonical resource URI grammar and resolver registry | WI-04 |
| `WI-ADR-006` | Authorization grammar, attenuation rules, decision explanation schema | WI-04 |
| `WI-ADR-007` | Effect state machine, commit-point vocabulary, `Uncertain`, and terminal outcomes | WI-04 |
| `WI-ADR-008` | Boundary receipt canonical form, content hashing, signing profile, and key rotation | WI-05 |
| `WI-ADR-009` | ContextStore versus artifact-store responsibility and publication atomicity | WI-05 |
| `WI-ADR-010` | Connector resolve/invoke/reconcile SPI and idempotency classes | WI-06 |
| `WI-ADR-011` | HTTP redirect, DNS, credential, SSRF, and replay policy | WI-06 |
| `WI-ADR-012` | Ingress provider identity, deduplication, ordering, gap evidence, and retention | WI-03 |
| `WI-ADR-013` | Subscriber delivery acknowledgment, retry, dead-letter, and privacy policy | WI-03 |
| `WI-ADR-014` | Common `ExecutionPlan`, plan hash, transform determinism, and Flow versioning | WI-07 |
| `WI-ADR-015` | Flow wait/timer lowering to `AQ-CONT-1` | WI-08 |
| `WI-ADR-016` | Agent Host transport, run credential, authoritative process-managed-proof-1 profile, executable/image representation, supported controls, artifact exchange and external-provider seam | WI-09; definition clauses before dependent provider admission/evidence work |
| `WI-ADR-017` | Capability admission checklist and connector quarantine | WI-10 |
| `WI-ADR-018` | Daemon authentication profiles, tenancy, organizational grants, and secret brokerage | WI-11 |
| `WI-ADR-019` | External identifier/identity-binding SPI, historical key state, and disclosure profiles | WI-12 |
| `WI-ADR-020` | Proof-bundle composition, issuer/executor/witness roles, and transparency publication | WI-12 |

### 4.1 Decisions that are not open during implementation

The implementation may not reopen these architectural boundaries without amending the normative design:

- caller-supplied `AgencyContext` is never authoritative;
- a workflow run is never the sole accountable principal;
- a connector descriptor never grants authority;
- a portable identifier never implies unique personhood or truth;
- a signature never substitutes for executor or provider evidence;
- root identity keys never enter ordinary connector or worker memory;
- an ActionQueue signal never becomes the authoritative domain event;
- a provider callback is stored before downstream wake;
- a self-generated purpose never widens authority;
- coding technique does not return to Fabric core for convenience.

---

### 4.2 Foundational decisions fixed by the amendment

1. Internal Vessel Genesis must preexist commissioning effects.
2. `ActingCapacityRef` is explicit; role attestations do not ambiently union authority.
3. `InformationUseEnvelope` is separate from resource access.
4. Candidate existence, verification, acceptance, activation, and retention are separate records.
5. Activation and rollback are lifecycle-supervisor effects, not ordinary worker privileges.
6. Build provenance and trial evidence cannot self-certify improvement.
7. Restore, migration, succession, and fork remain distinct operation classes.
8. Temporal provenance preserves source/assurance and does not claim semantic truth.
9. Schema and policy dependencies are pinned by exact semantic version.
10. The new seams use the existing AQ-CONT-1 primitives; no new ActionQueue domain concept is requested.

## 5. Target crate and module map

### 5.1 `worldinterface-protocol`

Proposed modules:

```text
version.rs
id.rs
principal.rs
agency.rs
reference.rs
artifact.rs
submission.rs
observation.rs
effect.rs
authorization.rs
receipt.rs
outcome.rs
flow_refs.rs
worker_refs.rs
identity_refs.rs
```

Responsibilities:

- protocol version envelope;
- domain IDs and opaque references;
- caller submission types that do not assert authority;
- authoritative observation/effect/authorization/receipt records;
- generic agency role references;
- protocol-safe artifact and content references;
- no connector implementation, store, policy engine, Flow parser, or Exoskeleton type.

### 5.2 `worldinterface-capability`

```text
effect_class.rs
resource.rs
scope.rs
descriptor.rs
idempotency.rs
reconciliation.rs
resolver.rs
connector.rs
transform.rs
verifier.rs
registry.rs
```

Responsibilities:

- closed effect-class and resource-operation vocabulary;
- canonical resource selector and resolved-resource interfaces;
- capability descriptors and grant attenuation;
- connector/transform/verifier contracts;
- explicit idempotency and reconciliation declarations;
- no final authorization policy or durable executor.

### 5.3 `worldinterface-runtime`

```text
admission/
agency_resolution/
authorization/
effect/
dispatch_outbox/
executor/
completion/
receipt/
reconciliation/
registry/
recovery/
aq_adapter/
storage/
```

Responsibilities:

- authenticate and resolve operational agency through injected control-plane interfaces;
- persist `EffectIntent` before dispatch;
- resolve exact resource/audience and perform final authorization;
- admit AQ work idempotently;
- execute connectors, publish outputs, generate receipts, classify uncertainty, reconcile;
- own no Flow parser and no Exoskeleton semantics.

### 5.4 `worldinterface-ingress`

```text
binding.rs
source_resolution.rs
provider.rs
dedup.rs
ordering.rs
observation.rs
receipt.rs
delivery.rs
ack.rs
outbox.rs
recovery.rs
http/
streaming/
```

Responsibilities:

- versioned authorized ingress bindings;
- provider/source canonicalization and authentication assurance;
- provider-event deduplication and ordering/gap evidence;
- durable `BoundaryObservation` and ingress receipt before notification;
- subscriber delivery outbox and idempotent acknowledgment;
- no conversation grouping, attention, truth judgment, or response obligation.

### 5.5 Flow crates

`worldinterface-flow-model`:

```text
spec.rs
node.rs
edge.rs
branch.rs
transform.rs
wait.rs
version.rs
```

`worldinterface-flow-compiler`:

```text
validate.rs
authority_analysis.rs
resource_analysis.rs
lower.rs
plan_hash.rs
diagnostics.rs
```

`worldinterface-flow-coordinator`:

```text
run.rs
trace.rs
handler.rs
resume.rs
branch_eval.rs
output.rs
aq_adapter.rs
recovery.rs
```

Flow model and compiler remain usable without starting the runtime. Coordinator depends on runtime protocol and `AQ-CONT-1` but does not bypass final node authorization.

### 5.6 `worldinterface-contextstore`

Target modules:

```text
manifest.rs
artifact.rs
publication.rs
output.rs
content_hash.rs
retention.rs
sqlite.rs
backup.rs
```

Responsibilities:

- immutable or write-once artifact storage;
- staged publication and completion visibility;
- content hashes and references;
- retention class and protected-content handling;
- no effect-state or authorization ownership.

### 5.7 `worldinterface-wasm`

Align current runtime modules to:

```text
component.rs
manifest.rs
capability_policy.rs
resource_adapter.rs
artifact_adapter.rs
connector_adapter.rs
transform_adapter.rs
verifier_adapter.rs
metering.rs
pool.rs
runtime.rs
```

A WASM module may implement a bounded connector, pure transform, verifier, or protocol adapter. It is not the universal host for long-lived specialist agents.

### 5.8 Identity and proof crates

`worldinterface-identity-spi`:

```text
external_identifier.rs
key_state.rs
binding.rs
resolver.rs
authenticator.rs
signer.rs
delegated_credential.rs
```

`worldinterface-proof`:

```text
statement.rs
proof_bundle.rs
attestation.rs
bond.rs
disclosure.rs
transparency.rs
verification.rs
```

Both are optional. Fabric protocol refers to bounded generic records and references; core runtime must operate with local authenticated principals and unsigned receipts when the selected deployment profile permits that.

### 5.9 Composition and service crates

`worldinterface-embedded` composes runtime, ingress, selected connectors, AQ, and stores in-process.

`worldinterface-daemon` exposes authenticated Fabric and Flow APIs, administration, health, metrics, and inspection.

`worldinterface-platform` supplies optional multi-principal control-plane services: organizations, tenants, policy administration, secret brokerage, quotas, audit access, and shared connector tenancy.

`worldinterface-cli` is a client and local administrative surface. It never becomes an alternate authority path.

### 5.10 Foundational lifecycle boundary

The target may begin with lifecycle types inside `worldinterface-protocol` and runtime handlers inside `worldinterface-runtime`. Promote a separate `worldinterface-lifecycle` or `worldinterface-deployment` crate only if deployment isolation, optional packaging, or credential boundaries justify it.

Logical ownership includes:

```text
protocol
    ActingCapacityRef · InformationUseEnvelope · CommissioningSubmission
    LifecycleOperation · CandidateArtifact · ComponentProvenance

runtime
    commissioning/lifecycle effect admission · authorization · reconciliation

proof
    build provenance · evaluation/trial references · activation proof bundles

platform
    role-attestation verification · lifecycle supervisor and separation of duties
```

### 5.11 Physical-crate staging rule

A logical boundary may begin as a module if creating a crate would add churn without enforcing dependency direction. It must become a crate before release when any of these are true:

- it is optional in a deployment profile;
- it must be independently fuzzed or conformance-tested;
- dependency inversion is security-relevant;
- downstream users need the types without the runtime;
- it would otherwise introduce a cycle;
- it hosts untrusted extension interfaces.

---

## 6. Critical path and parallel lanes

### 6.1 Critical path

```text
WI-01 evidence and contract
    ↓
WI-02 protocol and fresh persistence
    ├── WI-03 accountable ingress
    ├── WI-04 intent-first egress and authorization
    └── WI-05 receipt/evidence foundation
             ↓
          WI-06 connector and reconciliation runtime
             ↓
          WI-07 common ExecutionPlan and Flow
             ↓
          WI-08 AQ continuations and deletion of invalid waits
             ↓
          WI-09 managed workers and sandbox boundary
             ↓
          WI-10 connector rationalization closure
             ↓
          WI-11 authenticated daemon/platform
          WI-12 portable identity/proof SPI
             ↓
          WI-13 conformance and release hardening
             ↓
          WI-14 breaking release and EXO handoff
```

### 6.2 Parallel lanes

| Lane | May begin | Must wait for |
|---|---|---|
| Protocol IDs and pure state machines | Immediately after WI-01 | No upstream runtime. |
| Fresh SQLite schemas and repositories | WI-02 | Final AQ reference wrappers at WI-AQ-G1. |
| Ingress domain and transport adapters | WI-02 | Real AQ wake adapter at WI-AQ-G3. |
| Resource grammar and authorization reducer | WI-02 | Authentication profile decisions; AQ dispatch only at G2. |
| Receipt/evidence canonicalization | WI-02 | Connector commit-point details before final freeze. |
| Flow model/compiler | WI-02 | AQ coordinator lowering at G3/G4. |
| Connector deletion and sandbox disposition | WI-01 | No upstream dependency. |
| WASM contract alignment | WI-02 | Final connector SPI at WI-06. |
| Platform auth/control-plane domain | WI-02 | Runtime integration and remote conformance later. |
| Identity/proof SPI | WI-02 | Core Fabric stable enough to avoid identity-driven distortion. |

### 6.3 Merge discipline

A lane may merge pure domain code behind internal traits. It may not merge a fake durable adapter that silently uses current ActionQueue events, channels, or fire-and-forget submission. Mocks are test-only and must be named as mocks.

---

## 7. Pull-request implementation sequence

### WI-01 — Freeze evidence and establish the target development contract

**Maturity:** Independent  
**Prerequisites:** None  
**ActionQueue gate:** None

**Objective:** Create an immutable experimental baseline, make the clean-break posture executable in CI, and turn the architecture into repository-owned contracts before target code diverges.

**Primary areas:** `root workspace and CI`, `docs/adr`, `conformance/wi-fabric-2`, `fixtures/pre-wi-fabric-2`, `connector inventory tooling`.

**Implementation work**

- Tag `worldinterface/pre-wi-fabric-2` at the pinned baseline and record the full commit, toolchain, dependency lock, test command, and fixture hashes.
- Create and protect the `wi-fabric-2` integration branch. Freeze feature work on the experimental branch except evidence extraction.
- Add `contracts/stack.yaml` containing `STACK-2026-07-25-FOUNDATIONAL-2` and `contracts/wi-fabric-2.yaml` containing WorldInterface-owned invariants, IDs, conformance revision, and removed concepts.
- Create `conformance/wi-fabric-2/manifest.yaml`, fixture-version rules, test-package skeleton, and exact upstream pin fields.
- Generate a complete current connector/crate/type inventory and assign every item a target disposition.
- Archive representative FlowSpecs, receipts, webhook/stream messages, ContextStore outputs, WASM manifests, and failure cases without adding target readers.
- Add staged forbidden-symbol checks for `SignalRegistry`, `signal.await`, `signal.emit`, blocking `delay`, old receipt writers, compatibility principals, and coding-specific Fabric connectors.
- Add domain-leakage checks that reject Exoskeleton-specific types and mandatory EntityID/blockchain/token dependencies.
- Create the ADR queue from Section 4 and assign decision owners.
- Create the AQ assumption ledger and the issue template for upstream requirements.

**API/protocol impact:** No runtime API change. Repository contracts, manifests, fixture schemas, and CI policy become authoritative.

**Persistence impact:** Declare fresh target manifests and pre-contract directory rejection behavior; do not implement stores yet.

**Required tests**

- Baseline tag and lockfile reproduce current build/tests or document exact known failures.
- Inventory tool accounts for every current connector and workspace crate.
- Contract YAML parses and agrees with the normative architecture.
- Forbidden-symbol checks are staged and fail only when their scheduled deletion PR is reached.
- No target crate depends on archived fixtures.

**Exit gate:** The repository has a frozen evidence tag, one implementation branch, complete inventory/disposition, conformance skeleton, ADR queue, and executable clean-break checks. WI-02 issues are ready.

### WI-02 — Introduce target protocol vocabulary and fresh persistence skeleton

**Maturity:** Independent with AQ wrappers assumption-bound  
**Prerequisites:** WI-01  
**ActionQueue gate:** WI-AQ-G1 for final AQ reference wrappers

**Objective:** Create the target domain vocabulary and fresh durable stores without importing Flow-centric identity or legacy schemas.

**Primary areas:** `new protocol/capability crates or modules`, `runtime storage`, `contextstore manifest`, `store tooling`, `API schema fixtures`.

**Implementation work**

- Add target IDs for principals, actors, bindings, observations, deliveries, effects, authorizations, receipts, reconciliations, plans, workers, sandboxes, and proofs.
- Define `ClientRequestId`, generic opaque references, protocol version envelopes, canonical timestamps, content hashes, and artifact references.
- Define caller submission types separately from authoritative `AgencyContext`, `EffectIntent`, `BoundaryObservation`, `AuthorizationDecision`, and `BoundaryReceipt` records.
- Define pure state machines for ingress observation, subscriber delivery, effect intent, execution outcome, uncertainty/reconciliation, and proof publication.
- Create target SQLite manifests and repositories for Fabric domain state, Flow state, optional Platform state, and ContextStore artifacts.
- Implement repository transaction helpers and source-domain outbox primitives. Outboxes carry stable idempotency keys and opaque payload references.
- Implement pre-contract/nonempty directory rejection before opening any database.
- Add canonical serialization and digest fixtures for every record used in idempotency, authorization binding, or receipt hashing.
- Create AQ adapter interfaces using local wrapper traits; do not call the old ActionQueue API.
- Add repository-level query/read models distinct from mutation commands.

**API/protocol impact:** New `worldinterface-protocol` and `worldinterface-capability` public APIs. Old `worldinterface-core` target types are not re-exported.

**Persistence impact:** Fresh schema version 1 with store manifest, append-only event/audit records where required, state projections, outboxes, unique client-request keys, and immutable artifact references.

**Required tests**

- Property tests for every state-machine transition and illegal transition.
- Canonical serialization golden fixtures and stable digest tests.
- Duplicate client request with identical canonical request is idempotent; conflicting payload is rejected.
- Fresh initialization, valid reopen, unknown-version rejection, and pre-contract directory rejection.
- SQLite rollback tests prove no partial authoritative record/outbox pair.
- No flow/node/step identity is mandatory in foundational records.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Profile-bound worker/resource references and reviewed representation of process executable/configuration identity.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** Target protocol and state machines compile independently, fresh stores initialize/replay, canonical digests are fixed, and no old receipt/Flow/store reader exists.

### WI-03 — Build accountable ingress, observation delivery, and acknowledgment

**Maturity:** Independent domain; AQ wake adapter Contract-blocked  
**Prerequisites:** WI-02  
**ActionQueue gate:** WI-AQ-G3 for real durable wake/resume integration

**Objective:** Turn webhook and streaming callback mechanisms into one authoritative ingress lifecycle that stores provider evidence before notifying subscribers.

**Primary areas:** `worldinterface-ingress`, `current http-trigger transport code`, `streaming adapters`, `Fabric domain store`, `delivery outbox`, `ingress conformance`.

**Implementation work**

- Define `IngressBindingSubmission`, authoritative `IngressBinding`, binding version, canonical source resource, subscriber endpoint, authentication requirements, and delivery policy.
- Implement provider/source resolvers and transport-authentication adapters. Preserve assurance separately from semantic truth.
- Persist `BoundaryObservation` with provider identity, provider event key, source resource, asserted sender, ordering metadata, content/attachment artifact references, and observed timestamps.
- Implement provider-event deduplication, ordering/gap evidence, replay-window policy, and source-specific conflict handling.
- Generate an ingress-direction receipt in the same transaction as the observation and subscriber-delivery outbox.
- Implement idempotent `ObservationDelivery`, retry policy, dead-letter state, acknowledgment token, and retention rules.
- Port HTTP webhook and streaming source mechanisms directly into the new transport interfaces; do not wrap the old trigger registry.
- Provide a test-only subscriber and mock wake sink until WI-AQ-G3, then replace it with durable ActionQueue signal/task admission.
- Ensure downstream application acknowledgment occurs only after the application stores its own authoritative record.
- Add inspection APIs for binding, observation, provenance, deduplication, delivery, acknowledgment, and failure.

**API/protocol impact:** New ingress binding/observation/delivery API. Transport handlers submit provider events; they cannot directly construct application messages or attention semantics.

**Persistence impact:** Atomic observation + ingress receipt + subscriber-delivery outbox. Separate delivery-attempt and acknowledgment records. Provider deduplication index scoped by canonical provider/source identity.

**Required tests**

- Duplicate provider delivery produces one observation and one logical subscriber delivery.
- Crash after observation commit but before wake still causes later delivery.
- Crash after subscriber stores application state but before acknowledgment redelivers safely.
- Forged source, stale binding, wrong tenant, and invalid signature are rejected before observation admission.
- Ordering gaps are recorded rather than silently reordered into false certainty.
- Ingress receipt does not claim content truth, urgency, conversation membership, or authority.

**Exit gate:** HTTP and streaming ingress use the new binding/observation/delivery path; store-before-wake is proven with a mock sink and the AQ integration task is isolated behind one adapter.

### WI-04 — Implement intent-first egress, canonical resource resolution, and final authorization

**Maturity:** Assumption-bound; dispatch adapter Contract-blocked  
**Prerequisites:** WI-02  
**ActionQueue gate:** WI-AQ-G2 for `ensure_task` and stable admission identity

**Objective:** Create the authoritative outbound effect admission path from caller request to durable intent, exact resource resolution, explainable final authorization, and idempotent ActionQueue dispatch.

**Primary areas:** `runtime admission`, `agency resolution`, `resource resolvers`, `authorization reducer`, `dispatch outbox`, `AQ adapter`, `daemon request middleware`.

**Implementation work**

- Define `EffectSubmission` as a non-authoritative caller request containing effect class, resource selector, parameter artifact, purpose references, requested bounds, and `ClientRequestId`.
- Authenticate the requester through an injected control-plane interface and construct server-owned operational agency: requester, actor, accountable principal, delegation chain, sponsor, authorizer, and approver references.
- Implement canonical resource/audience resolution with resolver version, registry snapshot, observed resource version, and ambiguity/error handling.
- Implement structural authorization evaluation over exact resource, effect class, agency, grants, approvals, platform policy, and declared connector semantics.
- Persist `EffectIntent(Submitted)` and source outbox before any resolution-dependent or external dispatch side effect that could escape the process.
- Persist resolution and authorization decisions as versioned records linked to the intent; denial is an inspectable terminal outcome.
- Implement dispatch outbox mapping `EffectIntentId` and operation index to deterministic ActionQueue `AdmissionKey` and digest.
- At WI-AQ-G2, implement `ensure_task`; record the returned `TaskId` and separately discover/persist `RunId` mappings through authorized inspection and mark the admission outbox delivered after durable task acknowledgment. Run discovery is a separately recoverable projection, not part of the admission return payload.
- On repeated admission after a crash, accept `AlreadyExists` only when the canonical digest matches.
- Add explain APIs that show each attenuation and deny reason without interpreting unrestricted rationale text.

**API/protocol impact:** Public effect-submission API, server-owned agency/auth records, resource resolver SPI, policy evaluator SPI, and AQ dispatch adapter.

**Persistence impact:** Atomic intent + dispatch-outbox transaction; separate append-only resolution and authorization records; unique client-request index; deterministic intent-to-admission mapping.

**Required tests**

- Crash after intent commit but before AQ admission eventually dispatches exactly one task.
- Crash after AQ admission but before local outbox acknowledgment does not duplicate work.
- Same client request with different effect payload conflicts.
- Caller-supplied actor/principal/authorization metadata cannot override authenticated resolution.
- Resource alias, symlink, redirect, DNS, audience, and tenant ambiguity fail closed.
- Self-initiated purpose reference follows the same authorization path as external requests.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Admit the exact qualified provider profile; reject unsupported mandatory controls before launch without silent downgrade.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** Every outbound operation has a durable intent before dispatch, exact authorization is server-owned and explainable, and the AQ adapter passes idempotent admission conformance at G2.

### WI-05 — Implement boundary receipts, evidence classes, and completion publication

**Maturity:** Independent core; connector commit-point details Assumption-bound  
**Prerequisites:** WI-02; coordinates with WI-03 and WI-04  
**ActionQueue gate:** WI-AQ-G4 before final queue-completion convergence tests

**Objective:** Create one direction-aware evidence model and make receipt publication, artifact visibility, domain outcome, and queue completion converge after failure.

**Primary areas:** `protocol receipt/evidence`, `runtime receipt`, `contextstore publication`, `proof seams`, `inspection`, `crash harness`.

**Implementation work**

- Define immutable receipt headers with protocol version, receipt ID, direction, agency references, canonical resource, operation/effect class, timing, artifact hashes, executor identity, and causal references.
- Define separate ingress observation, outbound effect, communication-delivery, worker-control, and sandbox-grant receipt bodies.
- Define evidence classes: self-assertion, authorization evidence, executor receipt, provider/counterparty evidence, transparency evidence, and completeness scope.
- Define workflow traces and operational telemetry as separate schemas and stores/retention classes.
- Rebuild receipt generation so it consumes authoritative intent/observation, agency, resource, authorization, execution, and provider records rather than constructing truth after a raw connector call.
- Implement staged artifact/output publication: write immutable artifact, verify hash, publish reference, then make completion visible.
- Implement receipt canonical bytes and optional signing hook without requiring a portable identity provider.
- Add receipt disclosure/redaction profiles and protected-content references; do not embed secrets or large payloads by default.
- Add query APIs that reconstruct structural causal chains from intent/observation through receipt and downstream references.
- Add the `STACK-DEVELOPMENTAL-DIAGNOSTICS-1` evidence profile over existing causal references and evidence records: preserve bounded campaign/arm context, exact execution-envelope versions, explicit evidence gaps, and no task-quality semantics.
- Add comparison inspection that shows whether intended control arms used equivalent agency, canonical resources, policy/grant/approval versions, information-use envelopes, connectors, environments, and receipt profiles without declaring a winner.
- Add crash points around artifact write, publication, receipt commit, domain-state update, signal outbox, and AQ attempt commit.

**API/protocol impact:** New `BoundaryReceipt`, `EvidenceRef`, `ProofBundleRef`, `WorkflowTrace`, and telemetry APIs. Current step-shaped receipt is removed.

**Persistence impact:** Immutable receipt store/index, artifact publication journal, evidence links, trace store, telemetry retention store. Completion visibility is derived from published records, not temporary connector output.

**Required tests**

- Receipt bytes and digest are stable across replay.
- Receipt cannot exist without its authoritative intent/observation and canonical resource reference.
- Queue completion cannot expose an output reference that was not durably published.
- Crash at every publication boundary converges to one terminal receipt or one explicit uncertainty case.
- Redacted inspection preserves hashes and causal links without exposing protected content.
- A valid signature is reported as provenance, not semantic truth.
- Campaign or arm references cannot alter authorization, approval, connector selection, budget, or priority.
- A succeeded receipt exposes no winning-arm, binding-constraint, saturation, task-quality, continuity, or acceptance field.
- Material resource, policy, connector, environment, artifact, timing, and uncertainty differences remain visible as intended changes or confounds.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Separate enclosing-instance provenance from worker enforcement evidence; retain missing evidence and uncertainty.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** The target receipt/evidence model is the only active model, artifact publication precedes visible completion, and crash replay produces a single coherent causal record.

### WI-06 — Rebuild connector execution around resolve, invoke, commit observation, and reconcile

**Maturity:** Contract-blocked for real executor; pure SPI work Assumption-bound  
**Prerequisites:** WI-04, WI-05  
**ActionQueue gate:** WI-AQ-G4 for `AttemptDisposition` executor integration

**Objective:** Replace the current synchronous connector contract with explicit capability semantics, commit-point reporting, idempotency, and reconciliation.

**Primary areas:** `capability connector SPI`, `runtime executor`, `connector registry`, `reference connectors`, `WASM adapter`, `reconciliation service`.

**Implementation work**

- Define connector descriptors with effect class, resource grammar, input/output schemas, isolation profile, secret requirements, idempotency class, commit-observation semantics, reconciliation support, and evidence profile.
- Define target lifecycle methods or equivalent phases: validate/prepare, invoke, observe commit evidence, reconcile, and cancel where meaningful.
- Define idempotency classes: pure/read-only, naturally idempotent, externally keyed, compensatable, non-repeatable, and unknown. Unknown consequential connectors cannot register.
- Derive provider idempotency keys from stable effect/operation identity, never `AttemptId`.
- Implement executor handling that distinguishes pre-dispatch failure, confirmed not committed, confirmed committed, terminal provider failure, and ambiguous commit.
- Persist ambiguous outcomes as `Uncertain` and schedule reconciliation; do not return ordinary retryable failure.
- Implement reconciliation cases, deadlines, retry/backoff of reconciliation itself, evidence accumulation, operator escalation, and terminal `StillUnknown` policy.
- Rebuild reference `filesystem.read`, `filesystem.write`, and `http.request` connectors against canonical resource and authorization envelopes.
- Add at least one semantic communication/provider test connector with provider acceptance and reconciliation behavior.
- Align WASM connector adapter with the target SPI and deny registration when manifest declarations are incomplete.

**API/protocol impact:** New connector SPI, registry admission API, execution result/commit observation/reconciliation records, and quarantine controls. Existing `Connector::invoke` is removed.

**Persistence impact:** Connector registration snapshot, execution record, provider key mapping, commit observation, uncertainty case, reconciliation attempts, and terminal evidence.

**Required tests**

- Every registered connector passes effect-class/idempotency/reconciliation conformance.
- External-key connector crash after provider commit but before local receipt reconciles without duplicate effect.
- Non-repeatable connector cannot be automatically retried after ambiguous outcome.
- Filesystem canonicalization is symlink-safe and binds expected prior version where required.
- HTTP connector enforces destination, redirect, DNS/private-network, credential, body-size, and deadline policy.
- Connector cannot widen resource scope or substitute an authorization envelope.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Reconcile worker start/termination ambiguity and bounded host-supervisor effects using existing intent and reconciliation semantics.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** The current connector SPI and receipt-generation path are gone; reference connectors pass ambiguity and reconciliation suites; the real executor uses `AQ-CONT-1` attempt semantics.

### WI-07 — Compile direct submissions and Flow into one ExecutionPlan

**Maturity:** Flow model/compiler Independent; coordinator Contract-blocked  
**Prerequisites:** WI-04, WI-05, WI-06  
**ActionQueue gate:** WI-AQ-G4 for transactional child coordination

**Objective:** Make deterministic Flow an optional frontend over the same accountable operations used by direct submissions.

**Primary areas:** `flow-model`, `flow-compiler`, `flow-coordinator`, `runtime plan compiler`, `Flow store`, `trace APIs`.

**Implementation work**

- Define versioned Flow model containing boundary operations, pure transforms, branches, dependencies, waits/timers, inputs, outputs, and declared authority upper bounds.
- Define shared `ExecutionPlan` containing canonical operations, dependencies, resource selectors or bound resources, expected effect classes, required grants, transform hashes, and causal metadata.
- Implement a direct-submission compiler that produces a one-operation plan.
- Implement Flow validation for cycles, unreachable nodes, schema mismatch, hidden I/O, authority enlargement, ambiguous resource production, unbounded fan-out, and invalid wait semantics.
- Implement static authority upper-bound analysis. Runtime still performs exact node authorization after resource resolution.
- Implement deterministic pure transforms and branch evaluation with versioned transform/runtime hashes.
- Implement plan canonicalization and hash verification before execution.
- Build AQ-backed coordinator using transactional child admissions and durable parent waits; no process-local child submission channel.
- Persist Flow definition, plan, run, node disposition, branch trace, output references, and causal links separately from boundary receipts.
- Ensure a workflow run is the requester/execution mechanism, never the only accountable principal.

**API/protocol impact:** New Flow model/compiler/coordinator APIs and shared `ExecutionPlan`. Current FlowSpec types and compile path are removed rather than adapted.

**Persistence impact:** Fresh Flow definition/version store, immutable plan store, run projection, node trace, branch trace, and output links. AQ remains the task/run/attempt owner.

**Required tests**

- Direct and one-node Flow compile to semantically equivalent plans.
- Flow cannot introduce an operation or resource outside the submitting authority upper bound.
- Pure transform attempts hidden I/O and fails registration/conformance.
- Plan hash mismatch prevents execution.
- Crash during child admission yields all operations/dependencies/wait or none.
- Replay reproduces branch decisions and trace while receipts remain tied only to actual boundary crossings.

**Exit gate:** Direct and Flow paths converge on one plan/executor, Flow has no authorization bypass, and coordinator passes AQ transactional-child conformance.

### WI-08 — Delete invalid signal/delay machinery and lower waits to AQ-CONT-1 continuations

**Maturity:** Contract-blocked  
**Prerequisites:** WI-07  
**ActionQueue gate:** WI-AQ-G3 and WI-AQ-G4

**Objective:** Remove all process-local or blocking wait correctness paths and use durable queue continuations for callbacks, deadlines, and Flow waits.

**Primary areas:** `connector deletion`, `flow wait lowering`, `runtime callback/reconciliation coordination`, `AQ signal adapter`, `negative tests`.

**Implementation work**

- Delete `signal_await`, `signal_emit`, `SignalRegistry`, their connector descriptors, tests, registry entries, and documentation.
- Delete `delay` as a connector and any code path that sleeps while holding an execution lease.
- Define signal namespaces, kinds, correlation references, payload artifact profile, tenant/principal scoping, and deduplication keys for WorldInterface-owned wake events.
- Implement domain-record-plus-signal-outbox admission for provider callbacks, ingress delivery readiness, reconciliation outcomes, worker events, and Flow child completion.
- Compile Flow wait/timer nodes into `WaitSpec`, deadlines, and resumed coordinator logic.
- Handle early signal, late signal, duplicate signal, cancellation, deadline, and signal/deadline races using ActionQueue’s deterministic winner semantics.
- Ensure signal payload contains references to authoritative WorldInterface records, not authoritative data duplicated in the signal.
- Add operator inspection from WorldInterface domain record to AQ wait/signal references without interpreting AQ internals as effect state.
- Add static checks that forbid reintroduction of process-local wait registries or executor sleeps.
- Remove temporary mock wake sinks from WI-03.

**API/protocol impact:** WorldInterface-specific signal-admission adapter and Flow wait lowering; no public `signal.*` connector capability.

**Persistence impact:** Signal outboxes live with their authoritative WorldInterface records. AQ owns durable signal/wait state; WorldInterface stores only opaque references and delivery status.

**Required tests**

- Provider result stored before wait exists later wakes exactly once.
- Wait established before provider result wakes exactly once.
- Crash between WorldInterface record commit and AQ signal admission loses neither state nor eventual wake.
- Duplicate callback and duplicate signal do not double-resume.
- Deadline/cancellation/signal races replay identically.
- No executor remains leased while waiting.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Commit authoritative worker events before AQ wake; reprove duplicate, late, reordered and cancelled continuation handling.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** All old signal/delay machinery is absent, all waits use `AQ-CONT-1`, and lost-wakeup/blocked-executor negative tests pass.

### WI-09 — Add managed-worker and sandbox boundary capabilities

**Maturity:** Assumption-bound; end-to-end Integration-only  
**Prerequisites:** WI-06, WI-08  
**ActionQueue gate:** WI-AQ-G3/G4 for long-lived control/resumption

**Objective:** Provide a generic accountable boundary for specialist processes without embedding specialist reasoning or tool loops in WorldInterface.

**Primary areas:** `worker protocol`, `sandbox providers`, `Agent Host adapter`, `worker-control connectors`, `artifact exchange`, `hierarchical receipts`.

**Implementation work**

- Define `SandboxGrant`, isolation mode, image/runtime identity, mounted resources, network policy, secret policy, CPU/memory/process/time limits, telemetry level, and export policy.
- Define worker lifecycle operations: provision, start, inspect, signal/steer, checkpoint, suspend, resume, collect artifacts, terminate, and release.
- Define Agent Host protocol and short-lived run-scoped worker credential. The worker cannot self-assert Vessel/root principal identity.
- Implement one local process/container reference Agent Host behind the generic protocol.
- Implement fully mediated, sandboxed-native, and hybrid profiles. Hybrid keeps sensitive external effects mediated while allowing broad local technique inside the sandbox.
  These remain the broader target. The initial `process-managed-proof-1` realization is restricted by WI-WORKER-EXECUTION-1 and does not complete stronger or sensitive-execution claims.
- Persist sandbox/worker lifecycle records, grants, events, resource usage, exported artifact manifests, and boundary receipts.
- For candidate and controlled-trial environments, preserve image/runtime, toolchain, source revision, input/output artifact, provider profile, and declared resource-limit evidence without importing specialist task ontology.
- Implement worker events as authoritative WorldInterface records followed by AQ wake signals to owning coordinators.
- Implement hierarchical run receipt summarizing policy, inputs, outputs, resource use, semantic events, external receipts, and optional protected telemetry archive.
- Add verifier hooks for export scope, malware/secret scanning, artifact schemas, and declared acceptance evidence.
- Do not implement work-order quality, progress adequacy, or completion acceptance; those belong to the calling application.

**API/protocol impact:** Generic Agent Host and worker-control protocol, sandbox grants, worker event records, artifact manifests, and control capabilities.

**Persistence impact:** Sandbox grant/run/event store, worker credential references, resource accounting, export manifests, control outbox, and receipts.

**Required tests**

- Worker cannot escape mount/network/secret/resource policy.
  This is a full assurance requirement for profiles claiming those controls. The process proof declares its actual controls and exclusions; it does not satisfy hostile-code or independent-VM isolation by co-residing inside the Vessel VM.
- Run-scoped credential cannot invoke another run or widen authority.
- Crash during start/terminate/collect is reconciled without duplicate worker or lost artifact.
- Hybrid worker can use local tools but must cross Fabric for sensitive publication/communication/deployment.
- Exported artifact outside policy is quarantined and not published.
- Worker completion claim does not become application commitment completion.
- A worker or sandbox labeled as an experimental arm receives no wider authority than the ordinary grant permits.
- Candidate-environment provenance survives restart and allows Exoskeleton to detect material arm drift.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Own WI-WORKER-EXECUTION-1 and process-managed-proof-1; implement the bounded reference provider and WI-owned conformance harness.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** One reference Agent Host passes isolation and lifecycle conformance; WorldInterface exposes generic worker controls without specialist-domain concepts.

### WI-10 — Complete connector rationalization and target capability catalog

**Maturity:** Independent classification; reference implementations depend on WI-06/WI-09  
**Prerequisites:** WI-06, WI-09  
**ActionQueue gate:** No new AQ gate

**Objective:** Close the coding-driven abstraction leak, establish a small admitted capability vocabulary, and make connector admission an explicit governance process.

**Primary areas:** `connector catalog`, `capability registry`, `verifier profile`, `sandbox images`, `docs and examples`, `forbidden-symbol CI`.

**Implementation work**

- Remove all coding-specific connector source, registry entries, schemas, tests, and examples from Fabric runtime.
- Move useful code-search/edit/test utilities into a reference coding-agent sandbox image or separate example repository, not a WorldInterface dependency.
- Implement the target foundational capability set: `filesystem.read`, `filesystem.write`, optional `filesystem.enumerate`, `http.request`, tightly scoped `process.execute`, `artifact.import`, and `artifact.export`.
- Implement semantic communication send as a reference provider capability with explicit audience resolution and delivery milestones.
- Implement independent verifier examples: changed-path scope, artifact schema, test-report validation, secret scan, and patch metadata. Verification is not hidden connector I/O.
- Create capability admission checklist covering trust boundary, durable consequence, ambiguity, sensitive authority, independent evidence need, semantic governance value, frequency, and mediation cost.
- Create connector quarantine and disable controls for declaration failures or runtime anomalies.
- Publish a target connector-development guide covering canonical resources, authority, idempotency, reconciliation, artifacts, receipts, and privacy.
- Update WASM examples to bounded domain capabilities rather than code-agent technique.
- Enable final forbidden-symbol checks for all removed connector names and old registry surfaces.

**API/protocol impact:** Small target capability registry and connector admission tooling. No compatibility aliases.

**Persistence impact:** Connector registry snapshots, admission review metadata, quarantine state, and versioned descriptor hashes.

**Required tests**

- Every target connector passes admission and conformance checklist.
- Removed connector names cannot appear in target Flow validation or registry.
- Generic process/filesystem/network grants enforce exact resource and argument bounds.
- Verifier modules cannot perform undeclared external I/O.
- Connector registry snapshot is bound into authorization and receipt records.
- Coding specialist remains functional in sandbox acceptance fixture without Fabric coding connectors.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Register only qualified operations/profiles; require invalidation, quarantine and supported-profile inspection.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** The target catalog is small, fully classified, and conformance-tested; specialist coding functionality no longer shapes Fabric ontology.

### WI-11 — Rebuild embedded, daemon, CLI, and Platform control-plane surfaces

**Maturity:** Independent control-plane domain; runtime integration Assumption-bound  
**Prerequisites:** WI-03 through WI-10  
**ActionQueue gate:** WI-AQ-G5 before release-grade remote conformance

**Objective:** Expose the same Fabric semantics in embedded and authenticated remote profiles while adding optional multi-principal control-plane functions without forking the executor.

**Primary areas:** `worldinterface-embedded`, `daemon routes/middleware`, `platform domain`, `CLI`, `secret broker`, `quota/audit services`.

**Implementation work**

- Build `worldinterface-embedded` as the minimal in-process composition of target runtime, ingress, selected connectors, stores, and AQ adapter.
- Rebuild daemon APIs around authenticated principals and explicit local-development, single-tenant, and platform profiles.
- Implement request authentication middleware, principal/actor resolution, tenant and organization namespaces, policy-version selection, and control-mutation attribution.
- Implement secret brokerage that issues connector-scoped or worker-scoped credentials without exposing control-plane secrets to callers.
- Implement tenant quotas, connector concurrency policy, receipt/evidence access control, audit queries, and administrative disable/quarantine operations.
- Keep scheduling fairness in ActionQueue and provider-specific rate limits in WorldInterface; document the boundary.
- Rebuild CLI commands for principal/binding, ingress, observation/delivery, effect, authorization, receipt, reconciliation, flow, sandbox/worker, connector, identity/proof, backup, and explain surfaces.
- Ensure CLI and daemon call the same application services as embedded mode; no direct database mutation path.
- Add OpenAPI/protocol fixtures and generated clients only after target routes stabilize.
- Add explicit warnings and binding restrictions for the unauthenticated local-development profile.

**API/protocol impact:** Target embedded Rust API, authenticated daemon protocol, optional Platform administration API, and new CLI. Old route and command compatibility is not retained.

**Persistence impact:** Optional Platform store for organizations, tenants, principals, grants, secrets metadata, quotas, and audit access policy. Secret material lives in a selected custody backend, not ordinary domain rows.

**Required tests**

- Embedded and daemon profiles pass byte/semantic-equivalent protocol conformance.
- Cross-tenant principal, resource, receipt, signal, and secret access are denied.
- Caller cannot choose another principal through headers/body fields.
- Administrative control mutations are attributed and durable.
- Local-development profile cannot bind non-loopback by default and is clearly marked non-production.
- Backup/restore covers Fabric, Flow, ContextStore, and Platform stores consistently.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Expose the local provider through real embedded composition without requiring platform or remote-service completion for partial proof.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** Embedded and remote profiles expose one semantic runtime; authenticated multi-principal controls pass isolation tests; CLI has no bypass path.

### WI-12 — Add protocol-neutral portable identity and proof interfaces

**Maturity:** Independent optional layer; integration Assumption-bound  
**Prerequisites:** WI-04, WI-05, WI-11  
**ActionQueue gate:** No new AQ primitive; exact causal refs frozen at G1

**Objective:** Expose the seams needed by EntityID or comparable systems without making any identity protocol, chain, token, or reputation model mandatory.

**Primary areas:** `identity-spi`, `proof`, `platform bindings`, `receipt signer`, `CLI/inspection`, `reference adapters`.

**Implementation work**

- Define `ExternalIdentifier`, identifier method registry, time-versioned `IdentityBinding`, key-state snapshot, assurance level, validity, revocation, recovery, and disclosure policy.
- Define resolver, authenticator, signing-provider, delegated-credential, and transparency-publisher SPIs.
- Define proof-bundle records that distinguish actor-signed request, identity binding snapshot, authorization decision, executor receipt, provider/counterparty evidence, and transparency receipt.
- Define contextual trust-attestation and optional bond envelopes as evidence/effect types without global reputation or token assumptions.
- Implement a deterministic in-memory/test identity adapter and one independent practical adapter profile; keep protocol-specific dependencies outside core crates.
- Implement root-key custody boundary: runtime requests signing by reference; root private material never enters connector, worker, model, or general Fabric memory.
- Implement key rotation/revocation historical verification and bind receipt verification to key state at signing time.
- Implement local-only, direct-disclosure, selective-disclosure, batch-anchor, domain-transparent, and public publication profiles as policy choices.
- Add inspection language that distinguishes signature validity, authorization, execution evidence, provider evidence, transparency, and semantic acceptance.
- Add negative tests proving external identifier, signature, stake, or sponsor cannot widen authority.

**API/protocol impact:** Optional identity/proof SPI crates and control-plane bindings. Fabric core continues to work with local principals and bounded unsigned profiles.

**Persistence impact:** Identity-binding and key-state references, signed statements, proof-bundle manifests, disclosure decisions, attestation/bond records, transparency publication outbox.

**Required tests**

- Two independent adapters pass the same protocol-neutral conformance.
- Historical signature verifies against the key state valid at signing time.
- Revoked/delegated key behavior follows binding policy.
- Root key bytes are absent from logs, stores, connector inputs, worker inputs, and crash dumps in test profile.
- Signature validity does not mark a claim true or an effect completed.
- EntityID-specific types do not appear in `worldinterface-protocol`, `capability`, or core runtime dependencies.

**Exit gate:** Portable identity and proof are genuinely optional, protocol-neutral, key-lifecycle aware, and incapable of bypassing operational authorization.

### WI-13 — Complete conformance, crash, security, load, backup, and release hardening

**Maturity:** Integration-only  
**Prerequisites:** WI-01 through WI-12  
**ActionQueue gate:** WI-AQ-G5 exact AQ release candidate and conformance pin

**Objective:** Prove the complete target contract against the exact ActionQueue release candidate and remove all development-only uncertainty before publication.

**Primary areas:** `conformance/wi-fabric-2`, `chaos harness`, `security tests`, `load tests`, `backup/restore`, `release manifests`, `forbidden-symbol audit`.

**Implementation work**

- Populate `conformance/wi-fabric-2` with black-box embedded and daemon fixtures for every normative invariant and handoff.
- Pin exact ActionQueue implementation and `aq-cont-1` conformance revisions; fail CI on drift.
- Run deterministic crash injection across ingress admission/delivery, effect intent/outbox, AQ admission, connector commit, artifact publication, receipt/domain commit, reconciliation, Flow child/wait coordination, and worker lifecycle.
- Run model/property tests for resource attenuation, authorization, state machines, deduplication, Flow authority, and receipt/proof composition.
- Run security suites for principal spoofing, SSRF, path traversal/symlink races, secret leakage, cross-tenant access, forged callbacks, replay, worker escape, and receipt disclosure.
- Run performance/load suites for high-rate ingress, effect dispatch, retained uncertainty, Flow fan-out, receipt indexing, ContextStore artifacts, and daemon fairness.
- Run backup, restore, point-in-time consistency, corruption detection, unknown-version rejection, and target-to-target migration scaffold tests.
- Run EXO-V3 consumer fixtures for communication ingress, communication egress uncertainty, direct file observation, delegated worker control, self-initiated effect authorization, and a multi-arm developmental campaign.
- Run `wi-fabric-2-developmental-experiment-acceptance-matrix.yaml` for bounded causal attribution, experimental/ordinary authorization parity, execution-envelope evidence, uncertain-arm reconciliation, protected fixture handling, and semantic non-ownership.
- Perform a complete forbidden legacy/dependency audit and delete temporary mocks, development bridges, archived-fixture readers, and offline comparison harnesses from production targets.
- Publish the conformance manifest, fixture hashes, performance baseline, security assumptions, and known limitations.

**API/protocol impact:** No new architecture API. Freeze protocol and exact dependency revisions; only defect fixes are allowed after this PR begins.

**Persistence impact:** Freeze schema/version 1, backup/restore format, receipt/proof canonicalization, and supported future migration policy.

**Required tests**

- All target conformance passes in embedded and daemon profiles.
- Every injected crash converges to a valid authoritative state with no lost observation, unaccounted effect, duplicate non-idempotent effect, or orphaned completion.
- No target binary links old signal/coding/receipt/compatibility code.
- Performance stays within accepted regression budgets against archived baseline where comparison is meaningful.
- Fuzz/property tests run for the agreed duration with no invariant breach.
- EXO consumer fixtures pass against the published candidate protocol.
- Developmental evidence fixtures prove no WorldInterface API or record can select a winning arm, diagnose a binding constraint, declare saturation, or accept a candidate.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Collect profile-specific provider evidence separately from stronger isolation, remote parity and full-release obligations.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** `WI-FABRIC-2` is fully proven against the exact `AQ-CONT-1` RC, schemas and APIs are frozen, and release evidence is publishable.

### WI-14 — Publish the clean-break release and downstream handoff

**Maturity:** Integration-only  
**Prerequisites:** WI-13  
**ActionQueue gate:** Published exact `AQ-CONT-1` target release

**Objective:** Cut the first supported WorldInterface contract, publish the downstream integration package, and establish future stability discipline.

**Primary areas:** `release manifests`, `crate publication`, `tags`, `documentation`, `EXO handoff package`, `support policy`.

**Implementation work**

- Remove remaining development-only scaffolding, provisional feature gates, mock adapters, and offline comparison tools from release targets.
- Publish target crates in dependency order with exact ActionQueue version and conformance revision pins.
- Tag the repository release and publish `WI-FABRIC-2` contract revision, schema versions, fixture hashes, checksums, and release notes.
- Publish embedded and daemon deployment guides, security profiles, backup/restore procedures, operator runbooks, and connector development guide.
- Publish the EXO-V3 handoff package defined in Section 19: protocol clients, fixtures, causal-reference conventions, ingress ack contract, effect/outcome contract, worker-control contract, developmental-experiment evidence profile, and crash scenarios.
- Publish `STACK-DEVELOPMENTAL-DIAGNOSTICS-1` as an additive conformance profile with an explicit statement that it adds no lower-layer protocol primitive or authority path.
- Document supported target-to-target compatibility policy beginning with this release; explicitly state that pre-contract stores and APIs are unsupported.
- Create post-release defect and security response process without reopening old runtime compatibility.
- Archive the implementation branch according to repository policy and make the release branch/main the sole target authority.
- Open the WI-FABRIC-2 follow-up backlog only for evidence-backed improvements, not postponed legacy support.

**API/protocol impact:** First supported breaking `WI-FABRIC-2` API and protocol release.

**Persistence impact:** First supported target schema lineage. Future releases owe documented target-to-target migration/backup/restore support from this point.

**Required tests**

- Published crates install and conformance-test from a clean environment.
- Release bundle checksums and fixture hashes verify.
- Downstream sample compiles against published versions without path dependencies.
- Pre-contract data-directory rejection remains precise and non-destructive.
- No unpublished workspace crate is accidentally required by the public surface.

**Worker execution profile obligations (additive, not a substitute exit gate)**

- Publish the exact worker-provider definition, implementation/evidence pins, declared limitations and downstream handoff.

See `WI-WORKER-EXECUTION-1`; narrower checkpoint evidence may be produced in a partial slice, but outstanding full-parent obligations remain open.

**Exit gate:** The release is published, reproducible, conformance-backed, and consumable by EXO-V3. The experimental runtime is no longer part of the active implementation path.

### 7.15 Foundational allocation across the fourteen PRs

The implementation sequence remains fourteen PRs. Add the following responsibilities:

| PR | Foundational additions |
|---|---|
| WI-02 | acting capacity, information-use, commissioning, lifecycle, candidate, provenance, and temporal protocol/store records |
| WI-03 | commissioning and role/stewardship observation profiles where ingress carries them |
| WI-04 | acting-capacity and use-aware authorization; candidate/lifecycle effect admission |
| WI-05 | build, trial, activation, rollback, lifecycle proof bundles, bounded campaign causality, and developmental evidence profiles |
| WI-06 | reconciliation and commit-point semantics for lifecycle supervisors and deployment providers |
| WI-07 | deterministic composition of already-authorized commissioning/lifecycle effects |
| WI-09 | isolated candidate-build and lifecycle-host worker boundaries with reproducible environment and artifact provenance |
| WI-10 | admitted minimal lifecycle capability catalog; no arbitrary deployment shell |
| WI-11 | Platform role separation, lifecycle services, audit, and operator controls |
| WI-12 | role/stewardship attestations, key lifecycle, component signing, and proof profiles |
| WI-13 | conformance for commissioning, use constraints, candidate/active separation, activation/rollback, restore/fork, temporal provenance, authorization parity, evidence completeness, uncertainty, privacy, and semantic non-ownership |
| WI-14 | exact downstream fixtures plus the EXO-V3 developmental-experiment evidence handoff |

## 8. Persistence and recovery implementation detail

### 8.1 Store topology

The target separates semantic ownership even if multiple logical stores initially share one SQLite file in the embedded profile.

| Store/domain | Owns | Does not own |
|---|---|---|
| Fabric domain | principals/bindings refs, ingress observations, deliveries, effect intents, resolutions, authorizations, outcomes, receipts, reconciliations, outboxes | Artifact bytes, AQ runs, Exoskeleton commitments |
| Context/artifact | immutable content, staged publication, hashes, retention, quarantine | Effect lifecycle, authority |
| Flow | definitions, plans, runs, node/branch traces, output links | Connector receipts, AQ task lifecycle |
| Platform optional | organizations, tenants, principal bindings, grant administration, secret metadata, quotas, audit access | Secret key bytes, application cognition |
| ActionQueue external | tasks, runs, attempts, waits, signals, checkpoints | Effect/observation meaning |

Every store begins with a manifest containing contract label, schema version, instance ID, creation time, feature profile, and canonical serialization profile.

### 8.2 Core logical records

The target schema must support at least:

```text
PrincipalBindingRecord
ExternalIdentityBindingRecord
IngressBindingRecord
BoundaryObservationRecord
ObservationDeliveryRecord
EffectIntentRecord
ResourceResolutionRecord
AuthorizationDecisionRecord
ExecutionRecord
BoundaryReceiptRecord
UncertaintyCaseRecord
ReconciliationAttemptRecord
ProofBundleRecord
FlowDefinitionRecord
ExecutionPlanRecord
FlowRunRecord
WorkflowTraceRecord
SandboxGrantRecord
WorkerRunRecord
OutboxRecord
StoreManifest
```

Records that establish history are append-only. Mutable projections are rebuildable from authoritative records or use explicit version checks.

### 8.3 Ingress transaction boundaries

Provider admission:

```text
verify transport and binding
    ↓
BEGIN
  deduplicate provider event
  store BoundaryObservation
  store ingress BoundaryReceipt
  store ObservationDelivery or delivery-outbox entries
COMMIT
    ↓
submit downstream deliveries
```

Subscriber acknowledgment:

```text
subscriber stores its own authoritative state
    ↓
ack DeliveryId + delivery token
    ↓
BEGIN
  record acknowledgment
  close or advance delivery state
COMMIT
```

A transport handler never calls a downstream application before the observation transaction commits.

### 8.4 Egress transaction boundaries

Admission:

```text
validate request envelope and idempotency key
    ↓
BEGIN
  store EffectIntent(Submitted)
  store dispatch-outbox entry
COMMIT
```

Resolution/authorization may be performed before or after the initial transaction according to ADR-004 through ADR-007, but no external connector dispatch may occur before the intent exists. The final resolved resource and authorization record are immutable inputs to execution.

Dispatch:

```text
read undelivered outbox
    ↓
AQ ensure_task(persisted EnsureTaskRequest with stable TaskId)
    ↓
BEGIN
  store AQ task/run references
  mark outbox delivered
COMMIT
```

A crash after AQ admission and before local acknowledgment causes safe resubmission.

### 8.5 Execution and completion boundaries

The executor performs:

```text
load intent + resolved resource + authorization snapshot
validate connector descriptor/registry snapshot
prepare provider idempotency identity
invoke connector
classify commit observation
stage artifact/output
publish artifact/output
BEGIN
  append execution outcome
  append receipt or Uncertain record
  append downstream signal outbox if needed
COMMIT
return AQ AttemptDisposition
```

The exact ordering between WorldInterface commit and AQ attempt completion cannot be one cross-WAL transaction. Recovery must reconcile both directions:

- WorldInterface terminal outcome exists, AQ attempt not completed: redispatch handler detects terminal outcome and returns the corresponding completion without reinvoking provider.
- AQ attempt completion exists, WorldInterface terminal outcome absent: conformance should make this state unreachable; recovery treats it as corruption and fails closed.
- Provider may have committed, WorldInterface outcome absent: execution re-enters reconciliation, not blind invoke.

### 8.6 Reconciliation transaction boundaries

```text
load UncertaintyCase
perform provider query or counterparty check
stage evidence
BEGIN
  append ReconciliationAttempt
  append evidence references
  transition intent to ConfirmedCommitted / ConfirmedNotCommitted /
      StillUncertain / Escalated
  append receipt or updated proof bundle when justified
  append signal outbox
COMMIT
```

Reconciliation itself may be retried as ordinary read/query work when safe. The original effect is not repeated unless provider evidence establishes that it did not commit and policy explicitly permits another execution.

### 8.7 Flow persistence

A Flow definition is versioned and immutable after activation. Compilation creates an immutable `ExecutionPlan` with a plan hash. A run binds to exactly one plan hash, registry snapshot, and submitting authority upper bound.

Flow traces record deterministic orchestration events. Actual boundary operations reference their effect intents and receipts. Pure transform outputs are artifact references or bounded inline values with transform/runtime hashes.

### 8.8 Recovery phases

On startup:

1. validate manifests and schema versions;
2. open stores read/write only after validation;
3. verify critical indexes and canonical digest constraints;
4. recover incomplete artifact publications;
5. recover source outboxes;
6. reconcile AQ task mappings;
7. discover execution outcomes not reflected in AQ and terminal AQ runs lacking WorldInterface state;
8. recover ingress deliveries and acknowledgments;
9. schedule uncertainty reconciliation;
10. recover Flow coordinators and worker-control operations;
11. expose ready/healthy only after invariants hold.

### 8.9 Future evolution discipline

The clean break ends at the first target release. After `WI-FABRIC-2` is published:

- schema changes require explicit migrations and rollback/restore tests;
- protocol changes require version negotiation or breaking release policy;
- receipt canonicalization cannot change silently;
- connector descriptor semantics are versioned;
- backup/restore compatibility is part of release qualification;
- historical proof verification retains the original canonical/profile metadata.

---

### 8.10 Foundational record families

The Fabric store adds:

```text
acting_capacity_evidence
information_use_envelopes
commissioning_operation_state
candidate_artifacts
component_provenance
trial_and_evaluation_refs
activation_and_rollback_intents
lifecycle_supervisor_receipts
restore_migration_succession_fork_classification
```

Large builds, models, logs, and source archives remain artifact references. Candidate existence never updates the active-runtime pointer. Activation and rollback are separate authoritative events.

## 9. Runtime algorithms and invariants

### 9.1 Direct effect submission

```text
submit(request, authenticated_session)
  canonical_request = validate_and_canonicalize(request)
  existing = lookup_client_request(canonical_request.client_request_id)
  if existing:
      return same intent if digest matches
      else conflict

  agency = resolve_operational_agency(authenticated_session, request.references)
  intent = create_submitted_intent(canonical_request, agency)
  persist(intent + dispatch_outbox)
  return intent.id
```

Resolution and final authorization advance the intent through explicit states. They do not mutate the original submission.

### 9.2 Resource resolution and authorization

```text
selector
  → resolver registry snapshot
  → canonical resource + observed version + resolver evidence
  → authority attenuation chain
  → platform/resource policy
  → exact AuthorizationDecision
```

The authorization decision binds:

- intent ID and request digest;
- actor/accountable principal/delegation references;
- effect class;
- exact canonical resource or audience;
- operation and parameter constraints;
- connector/registry snapshot where relevant;
- approval and grant references;
- expiration and one-use/replay policy;
- explanation trace.

### 9.3 ActionQueue dispatch

Each effect operation derives a stable admission tuple:

```text
namespace = worldinterface/effect
key       = EffectIntentId + operation_index
payload   = immutable execution reference
causal    = opaque references to intent, agency, purpose, authorization
```

Only the AQ adapter knows the concrete AQ types. Fabric domain records store opaque AQ references and admission status.

### 9.4 Connector execution guard

Before invocation, the handler reloads:

- current intent state;
- exact authorization and expiration;
- resolved resource and version constraints;
- connector registry snapshot and descriptor;
- prior execution/uncertainty/reconciliation state.

If a terminal outcome already exists, it returns the matching AQ disposition without provider invocation. If authority expired before dispatch and policy does not permit grandfathered execution, it records denial/cancellation rather than executing.

### 9.5 Ingress delivery

Observation delivery is at-least-once until acknowledged, with application-level idempotency by `DeliveryId`. WorldInterface does not assume that HTTP 200 alone means the application stored authoritative state unless the subscriber protocol defines it as the acknowledgment operation.

### 9.6 Communication egress

Semantic `message.send` resolves:

```text
provider account
sender identity
channel or mailbox
thread/reply target
exact audience
content artifact
required delivery milestone
provider idempotency support
```

Generated content is not sent state. Provider acceptance, delivery, read/acknowledgment, failure, uncertainty, and reconciliation remain distinct evidence milestones.

### 9.7 Flow execution

The compiler lowers each boundary node to an operation descriptor and each pure node to a deterministic transform descriptor. The coordinator uses AQ task dependencies and transactional child admission. Exact node authorization occurs immediately before dispatch against runtime-resolved resources.

A branch selects among already admitted possibilities. It cannot introduce a new effect class or resource scope.

### 9.8 Managed worker execution

WorldInterface controls worker lifecycle and sandbox boundary. It does not decide whether work is worth doing or whether output satisfies an application commitment. The worker’s completion event is evidence and a wake trigger, not acceptance.

---

### 9.9 Commissioning operation

```text
receive CommissioningSubmission referencing existing Genesis
    → authenticate requester and accountable principal
    → resolve exact external principal/persona/ingress/credential targets
    → apply acting-capacity, purpose, and information-use policy
    → persist EffectIntent and outbox
    → execute through ordinary connector lifecycle
    → store receipts or Uncertain
```

The adapter never creates or updates internal Vessel Genesis.

### 9.10 Candidate build and activation

```text
candidate.build
    → isolated builder / worker
    → immutable candidate artifact + provenance

runtime.activate
    → require external acceptance reference and lifecycle authority
    → checkpoint active runtime
    → supervisor transition
    → health/continuity evidence
    → ActivationReceipt or Uncertain

runtime.rollback
    → separate exact authorization
    → restore approved known-good target
```

A candidate, test report, signature, campaign reference, or claimed winning-arm result cannot invoke activation on its own.

For controlled developmental work, each ordinary operation carries bounded causal references to an immutable Exoskeleton-owned campaign execution context. WorldInterface preserves exact resource, agency, policy, information-use, connector, environment, artifact, timing, outcome, and uncertainty evidence. It does not score the arm or interpret the changed lever.

## 10. Connector and capability implementation strategy

### 10.1 Initial reference capability set

The first release should contain enough reference implementations to prove the fabric without recreating a broad automation marketplace:

1. `filesystem.read`
2. `filesystem.write`
3. `http.request`
4. `artifact.import`
5. `artifact.export`
6. tightly scoped `process.execute` for fully mediated tests
7. semantic `message.send` test/reference provider
8. `sandbox.provision` / `sandbox.release`
9. `worker.start` / `worker.inspect` / `worker.collect` / `worker.terminate`

`filesystem.enumerate` may be included if its resource semantics and receipt value are justified. Code-specific names are not.

### 10.2 Capability admission checklist

A dedicated semantic capability is admitted only when at least one is true:

- it crosses a meaningful trust boundary;
- it mutates durable/shared state;
- its outcome can be ambiguous after failure;
- it exercises sensitive authority;
- independent evidence is required to discharge a commitment;
- domain semantics materially improve authorization, reconciliation, verification, or explanation.

The review also considers frequency and mediation cost. High-frequency, reversible, sandbox-local technique should stay inside the sandbox.

### 10.3 Descriptor requirements

Every connector descriptor declares:

```text
effect class
resource grammar and resolver
operation schema
input/output artifact profile
idempotency class
provider key mapping
commit point or observable commit evidence
reconciliation support
retry boundaries
isolation profile
secret requirements
time/memory/output limits
receipt/evidence profile
privacy/retention class
```

Missing or contradictory declarations fail registration.

### 10.4 Connector versioning

A connector implementation version and descriptor hash are bound to authorization and receipt records. A connector upgrade may not reinterpret an already authorized operation. In-flight work either uses the bound implementation/profile or is canceled and reauthorized under explicit policy.

### 10.5 WASM profile

WASM remains ideal for bounded, portable, untrusted connectors, transforms, verifiers, and adapters. It is not expected to host Node/Python/JVM specialist runtimes or arbitrary process trees. Its capability manifest must map to the same canonical resource and authority model as native connectors.

---

## 11. Test migration and acceptance inventory

### 11.1 Existing tests

Classify current tests into:

- **retain concept:** invariant remains, test rewritten against target API;
- **replace:** useful scenario, current expected behavior is obsolete;
- **reject:** test protects removed behavior;
- **archive only:** diagnostic fixture used for historical comparison.

Likely retain/replace areas include Flow DAG validation, ContextStore write-once semantics, WASM policy, connector registry, embedded/daemon basic operation, HTTP trigger transport parsing, and ActionQueue recovery.

Tests protecting `SignalRegistry`, blocking waits, current receipt bytes, connector aliases, or coding-specific Fabric tools are rejected from the target suite.

### 11.2 Required acceptance families

#### Ingress

- authenticated binding and source canonicalization;
- duplicate, reordered, missing, and forged provider events;
- observation/receipt/outbox atomicity;
- subscriber redelivery and acknowledgment;
- crash between every phase;
- protected attachment quarantine.

#### Effect admission and authorization

- client request idempotency and conflict;
- caller identity spoofing;
- exact resource resolution and attenuation;
- grant/approval expiry;
- self-initiated effect equivalence;
- unknown effect fail-closed.

#### Connector execution

- pre-dispatch failure versus post-commit ambiguity;
- external key replay safety;
- non-repeatable effect behavior;
- output publication before completion;
- receipt/effect/AQ convergence;
- reconciliation and escalation.

#### Flow

- static authority bounds;
- deterministic plan hash and branches;
- pure transform no-I/O property;
- transactional children;
- durable waits/timers;
- trace/receipt separation.

#### Workers and sandbox

- lifecycle idempotency;
- credential scoping;
- mount/network/secret/resource isolation;
- artifact export policy;
- hierarchical receipts;
- no completion self-certification.

#### Identity and proof

- key rotation/revocation history;
- two adapter equivalence;
- signature-versus-truth distinction;
- disclosure profile;
- actor/sponsor/witness separation;
- root-key exclusion.

#### Commissioning and role evidence

- Genesis reference required before external setup;
- idempotent principal, persona, ingress, and credential provisioning;
- role/stewardship attestation verification, expiry, revocation, and acting-capacity isolation;
- partial commissioning recovery without Genesis mutation.

#### Information use

- disclosure denied despite basic read access;
- audience and purpose constraints;
- worker and platform export restrictions;
- proof-publication and retention-trigger policy.

#### Development and lifecycle

- candidate/active separation;
- provenance and protected evaluation references;
- activation/rollback authorization and crash recovery;
- lifecycle-supervisor credential isolation;
- restore, migration, succession, and fork evidence;
- temporal provenance and schema-version replay.

#### Developmental experiment evidence

- bounded campaign and intervention-arm causal references;
- experimental and ordinary authorization-path equivalence;
- exact agency, resource, resolver, registry, grant, approval, policy, information-use, connector, environment, artifact, timing, resource, and uncertainty evidence;
- intended control-arm parity and explicit confound visibility;
- uncertain-arm reconciliation and crash-safe evidence publication;
- protected workload/artifact handling and audience restrictions;
- no benchmark scoring, winning-arm, binding-constraint, saturation, acceptance, or continuity semantics.

### 11.3 Crash-point matrix

At minimum, expose deterministic kill points:

```text
ingress: before/after observation transaction
         before/after subscriber outbox delivery
         before/after acknowledgment

egress: before/after intent transaction
        before/after AQ ensure_task
        before/after local outbox acknowledgment

execution: before provider call
           after call before commit classification
           after artifact stage
           after artifact publish
           before/after outcome+receipt transaction
           before/after AQ AttemptDisposition commit

reconciliation: before query
                after provider evidence
                before/after state+signal-outbox transaction

flow: before/after plan commit
      before/after child admission
      before/after wait establishment
      before/after branch trace

worker: before/after provision/start/terminate/collect/release
```

### 11.4 Model and property tests

Build small reference models for:

- effect state transitions;
- ingress dedup/delivery state;
- authority attenuation;
- resource-scope containment;
- outbox delivery;
- uncertainty/reconciliation;
- Flow plan/run/node disposition;
- worker lifecycle.

Generated command sequences compare implementation projections with model state after replay.

### 11.5 Cross-stack fixtures

WorldInterface owns fixtures that let Exoskeleton prove:

- ingress observation stored before application wake;
- delivery acknowledgment after application durable admission;
- communication send acceptance/delivery/uncertainty distinctions;
- self-initiated effect follows normal authorization;
- worker completion is evidence, not commitment completion;
- portable identity binding does not imply authority or truth.

---

## 12. CI and quality gates

### 12.1 Proposed CI jobs

```text
format-and-lint
workspace-build-minimal
workspace-build-all-features
unit-and-property
protocol-golden-fixtures
store-replay-and-recovery
sqlite-crash-matrix
aq-contract-adapter
embedded-conformance
daemon-conformance
flow-model-and-compiler
connector-conformance
wasm-conformance
ingress-transport-conformance
security-negative-suite
cross-tenant-suite
worker-sandbox-suite
identity-proof-neutrality
backup-restore
load-smoke
forbidden-legacy-symbols
domain-leakage
contracts-and-docs-consistency
```

### 12.2 PR gates by phase

| PR range | Required gates |
|---|---|
| WI-01 | contracts, inventory, baseline reproduction, staged forbidden symbols |
| WI-02 | protocol golden fixtures, state-machine/property, fresh store/replay |
| WI-03 | ingress conformance and crash matrix with mock wake; AQ adapter after G3 |
| WI-04 | auth/resource negative suite and AQ admission after G2 |
| WI-05 | receipt/artifact crash matrix and canonicalization |
| WI-06 | connector/idempotency/reconciliation conformance and AQ attempt adapter after G4 |
| WI-07/08 | Flow model/compiler, AQ workflow/continuation conformance, legacy wait deletion |
| WI-09/10 | sandbox and full connector catalog/disposition checks |
| WI-11/12 | remote/platform security and identity/proof neutrality |
| WI-13 | all jobs, long chaos/load, backup/restore, exact AQ pin, EXO fixtures |
| WI-14 | clean install/publish/reproducibility and release checksums |

### 12.3 Static architecture checks

CI should reject:

- dependencies from protocol/capability into runtime, Flow coordinator, Exoskeleton, or protocol-specific identity SDKs;
- direct SQLite writes from CLI/HTTP route modules;
- connectors constructing authoritative agency or authorization;
- use of ActionQueue `RunId` as provider idempotency identity without an explicit mapping record;
- `sleep`/blocking wait inside connector or coordinator execution paths;
- target source references to removed coding/signal connectors;
- secret-bearing fields in receipt/debug output;
- unsigned or unversioned canonical records used for digest/signature decisions.

---

## 13. Security and abuse-oriented implementation tasks

### 13.1 Principal spoofing and confused deputy

Authenticate transport/session independently of request body. Construct agency server-side. Bind delegated credentials to exact actor, accountable principal, purpose, effect class, resource scope, expiry, and replay policy.

### 13.2 Canonical resource attacks

Test path traversal, symlink replacement, hard links, case normalization, Unicode confusables, URL parser differences, redirects, DNS rebinding, private-network access, metadata endpoints, mailbox/channel aliases, and resource registry races.

Authorization binds the canonical identity and resolver snapshot, not only the original selector.

### 13.3 Secrets

Use reference-based secret brokerage. Prevent secrets from entering receipts, telemetry, Flow definitions, worker transcripts, or artifact exports. Rotate/revoke issued credentials and record only custody-safe metadata.

### 13.4 Replay and idempotency abuse

Protect `ClientRequestId`, provider idempotency key, ingress provider key, delivery acknowledgment, and ActionQueue admission key as distinct namespaces. Detect conflicting reuse and rate-limit unbounded unique-key attacks.

### 13.5 Connector declaration fraud

Treat descriptor claims as reviewed code assertions, not truth. Conformance, anomaly detection, quarantine, and conservative runtime fallback to uncertainty are required.

### 13.6 Cross-tenant and audit leakage

Every principal, binding, resource, receipt, artifact, Flow, worker, and query is tenant/organization scoped under Platform. Test enumeration, timing, error-message, and correlation leakage.

### 13.7 Worker escape and artifact smuggling

Constrain mounts, devices, network, process count, cgroups/limits, credentials, and host sockets. Quarantine exports; scan for secrets and policy violations before publication. Treat sandbox telemetry as potentially attacker-controlled.

### 13.8 Provider callback forgery

Verify provider signatures or authenticated sessions, binding state, replay windows, source identity, and tenant routing before observation admission. Preserve assurance level and gaps.

### 13.9 Proof and signature misuse

Separate signature-valid, authorized, executed, witnessed, transparent, and semantically accepted states in APIs and UI. Prevent a self-signed claim from presenting as an executor receipt.

### 13.10 Denial of service

Bound:

- ingress body/attachment size;
- deduplication retention and indexes;
- Flow nodes/fan-out/depth;
- artifact counts and bytes;
- outstanding uncertainty cases;
- reconciliation frequency;
- worker resource grants;
- receipt/proof query breadth;
- connector concurrency and provider rate limits.

---

### 13.11 Commissioning and lifecycle abuse

Test manifest substitution, role-union escalation, commissioning replay, candidate exfiltration, protected-test mutation, self-activation attempts, rollback-image tampering, lifecycle-credential theft, duplicate live restore, and activation uncertainty.

### 13.12 Information-purpose laundering

Test a principal or worker attempting to use, export, summarize, derive from, or publish information outside the permitted purpose/audience even when ordinary filesystem or object access exists.

## 14. Observability and operator surfaces

### 14.1 Questions the system must answer

For an ingress observation:

```text
Which binding admitted it?
Which canonical provider/source produced it?
What authentication assurance was established?
Was it deduplicated, reordered, or gap-affected?
Which subscriber deliveries exist and which acknowledged?
What content artifacts are protected or quarantined?
```

For an effect:

```text
Who requested it and on whose behalf?
What purpose and delegation chain were referenced?
What exact resource/audience was resolved?
Which grant, approval, and policy allowed or denied it?
Which connector/version executed it?
What provider idempotency key was used?
Did the provider commit, fail, or become uncertain?
What reconciliation evidence exists?
Which receipt and artifacts prove what?
```

For a Flow:

```text
Which definition/plan hash ran?
Which branch and transform decisions occurred?
Which nodes were suppressed, blocked, or awaited?
Which actual boundary receipts correspond to nodes?
Did any node request authority beyond the plan upper bound?
```

For a worker:

```text
Which sandbox grant and image/runtime were used?
What authority and credentials were issued?
What entered and left the sandbox?
Which external crossings were mediated?
What resource limits and telemetry summaries exist?
```

For a developmental operation:

```text
Which Exoskeleton-owned campaign and arm caused it?
Did the arm use the intended principal, acting capacity, resource, grants, approvals, policy, and information-use envelope?
Which connector, provider, image, toolchain, artifact, timing, resource, and uncertainty evidence is pinned?
Which material differences remain confounds?
Is this only a boundary fact, or has Exoskeleton separately admitted verification/evaluation evidence?
```

WorldInterface inspection never presents a winning arm or binding constraint.

### 14.2 Metrics

Suggested metrics:

```text
ingress_observations_total{provider,binding,result}
ingress_duplicates_total{provider}
ingress_delivery_lag_seconds{subscriber}
effect_intents_total{effect_class,state}
authorization_decisions_total{effect_class,result,reason}
aq_dispatch_outbox_age_seconds
connector_invocations_total{connector,outcome}
uncertain_effects_current{connector,effect_class}
reconciliation_age_seconds{connector}
receipts_total{direction,profile}
artifact_publication_failures_total
flow_runs_total{result}
flow_wait_duration_seconds
worker_runs_total{backend,result}
sandbox_policy_violations_total
tenant_quota_denials_total
developmental_effects_total{effect_class,outcome}
developmental_evidence_gaps_total{class}
developmental_authorization_mismatch_total{class}
```

Avoid high-cardinality principal, resource, campaign, arm, task-distribution, evaluation-plan, or component-fingerprint IDs as metric labels. Use traces/logs with access control for drill-down.

### 14.3 Structured logs and traces

Logs carry IDs and classifications, not raw secrets/content. Distributed trace context may cross ActionQueue and connectors, but trace identity is not authorization. Receipt IDs and intent IDs allow causal navigation after traces expire.

### 14.4 CLI command families

```text
wi principal ...
wi binding ingress ...
wi observation ...
wi delivery ...
wi effect submit|show|explain|cancel ...
wi authorization explain ...
wi receipt show|verify ...
wi reconcile ...
wi flow validate|compile|submit|inspect ...
wi sandbox ...
wi worker ...
wi connector ...
wi identity ...
wi proof ...
wi store verify|backup|restore ...
```

CLI commands call the same authenticated services as programmatic clients.

---

### 14.5 Foundational operator questions

The implementation must answer:

- Which acting capacity authorized this operation?
- Which information-use envelope governed this crossing?
- Which commissioning operation remains blocked or uncertain?
- Which candidate, source, builder, evaluation plan, and active runtime are involved?
- Who authorized and executed activation or rollback?
- Is this restore, migration, succession, or fork?
- Which schema and policy versions determined the decision?
- Which bounded campaign and intervention-arm references are preserved?
- Were the intended fixed boundary conditions equivalent, and which differences are explicit confounds?
- Which environment and artifact versions support reproduction?
- Can the operator distinguish receipt, uncertainty/reconciliation, verification, evaluation, acceptance, and activation? WorldInterface owns only the first two families.

## 15. Risk register

| Risk | Consequence | Mitigation / gate |
|---|---|---|
| AQ contract differs from assumptions | Adapter churn or missing durability | Assumption ledger and WI-AQ gates; generic upstream requirements before AQ freeze. |
| Protocol crate becomes a dumping ground | Foundational coupling returns | Dependency checks, explicit ownership, Flow/identity optional crates. |
| Receipt model grows into full event log | Cost/noise, hidden meaning | Separate receipt, trace, evidence, telemetry and retention tiers. |
| Resource canonicalization is inconsistent | Authorization bypass | Resolver conformance, canonical URI registry, bind resolver/version to decision. |
| Connector idempotency claim is wrong | Duplicate external effects | Mandatory conformance, conservative uncertainty, quarantine, provider reconciliation. |
| Flow becomes authority bypass | Unapproved effects | Static upper bounds plus runtime exact authorization and plan-hash binding. |
| Ingress receipt mistaken for truth | Bad downstream decisions | Assurance semantics and explicit epistemic boundary in API/UI. |
| Portable identity dominates core | Lock-in and false sovereignty claims | Protocol-neutral SPI, two adapters, optional dependencies. |
| Platform features bloat embedded profile | Adoption and security cost | Product/profile crate boundaries and shared conformance. |
| Managed worker becomes delegation engine | Domain leakage | Lifecycle controls only; work-order/acceptance stay downstream. |
| Sandbox freedom hides consequential effects | Lost accountability | Hybrid default for sensitive actions, perimeter receipts, export inspection. |
| Fresh-store rejection is inconvenient | Temptation to add unsafe legacy reader | Archive/export tools outside active runtime; no silent promotion. |
| Parallel lanes encode speculative AQ API | Reverse dependency pressure | Internal adapter traits and explicit reconciliation gates. |
| Too many crates slow development | Build/churn overhead | Logical modules first; split only under Section 5.10 criteria. |
| Security work deferred to Platform | Embedded misuse becomes unsafe | Canonical resources and final authorization exist in Fabric core; Platform adds multi-principal controls. |
| Experimental labels bypass ordinary policy | Unauthorized or incomparable effects | Conformance compares experimental and ordinary submissions; no label changes resolution, approval, information-use, or authorization. |
| Execution drift is hidden | Invalid causal inference | Pin or qualify resource, resolver, policy, connector, environment, artifact, timing, resource, and uncertainty conditions. |
| Receipt mistaken for task quality | False improvement acceptance | Preserve boundary facts only; Exoskeleton owns verification, evaluation, diagnosis, saturation, and acceptance. |
| Developmental evidence leaks protected workloads | Privacy and contractual harm | Governed artifact refs, receipt profiles, redaction, information-use policy, audience controls, and retention tests. |

---

## 16. Definition of implementation complete

### 16.1 Protocol and domain

- [ ] Foundational protocol types are not flow-centric.
- [ ] Caller submissions are distinct from authoritative agency, intent, authorization, observation, and receipt records.
- [ ] All IDs, digests, and canonical records have versioned fixtures.
- [ ] Portable identity remains optional and protocol-neutral.

### 16.2 Persistence and recovery

- [ ] Fresh target stores initialize, reopen, back up, restore, and reject pre-contract/unknown data without modification.
- [ ] Every cross-store handoff uses source record + outbox + idempotent admission.
- [ ] Ingress observation/receipt/delivery is atomic before wake.
- [ ] Effect intent exists before dispatch.
- [ ] Output publication precedes visible completion.
- [ ] Recovery reconciles WorldInterface domain state with ActionQueue state.

### 16.3 Authorization and resources

- [ ] Operational agency is server-resolved.
- [ ] Exact canonical resource/audience resolution precedes final authorization.
- [ ] Authority only attenuates.
- [ ] Unknown consequential effects fail closed.
- [ ] Self-generated purpose receives no special authority.

### 16.4 Effects, receipts, and reconciliation

- [ ] Every connector declares and proves idempotency/reconciliation semantics.
- [ ] Ambiguous commit produces `Uncertain`, never blind retry.
- [ ] Boundary receipts, workflow traces, evidence, and telemetry remain distinct.
- [ ] Provider acceptance/delivery/acknowledgment are distinct when available.
- [ ] Signatures and proofs state exactly what they establish.

### 16.5 Ingress and communication

- [ ] Ingress bindings are versioned and authorized.
- [ ] Provider events are authenticated, deduplicated, stored, receipted, and delivered idempotently.
- [ ] WorldInterface does not decide conversation, attention, truth, or response obligation.
- [ ] Generated content is never represented as sent/delivered without provider evidence.

### 16.6 Flow and execution

- [ ] Direct and Flow paths compile to one `ExecutionPlan`.
- [ ] Pure transforms cannot perform hidden I/O.
- [ ] Flow cannot enlarge authority.
- [ ] Waits/timers use `AQ-CONT-1`; no blocked executor or process-local signal registry remains.
- [ ] Workflow run is not the sole accountable principal.

### 16.7 Workers and connector catalog

- [ ] Managed-worker controls and three isolation profiles pass conformance.
- [ ] Specialist coding technique is absent from Fabric core.
- [ ] Target connector catalog is fully classified and admitted.
- [ ] Sandbox export and sensitive boundary crossings are governed and receipted.

### 16.8 Deployment and release

- [ ] Embedded and daemon profiles pass the same conformance package.
- [ ] Platform profile passes cross-tenant and secret isolation.
- [ ] Exact `AQ-CONT-1` release/conformance revision is pinned.
- [ ] EXO-V3 handoff fixtures are published.
- [ ] No legacy readers, adapters, aliases, dual paths, or old connector names remain.

---

### 16.9 Foundational completeness

- acting capacity and role/stewardship evidence are explicit;
- information-use enforcement is separate from access;
- commissioning requires preexisting owning-domain genesis;
- candidate, evidence, acceptance, and active runtime remain separate;
- lifecycle activation/rollback use independent authority and reconciliation;
- restore/migration/succession/fork remain distinguishable;
- temporal and schema provenance are replayable;
- the additions introduce no Exoskeleton domain dependency or new AQ semantic primitive.

### 16.10 Developmental experiment evidence

- [ ] bounded campaign and arm causal references survive submission, execution, receipt, uncertainty, reconciliation, crash, replay, backup, and restore;
- [ ] experimental labels cannot alter ordinary authority, priority, connector eligibility, approval, or information-use decisions;
- [ ] exact material boundary conditions are reconstructible or explicitly recorded as evidence gaps/confounds;
- [ ] protected task fixtures and customer artifacts remain governed and audience-constrained;
- [ ] WorldInterface exposes no task-quality, winning-arm, binding-constraint, saturation, acceptance, or continuity primitive;
- [ ] the 20-case developmental experiment acceptance matrix passes in embedded and daemon profiles.

## 17. Immediate issue set for WI-01

1. Tag `worldinterface/pre-wi-fabric-2` at `f48df82e98203fa94c86eecae60425205c3e00e9`.
2. Create/protect `wi-fabric-2` and document branch/review policy.
3. Add canonical stack and WI contract files.
4. Create `conformance/wi-fabric-2` package skeleton and manifest schema.
5. Generate current crate/module/type/connector inventory.
6. Assign target disposition to every current connector and crate.
7. Archive representative FlowSpec, receipt, ContextStore, webhook, streaming, WASM, and coding benchmark fixtures.
8. Record reproducible baseline build/test/benchmark commands and results.
9. Create `docs/adr` queue with `WI-ADR-001` through `WI-ADR-020`.
10. Add clean-break store-manifest and rejection-test scaffold.
11. Add staged forbidden legacy symbol manifest.
12. Add Exoskeleton domain-leakage and mandatory-identity-protocol dependency checks.
13. Add AQ assumption ledger and upstream requirement issue template.
14. Add maturity labels to GitHub issue/PR templates.
15. Open WI-02 protocol/store work as independently reviewable issues.
16. Open early AQ feedback issues only for requirements already demonstrated by the WorldInterface lifecycles.

---

## 18. Upstream ActionQueue feedback package

WorldInterface should continuously publish a small machine-readable consumer package to the accepted ActionQueue source/build binding containing:

```text
required aq-cont-1 conformance revision
assumption ledger status
blocked WI PRs
canonical admission examples
wait/signal ordering fixtures
cancellation/deadline race fixtures
compound child/wait fixture
resume-context fixture
opaque causal-reference fixture
```

### 18.1 Source-resolved core mappings; consumer verification at WI-AQ-G1

TaskPayload is bounded opaque bytes, while the actual signal/checkpoint/output APIs use DataRef. Large task input uses an application-owned locator envelope. EnsureTaskOutcome returns TaskId, not RunIds; inspect runs separately. Use bounded causal fields and same-store typed ancestry; foreign queue provenance is an external_ref. Stores bind manifest 1, WAL frame 1 and snapshot/projection 9, with complete retained WAL and fresh-lineage rejection. See the source lock and AB-WI-001, 015, 016 and 019.

### 18.2 Source-resolved admission; consumer verification at WI-AQ-G2

Persist the entire stable EnsureTaskRequest, including TaskId and AdmissionKey, before first submission. The public request computes CanonicalAdmissionV2. Same key/changed canonical content conflicts; Created and AlreadyExists return sequence, TaskId, key and digest only. One admission atomically covers a bounded task/run/dependency/parentage shape, not an unbounded Flow graph. Unknown acknowledgment requires recovery and identical-request retry. See AB-WI-002 through 004 and 005.

### 18.3 Source-resolved continuation; consumer verification at WI-AQ-G3

There is one active wait per run: one exact filter or a direct-children target, not multiple OR filters or principal matching. AnyRetained requires correlation; After uses SignalSequence, not WAL sequence. Commit event plus wake-outbox obligation regardless of Awaiting state. Advance the processed cursor or correlation generation when re-arming to avoid repeatedly matching the same retained event. First committed eligible resolution wins; resume context may redeliver on Retry/Recovery and is not always signal-shaped. See AB-WI-006 through 011, 021 and 022.

### 18.4 Source-resolved disposition and hosting; consumer verification at WI-AQ-G4

Use the validated disposition algebra: Complete cannot contain children/checkpoint/wait; Awaiting requires a wait and no output; Suspended has a narrower effect set. Validate bounded child/wait references before commit. Test local invalid/wait-capacity terminal fallback versus remote fence-preserving rejection, without leaking subordinate effects. Host authority is separate from opaque attribution, and embedded handlers require explicit dispatch. A handler must reload and deduplicate terminal domain outcomes. See AB-WI-012 through 014, 017, 018, 020 and 023.

These choices are resolved against the supplied source; actual WI fixtures and gate attestations are still required. The 24-case baseline matrix supplements the original tests. Reviewers must bind the exact implementation/build and conformance revision 16 rather than treating this prose or static validation as runtime evidence.

---

## 19. Downstream handoff package for EXO-V3

WorldInterface owns a consumer package that lets Exoskeleton integrate without depending on internal runtime crates.

### 19.1 Protocol clients and fixtures

Publish:

- embedded and remote client interfaces;
- protocol schemas and canonical test vectors;
- ingress binding/observation/delivery/ack fixtures;
- effect submission/intent/authorization/outcome/receipt fixtures;
- communication provider milestone and uncertainty fixtures;
- worker-control/sandbox/artifact fixtures;
- identity binding and proof fixtures;
- error taxonomy and retry guidance.

### 19.2 Causal-reference conventions

The package documents how Exoskeleton may provide opaque purpose and origin references, for example:

```text
ref://exoskeleton/commitment/C
ref://exoskeleton/communication-intent/M
ref://exoskeleton/work-order/W
ref://exoskeleton/initiative-decision/I
ref://exoskeleton/developmental-campaign/DC
ref://exoskeleton/campaign-execution-context/CEC
```

WorldInterface stores and propagates them but does not resolve their meaning.

### 19.3 Ingress acknowledgment contract

The fixture proves:

```text
WorldInterface observation + receipt + delivery outbox durable
    ↓
Exoskeleton stores CommunicationEvent/AttentionDemand or other domain record
    ↓
Exoskeleton acknowledges DeliveryId
    ↓
WorldInterface closes delivery
```

### 19.4 Effect contract

The package specifies:

- stable `ClientRequestId` mapping from an Exoskeleton intent;
- duplicate/conflict behavior;
- server-resolved agency and exact authorization;
- outcome polling/subscription;
- uncertainty and reconciliation;
- receipt/evidence semantics;
- cancellation limitations after possible provider commit.

### 19.5 Worker-control contract

The package makes clear:

- Exoskeleton owns work order, supervision, verification, and acceptance;
- WorldInterface owns sandbox/worker boundary control, authority, artifacts, and receipts;
- ActionQueue owns durable execution/waiting;
- a worker event is evidence, not commitment completion.

### 19.6 Commissioning and lifecycle handoff

Provide EXO-V3 fixtures for:

- idempotent commissioning submissions referencing a durable Genesis;
- role and stewardship attestation resolution;
- acting-capacity authorization;
- information-use envelopes;
- candidate build/provenance/trial records;
- runtime activation, rollback, quarantine, restore, migration, succession, and fork operations;
- lifecycle-supervisor receipts and uncertainty.

WorldInterface fixtures establish boundary behavior only. They do not encode EXO-V3 evaluation, binding-constraint, saturation, acceptance, continuity, or narrative decisions.

### 19.7 Developmental experiment evidence handoff

Publish fixtures proving:

- bounded campaign and intervention-arm causal-reference propagation;
- ordinary authorization and information-use parity for experimental work;
- exact execution-envelope and environment evidence;
- explicit confound and evidence-gap visibility;
- uncertain-arm reconciliation and crash recovery;
- protected workload handling;
- absence of WorldInterface task-quality, winning-arm, binding-constraint, saturation, acceptance, and continuity semantics.

The downstream package includes `wi-fabric-2-developmental-experiment-evidence-profile.md` and `wi-fabric-2-developmental-experiment-acceptance-matrix.yaml`.

### 19.8 Downstream freeze rule

Exoskeleton may build pure domain work against fixture schemas before `WI-FABRIC-2` is released. Its real integration adapter freezes only against the published WorldInterface conformance revision. WorldInterface does not preserve an Exoskeleton adapter guess made before that gate.

---

## Appendix A — PR dependency and gate matrix

| PR | Depends on WI | AQ gate | Maturity at start | Primary release blocker |
|---|---|---|---|---|
| WI-01 | — | — | Independent | Evidence/contract completeness |
| WI-02 | WI-01 | G1 for AQ wrappers | Independent | Protocol/store correctness |
| WI-03 | WI-02 | G3 for wake adapter | Mixed | Store-before-wake ingress |
| WI-04 | WI-02 | G2 for dispatch | Mixed | Intent-before-dispatch and exact auth |
| WI-05 | WI-02 | G4 for final convergence | Mixed | Receipt/artifact consistency |
| WI-06 | WI-04, WI-05 | G4 | Contract-blocked | Connector ambiguity/reconciliation |
| WI-07 | WI-04–06 | G4 | Mixed | Common plan and Flow authority |
| WI-08 | WI-07 | G3/G4 | Contract-blocked | Delete invalid waits |
| WI-09 | WI-06, WI-08 | G3/G4 | Mixed | Worker/sandbox lifecycle |
| WI-10 | WI-06, WI-09 | — | Mixed | Catalog rationalization |
| WI-11 | WI-03–10 | G5 for release tests | Mixed | Authenticated parity/tenancy |
| WI-12 | WI-04, WI-05, WI-11 | G1 refs | Independent optional | Identity/proof neutrality |
| WI-13 | WI-01–12 | G5 | Integration-only | Complete conformance |
| WI-14 | WI-13 | Published AQ | Integration-only | Reproducible release/handoff |

## Appendix B — Suggested issue labels

```text
contract:wi-fabric-2
area:protocol
area:ingress
area:effects
area:authorization
area:resources
area:receipts
area:flow
area:connectors
area:wasm
area:workers
area:platform
area:identity-proof
area:persistence
area:conformance
maturity:independent
maturity:assumption-bound
maturity:contract-blocked
maturity:integration-only
upstream:aq-g1
upstream:aq-g2
upstream:aq-g3
upstream:aq-g4
upstream:aq-g5
risk:security
risk:durability
risk:breaking
clean-break:deletion
```

## Appendix C — Forbidden legacy inventory at release

The following must be absent from target runtime source, registry, public API, and release binaries except archival documentation/tests that are excluded from production targets:

```text
SignalRegistry
signal.await
signal.emit
signal_await
signal_emit
blocking delay connector
legacy receipt writer/reader
anonymous compatibility principal
old connector-name aliases
current FlowSpec compatibility parser
invoke_single as bypass
fire-and-forget AQ submission fallback
coding-specific Fabric connectors (`code_apply_patch`, `code_edit`, `code_write`, and the complete archived catalog)
live old/new authorization or receipt path
pre-WI-FABRIC-2 store reader
```

## Appendix D — Planning assumptions that require evidence before change

1. SQLite remains an acceptable embedded durable store for target-scale development and crash testing.
2. ActionQueue remains the sole durable executor and supplies `AQ-CONT-1` semantics.
3. Direct and deterministic Flow operations can share one execution-plan representation.
4. Exact resource resolution can be separated from caller selectors through a resolver registry.
5. Provider ambiguity can be represented conservatively even when some connectors cannot reconcile automatically.
6. Specialist agents remain effective with sandbox-local technique and mediated consequential boundaries.
7. Embedded and daemon profiles can share protocol semantics without sharing all control-plane dependencies.
8. Portable identity can remain optional through SPI and reference types.
9. Fresh target persistence is cheaper and safer than carrying experimental formats.
10. Exoskeleton can integrate through protocol fixtures without WorldInterface learning Vessel semantics.

Change any of these only with an ADR containing implementation evidence, failed conformance case, or measured operational constraint.

## Appendix E — Current-to-target file-family map

| Current source family | Target family |
|---|---|
| `worldinterface-core/src/{id,receipt,descriptor,streaming,...}` | `protocol`, `capability`, `flow-model`, optional identity/proof envelopes |
| `worldinterface-flowspec/src/*` | `flow-model`, `flow-compiler` |
| `worldinterface-contextstore/src/*` | fresh target ContextStore/artifact publication |
| `worldinterface-connector/src/traits.rs` | capability connector SPI |
| `worldinterface-connector/src/context.rs` | runtime execution context with verified agency/intent/auth/resource |
| `worldinterface-connector/src/receipt_gen.rs` | runtime receipt/evidence service after authoritative records |
| `worldinterface-connector/src/registry.rs` | capability registry + descriptor snapshot/quarantine |
| `worldinterface-connector/src/signal_registry.rs` | deleted |
| `worldinterface-connector/src/connectors/code_*` and repo helpers | sandbox/specialist/verifier or deleted |
| `worldinterface-coordinator/src/*` | `flow-coordinator` over AQ continuations and transactional children |
| `worldinterface-host/src/*` | `embedded` composition/runtime host |
| `worldinterface-http-trigger/src/*` | `ingress/http` binding/provider/delivery path |
| `worldinterface-daemon/src/*` | authenticated daemon and optional Platform composition |
| `worldinterface-cli/src/*` | target CLI client/admin surfaces |
| `worldinterface-wasm/src/*` | target connector/transform/verifier adapters and policy |

## Appendix F — Planning artifacts

This plan is accompanied by:

```text
wi-fabric-2-work-breakdown.yaml
wi-fabric-2-aq-assumption-ledger.yaml
wi-fabric-2-planning-validation.json
wi-fabric-2-planning-package-README.md
```

The YAML files are planning aids. The Markdown architecture, canonical stack contract, repository ADRs, and executable conformance packages remain normative in that order.


## Appendix G — Embedded worker-provider staging and downstream handoff

`WI-WORKER-EXECUTION-1` is the authoritative provider definition. Read `wi-fabric-2-worker-execution-profile.md`, its YAML definition and acceptance matrix, and `WORKER-CONCURRENCY-REBINDING.md`. WI-ADR-016 resolves field mapping without changing frozen wire contracts by implication. The new worker-boundary assumption ledger belongs to WI; the AQ ledger is unchanged.

The proof path is: reviewed provider definition/admission/evidence → real embedded lifecycle using AQ G1–G4 → artifacts/composition/handoff → `WI-EMBEDDED-WORKER-PROOF-1` → EXO consumption and independent acceptance. There is no EXO completion prerequisite on producer proof. G1–G4 are provider checkpoints (`WI-WORKER-*`), not replacements for `WI-AQ-*` or `EXO-WI-*`. The separate G5 host-supervisor boundary is consumed by EXO lifecycle qualification and is not a dependency of the basic process proof.

Evidence owners can produce explicit embedded slices in WI-09/10/11/13/14 without accepting unfinished broader parents. In particular, no platform/remote completion or published WI-14 artifact is required merely to record the WI-owned partial proof. Full dependency edges, full parent exits, release RC/conformance requirements and publication order remain in force for their respective claims.

The initial reference provider is not a permanent locality assumption: opaque provider/run references, asynchronous controls, artifacts, qualified operation support and non-amplifying authority remain compatible with later externally provided resources. No future external worker backend, platform service, runtime engine or DigiCorp authority is implemented by this revision.

## Implemented ActionQueue source alignment (2026-09-15)

The normative [AQ baseline amendment](AQ-BASELINE-ALIGNMENT.md) resolves all AQ-facing interface sketches in this plan. The exact [source lock](aq-baseline.lock.json) and [24 additional consumer cases](aq-baseline-integration-acceptance-matrix.yaml) are part of every affected work-item exit. Earlier AQ-NN producer references are retained capability/evidence mappings, not pending AQ development tasks. Read the amendment before implementing any AQ adapter. Candidate-branch evidence can satisfy premerge contract review; no circular requirement to verify an already-gated merge is introduced.
