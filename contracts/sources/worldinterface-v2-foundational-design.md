# WorldInterface v2: Accountable Boundary Fabric, Portable Agency, Lifecycle Support, and Deterministic Flow

> **2026-09-15 AQ source alignment:** AQ-facing pseudocode is governed by [AQ-BASELINE-ALIGNMENT.md](AQ-BASELINE-ALIGNMENT.md) and the exact source lock. Constitutional and domain semantics remain unchanged.

**Status:** Implementation-ready foundational-completeness edition with an additive developmental-experiment evidence profile; portable identity remains pluggable and target-only conformance is normative  
**Target repository:** `zed-colonel/worldinterface`  
**Baseline reviewed:** public `main` at commit [`f48df82e98203fa94c86eecae60425205c3e00e9`](https://github.com/zed-colonel/worldinterface/commit/f48df82e98203fa94c86eecae60425205c3e00e9), June 9, 2026 (“New coding capabilities”)  
**Design-set contract:** `WI-FABRIC-2`  
**Upstream dependency:** `ActionQueue Hardening: Durable Continuations, Idempotent Admission, and Causal Execution` (`AQ-CONT-1`)  
**Downstream companion:** `Exoskeleton v3: Constitutional Vessel, Governed Initiative, Communication, Narrative Identity, Developmental Integrity, and Delegated Agency` (`EXO-V3`)  
**Date:** August 10, 2026  
**Scope:** WorldInterface protocol, agency and authorization, portable external identity and role-attestation bindings, acting capacity, information-use constraints, signed statements and proof bundles, accountable ingress observations, effect and communication-delivery execution, commissioning and lifecycle effects, component provenance, receipts, reconciliation, controlled developmental-experiment evidence, FlowSpec orchestration, isolation runtimes, embedded and centralized deployment  
**Working thesis:** *WorldInterface mediates accountable crossings in both directions, verifies operational agency and acting capacity, enforces purpose- and information-use-aware boundary constraints, and produces evidence without pretending that an identifier, signature, receipt, build, trial, campaign correlation, or execution outcome establishes semantic truth, task quality, a binding constraint, or acceptance. WorldInterface Flow composes authorized crossings deterministically.*

**Review posture:** This document is grounded in a static review of the pinned public repository and the architecture discussion that produced Exoskeleton v3. The developmental-experiment clarification is informed by a colleague-provided intelligence/ontology/harness moving-frontier diagram, but it uses that idea only to improve evidence quality for Exoskeleton-owned developmental campaigns. It adds no WorldInterface performance ontology, evaluator, authority path, or protocol primitive. The current repository is a frozen experimental baseline, not a compatibility target. Proposed APIs are illustrative until compiled and proven by target-only conformance and crash suites.

> **WI-PLAN-5 implementation profile:** `WI-WORKER-EXECUTION-1` selects bounded process-managed proof for the first embedded Agent Host. See [the provider supplement](wi-fabric-2-worker-execution-profile.md). It does not equate the Vessel's outer Kata VM with worker isolation or remove the broader target conformance obligations.

---

## 0. Coordinated design-set contract

**Canonical contract revision:** `STACK-2026-07-25-FOUNDATIONAL-2`  
**Canonical machine-readable source:** `constitutional-stack-foundational-contracts.yaml`  
**Additive planning/conformance profile:** `STACK-DEVELOPMENTAL-DIAGNOSTICS-1` in `developmental-diagnostics-cross-stack-profile.yaml`

The labels below name architectural behavior contracts, not crate semantic versions.

| Contract | Repository | Normative responsibility | Depends on |
|---|---|---|---|
| `AQ-CONT-1` | ActionQueue | Durable opaque execution, compound idempotent admission, run/attempt lifecycle, `Awaiting`, durable signals, checkpoints, opaque causal-reference propagation, and recovery. | Nothing in the design set. |
| `WI-FABRIC-2` | WorldInterface | Verified operational agency, portable-identity and role-attestation bindings, explicit acting capacity, canonical resource and audience resolution, purpose- and information-use-aware final authorization, accountable observations and effects, lifecycle operations, component provenance, proof bundles, delivery, reconciliation, receipts, and deterministic boundary workflows. | `AQ-CONT-1`. |
| `EXO-V3` | Exoskeleton | Vessel blueprint and genesis, identity and lifecycle, Purpose Charter, roles and acting capacity, governed mission and commitment admission, initiative, interaction, communication, cognitive faculties, constitutions, epistemic state and semantic time, relationships, memory, narrative continuity, information-use obligations, viability, external personas, delegation, verification, developmental integrity, governed self-improvement, and acceptance. | `WI-FABRIC-2` and `AQ-CONT-1`. |

```text
AQ-CONT-1 → WI-FABRIC-2 → EXO-V3
```

A portable identity foundation such as EntityID, KERI, a W3C controlled-identifier profile, certificates, or a future equivalent remains an **optional adjacent foundation**. It integrates through WorldInterface identity and proof interfaces; it is not a mandatory fourth runtime layer and does not replace internal Vessel identity.

```text
Portable identity foundation
  control lineage · signatures · attestations · optional transparency
                         │
                         ▼
AQ-CONT-1 ─────────→ WI-FABRIC-2 ─────────→ EXO-V3
```

### 0.1 Clean-break implementation posture

The pinned repositories are pre-production experimental baselines. They are retained as source history, characterization fixtures, and evidence of failure modes—not as runtime compatibility obligations.

The target implementation follows six shared rules:

1. **Pre-contract implementations are evidence, not dependencies.**
2. **One authoritative path exists at every boundary.** Offline fixture comparison is allowed; live dual authority is not.
3. **Pre-contract state is never silently promoted into target commitments, identity, relationships, authorization, receipts, beliefs, narrative, or queue history.**
4. **Target conformance outranks source compatibility.** Intentional guarantees are re-proved under the target contracts.
5. **Future stability begins with the target release.** Each target establishes explicit versioning, backup, restore, and supported migration policy for releases that follow it.
6. **Repository tags and archives are the legacy archive.** The active runtime does not carry obsolete readers, adapters, aliases, fallback paths, or downgrade support merely because the old code exists.

### 0.2 Shared handoff rule

Every cross-store or cross-process handoff follows one pattern:

```text
source authoritative domain record + source outbox
    → idempotent downstream admission
    → downstream authoritative record + downstream signal outbox
    → opaque ActionQueue wake-up or task reference
```

An ActionQueue signal wakes work. It is never the authoritative message, approval, worker result, identity event, receipt, effect outcome, belief revision, lifecycle event, or narrative fact.

Three specialized handoffs refine that rule without changing it:

```text
Vessel commissioning
    Exoskeleton GenesisRecord + commissioning outbox
        → WorldInterface bounded commissioning effects and receipts
        → Exoskeleton commissioning evidence and ActivationDecision

Governed improvement
    Exoskeleton CapabilityFrontierAssessment + ImprovementProposal
    + frozen EvaluationPlan + exact ComponentFingerprint
        → ordinary WorldInterface candidate-development / build / trial / activation effects
          with bounded campaign and intervention-arm causal references
        → receipts, environment/provenance evidence, uncertainty, and reconciliation
        → external lifecycle supervisor transition
        → Exoskeleton evaluation, ContinuityAssessment, saturation/stop decision,
          and developmental history

Information use
    Exoskeleton social and epistemic use obligations
        → WorldInterface boundary enforcement for disclosure, export,
          audience, retention-trigger, and proof-publication constraints
```

### 0.3 Ownership and identity boundaries

| Identity or record | Owner | Meaning |
|---|---|---|
| `AdmissionKey`, `TaskId`, `RunId`, `AttemptId`, `WaitId`, `SignalId` | ActionQueue | Durable admission and opaque execution lifecycle. |
| `IngressBindingId`, `IngressObservationId`, `DeliveryId`, `EffectIntentId`, `AuthorizationId`, `ReceiptId` | WorldInterface | Accountable boundary admission, executable authority, effect/observation/lifecycle operation, and evidence. |
| `VesselId`, `GenesisRecordId`, `RoleAssignmentId`, `CommitmentId`, `DecisionId`, `WorkOrderId`, `CommunicationIntentId`, `CognitiveEpisodeId`, `BeliefRevisionId`, `NarrativeVersionId`, `ImprovementProposalId`, `TaskDistributionId`, `CapabilityProfileId`, `RepresentationProfileId`, `ComponentFingerprintId`, `CapabilityFrontierAssessmentId`, `DevelopmentalCampaignId` | Exoskeleton | Enduring identity, origin, purpose, role, local constitutional decisions, interaction, cognition, epistemic state, delegation, development, and interpretation. |
| provider idempotency key | WorldInterface connector protocol | External replay protection. It is never an `AttemptId`. |
| external identifier and key-state reference | External foundation / WorldInterface binding | Portable control lineage and provenance evidence; never unique personhood, semantic truth, or automatic authority. |
| candidate build or deployment identity | WorldInterface / lifecycle supervisor boundary | A versioned artifact and transition target; never active Vessel identity merely because it exists. |

Illustrative opaque references use one non-normative syntax throughout the set:

```text
ref://<system>/<kind>/<id>
ref://exoskeleton/commitment/C
ref://worldinterface/effect-intent/E
ref://actionqueue/run/R
```

Consumers treat these as bounded opaque references unless their own domain explicitly owns the referenced namespace.

### 0.4 Cross-stack invariant matrix

| Shared rule | ActionQueue | WorldInterface | Exoskeleton | Required cross-layer proof |
|---|---|---|---|---|
| Store authoritative state before wake | `AQ-H3`, `AQ-H4`, `AQ-H5` | `W26` | `C36` | Crash between domain commit and signal delivery loses neither fact nor wake-up. |
| Attribution is not authorization | `AQ-H10`, `AQ-H11` | `W4`, `W5` | `C13`, `C14` | Forged caller metadata cannot create executable authority. |
| Routing or execution ability is not resource authority | `AQ-H12` | `W15` | `C4`, `C5` | Executor or connector eligibility cannot widen a grant. |
| Unknown consequential effects fail closed | — | `W12` | `C6` | Unclassified consequential work cannot dispatch. |
| Intent is durable before consequential dispatch | — | `W3` | `C16` | A crash before dispatch leaves an inspectable intent, not an unaccounted effect. |
| Ambiguity is first-class and never blind retry | `AQ-H13`, `AQ-H14` | `W13` | `C17` | Queue waiting, effect uncertainty, and Vessel acceptance remain separate states. |
| Generated content is not delivery | — | `W29` | `C31` | Model output cannot create provider acceptance or delivery evidence. |
| Endogenous or self-improvement origin grants no authority | `AQ-H11` | `W41`, `W54` | `C43`, `C73` | A self-generated goal or candidate uses the ordinary authorization path. |
| Genesis precedes external commissioning | — | `W44` | `C55`, `C58` | External setup cannot create or rewrite internal Vessel origin. |
| Acting capacity is explicit | — | `W45` | `C60`, `C67` | Multiple roles cannot ambiently union authority. |
| Access permission is not information-use permission | — | `W46` | `C68` | Read access cannot silently imply retention, derivation, disclosure, or reuse. |
| Candidate is not active runtime | — | `W48`, `W49` | `C71`, `C78` | Candidate production cannot activate or overwrite the running Vessel. |
| Evidence and evaluation are not acceptance | — | `W50` | `C72`, `C74` | A candidate or provider cannot certify its own acceptance. |
| Schema meaning is versioned | — | `W51` | `C69` | Policy replay pins exact semantic schema versions. |
| Restore is not fork | — | `W52` | `C63` | Migration, restore, succession, and branch creation remain distinguishable. |
| Temporal provenance is not semantic truth | — | `W55` | `C65` | Occurrence, observation, receipt, admission, and validity times remain distinct. |
| Developmental correlation is attribution, not authority or priority | `AQ-H19` | `W56` | `C96`, `C104` | Campaign and intervention-arm references cannot widen authority, route work, set priority, or create acceptance. |
| Boundary and execution evidence are not performance judgment | `AQ-H20` | `W57` | `C100`, `C101` | Queue completion and boundary receipts remain evidence inputs to Exoskeleton evaluation. |
| Experimental work follows ordinary policy | — | `W58` | `C100`, `C101` | A research, trial, benchmark, or self-improvement label cannot bypass resolution, information-use, approval, authorization, or reconciliation. |
| Comparative evidence pins its execution envelope | — | `W59` | `C96`, `C100` | Material resource, policy, connector, environment, artifact, timing, and uncertainty differences remain visible as intended changes or confounds. |
| Clean replacement has one authoritative path | `CLEAN-2` | `CLEAN-2` | `CLEAN-2` | No runtime old/new dual writes, dual reducers, or fallback authority. |

### 0.5 Conformance packages and release order

Each repository owns a target-only conformance package under its own source tree:

```text
actionqueue/conformance/aq-cont-1
worldinterface/conformance/wi-fabric-2
exoskeleton/conformance/exo-v3
```

Each package carries:

- contract label and integer contract revision;
- fixture schema version;
- executable black-box tests and crash scenarios;
- a manifest of required upstream conformance revisions;
- immutable fixture hashes.

A fixture change that only adds coverage increments the package revision. A change that alters a normative behavior contract requires an explicit contract amendment. Downstream repositories pin exact upstream conformance revisions in development and release manifests.

Development may use path or pinned Git dependencies. Published breaking releases occur only in this order:

```text
1. AQ-CONT-1 ActionQueue release
2. WI-FABRIC-2 WorldInterface release built against that ActionQueue contract
3. EXO-V3 Exoskeleton release built against both published upstream contracts
```

No compatibility release is published merely to keep the experimental stack compiling.

## Document map

0. [Coordinated design-set contract](#0-coordinated-design-set-contract)
1. [Executive decision](#1-executive-decision)
2. [Baseline and diagnosis](#2-baseline-and-diagnosis)
3. [Goals and non-goals](#3-goals-and-non-goals)
4. [Constitutional invariants](#4-constitutional-invariants)
5. [Product and deployment decomposition](#5-product-and-deployment-decomposition)
6. [Core protocol and domain model](#6-core-protocol-and-domain-model)
7. [Direct effects and accountable ingress](#7-direct-effects-and-accountable-ingress)
8. [Deterministic Flow](#8-deterministic-flow)
9. [Capability and connector model](#9-capability-and-connector-model)
10. [Isolation, WASM, and managed workers](#10-isolation-wasm-and-managed-workers)
11. [Resource resolution and authorization](#11-resource-resolution-and-authorization)
12. [Boundary lifecycles, idempotency, and reconciliation](#12-boundary-lifecycles-idempotency-and-reconciliation)
13. [Receipts, traces, evidence, and telemetry](#13-receipts-traces-evidence-and-telemetry)
14. [Agency and accountability](#14-agency-and-accountability)
15. [Contract with Exoskeleton](#15-contract-with-exoskeleton)
16. [Platform and NetCorp](#16-platform-and-netcorp)
17. [Crate and dependency refactor](#17-crate-and-dependency-refactor)
18. [API, CLI, and operator surfaces](#18-api-cli-and-operator-surfaces)
19. [Clean-break implementation and connector disposition](#19-clean-break-implementation-and-connector-disposition)
20. [Persistence, versioning, and recovery](#20-persistence-versioning-and-recovery)
21. [Security model](#21-security-model)
22. [Verification plan](#22-verification-plan)
23. [Observability and performance](#23-observability-and-performance)
24. [Risks and architectural decisions](#24-risks-and-architectural-decisions)
25. [Definition of done](#25-definition-of-done)
26. [Implementation sequence](#26-implementation-sequence)
27. [Protocol sketch and worked examples](#27-protocol-sketch-and-worked-examples)
28. [Foundational lifecycle and developmental seams](#28-foundational-lifecycle-and-developmental-seams)

---

## 1. Executive decision

WorldInterface should **not** be forked into an “Exoskeleton variant” and a “workflow-engine variant.”

It should be refactored into one shared accountable-boundary kernel with distinct product surfaces and deployment profiles:

### WorldInterface Fabric

The shared protocol and runtime for observing and exercising bounded capabilities across resources outside a principal's trusted internal state.

It owns:

- generalized principal and actor identity;
- agency attribution and delegation lineage;
- capability descriptors and resource scopes;
- authorization envelopes and enforcement;
- durable ingress observations and effect intent;
- connector ingress and execution;
- idempotency classification;
- ingress and egress receipts, delivery state, and effect uncertainty;
- reconciliation;
- embedded host and remote protocol conformance.

### WorldInterface Flow

An optional deterministic orchestration frontend over Fabric.

It owns:

- FlowSpec authoring and versioning;
- parsing and static validation;
- branches and pure transforms;
- compilation to a common execution plan;
- deterministic node scheduling through ActionQueue;
- workflow traces;
- flow-specific operator and API surfaces.

It does not own the principal's goals, open-ended planning, or constitutional authority.

### WorldInterface Platform

An optional centralized, authenticated, multi-principal deployment of Fabric, usually with Flow.

It owns:

- authentication and principal registration;
- organizations and tenancy;
- policy administration;
- shared connectors and secret brokerage;
- quotas, rate limits, and scheduling policy;
- audit access control;
- high availability and platform operations.

Exoskeleton should consume Fabric. NetCorp will likely operate Platform plus Flow.

The product relationship is:

```text
WorldInterface Protocol
      identity · agency · authority · observation · intent · receipt
                             │
                             ▼
                WorldInterface Fabric Runtime
     resource resolution · enforcement · execution · reconciliation
                             │
            ┌────────────────┴─────────────────┐
            │                                  │
            ▼                                  ▼
  Ingress Observations / Direct Effects      WorldInterface Flow
  BoundaryObservation / EffectIntent          FlowSpec → ExecutionPlan
            │                                  │
            └────────────────┬─────────────────┘
                             ▼
                         ActionQueue
              scheduling · leases · WAL · recovery
```

There are two independent axes:

| | Direct accountable boundary crossing | Deterministic boundary workflow |
|---|---|---|
| Embedded | local ingress or Vessel/application invocation | local automation flow |
| Centralized | shared organizational ingress/effect broker | NetCorp/platform workflow service |

All four quadrants should use the same agency, boundary-observation, effect, authorization, receipt, delivery, and reconciliation semantics.

### 1.1 The scope correction

WorldInterface is not the Vessel's universal syscall layer. It does not need to mediate every file read, grep, compiler invocation, or temporary write performed by a specialist inside a contained environment.

Its purpose is narrower and more valuable:

> **WorldInterface records and governs crossings in either direction whose authority, provenance, consequence, ambiguity, sensitivity, or evidentiary value matters to an accountable principal.**

A low-level action belongs at the WorldInterface boundary when at least one of the following is true:

- it crosses a trust zone;
- it accesses a sensitive resource;
- it changes durable or shared state;
- it may have an ambiguous outcome after failure;
- independent proof is needed;
- semantic integration materially improves governance;
- it issues or changes a bounded authority envelope.

Operations that satisfy none of these conditions normally belong inside a sandbox, specialist runtime, library, or operational telemetry stream.

### 1.2 The workflow correction

WorldInterface can remain a workflow engine because its flows compose bounded effects and pure transforms. The workflow product should be described precisely as a **durable boundary-workflow engine**: ingress may trigger a run, while boundary nodes compose authorized effects and pure transforms. It is not a general cognitive planner.

A Flow may deterministically arrange already-authorized actions. It may never enlarge authority, infer strategic purpose from prose, or decide whether a principal's long-term commitment should exist.

### 1.3 The delegation correction

A long-lived delegated agent should not be modeled as one enormous connector invocation.

WorldInterface may mediate worker-control operations:

```text
worker.start
worker.command
worker.inspect
worker.checkpoint
worker.suspend
worker.resume
worker.terminate
worker.collect_artifacts
```

The worker's objective, supervision, progress interpretation, and acceptance belong to the delegating system—Exoskeleton in the companion design.

WorldInterface's WASM runtime remains strategically useful for narrow bounded capabilities, deterministic verification, remote adapters, and untrusted lightweight extensions. It is not the mandatory host for specialist agents whose native runtimes and long-lived lifecycles do not fit a connector-shaped `invoke()` contract.

### 1.4 The communication correction

Incoming communication is not an effect requested by a principal, but it is an accountable boundary crossing. WorldInterface must therefore support two distinct lifecycle families:

```text
Ingress observation
    provider/channel event → authenticated, deduplicated BoundaryObservation
    → ingress receipt → delivery to the owning application

Egress effect
    EffectSubmission → durable EffectIntent → resolution/authorization
    → connector execution → receipt, delivery evidence, or uncertainty
```

WorldInterface owns transport provenance, provider identifiers, channel/thread/reply metadata, identity assurance, exact outbound audience resolution, and delivery evidence. It does not own Exoskeleton conversations, attention, response obligations, relationship meaning, or epistemic acceptance.

A generated response is not “sent” until an egress communication effect has crossed the boundary and produced the appropriate provider evidence.

---


### 1.5 The portable-agency correction

Digital collaboration eventually requires principals whose agency can remain recognizable across applications and administrative domains. WorldInterface should therefore support externally controlled identifiers, time-versioned key state, signed requests, delegated credentials, contextual attestations, and optional transparency publication.

This support must remain precise:

- an identifier demonstrates a control lineage, not unique personhood;
- a signature demonstrates cryptographic provenance, not truth;
- an attestation records what an issuer claimed, not what WorldInterface must believe;
- a sponsor may pay or stake resources without becoming the actor;
- public registration increases accountability but does not prove completeness;
- key custody, rotation, recovery, revocation, and historical verification are mandatory for long-lived principals;
- EntityID-specific ledger, token, consensus, reputation, and bond mechanisms remain optional profiles, not Fabric prerequisites.

WorldInterface is the integration boundary because this is where identity becomes operational agency against a concrete resource. The portable identity protocol remains independently usable and separately governed.

### 1.6 The lifecycle and developmental correction

The accountable boundary must support more than ordinary tool calls and workflows. A persistent Vessel will eventually be instantiated, commissioned, assigned roles, migrated, improved, suspended, restored, and retired. Those processes cause external effects, but their meaning remains outside WorldInterface.

WorldInterface therefore adds protocol-neutral seams for:

- explicit acting capacity and externally verifiable role or stewardship evidence;
- information-use constraints distinct from access permission;
- bounded commissioning operations after internal Vessel genesis;
- candidate-build, provenance, trial, activation, rollback, and quarantine effects;
- lifecycle operations that preserve the difference among restore, migration, succession, and fork.

WorldInterface does not decide whether a Vessel is the same entity after a change, whether a mission should be accepted, whether a candidate is an improvement, or what a role means internally. It supplies exact authorization, durable intent, execution, receipts, and uncertainty for the boundary crossings those decisions require.

## 2. Baseline and diagnosis

### 2.1 Existing strengths

The current repository already contains much of the durable execution fabric worth retaining:

- FlowSpec parsing, validation, and compilation to ActionQueue DAGs;
- ActionQueue-backed scheduling and crash recovery;
- connector descriptors and a connector registry;
- pure transforms and deterministic branches;
- ContextStore output persistence;
- per-step receipts;
- embedded host and daemon surfaces;
- HTTP trigger support;
- a WASM connector runtime with explicit capability policy;
- direct `invoke_single` and submitted-flow paths;
- invariants around ActionQueue execution, output write-once behavior, and idempotent run identities.

The redesign should preserve these strengths while clarifying what the foundational layer means.

### 2.2 Current conceptual compression

Several distinct concerns are currently grouped into foundational types:

- FlowSpec models;
- flow and node identity;
- connector descriptors;
- invocation context;
- receipts;
- direct invocation;
- workflow orchestration.

The existing receipt is primarily flow-step shaped: flow run, node, step run, connector name, attempt, hashes, status, error, and duration. The existing invocation context similarly centers flow, node, step, run ID, and attempt.

That representation is useful for orchestration but incomplete for a generalized accountable boundary because it does not establish:

- the accountable principal;
- the immediate requester;
- delegation ancestry;
- purpose or commitment;
- the grant and approval that authorized the operation;
- the canonical external resource;
- effect class and idempotency semantics;
- executor identity;
- uncertainty and reconciliation state.

V2 should make those concerns foundational and make flow references optional causal context.

### 2.3 The coding-capability expansion as diagnostic evidence

The current latest commit adds thousands of lines of coding-specific connector functionality, including code search, symbol lookup, references, implementation lookup, tests, repository context, and semantic Rust analysis.

These capabilities are useful to a coding agent. Their appearance in the global boundary fabric shows how easily a specialist vertical can expand the core ontology when WorldInterface is treated as an agent tool catalog.

`code_grep` is the clearest example. Searching a repository inside an authorized worktree may be operationally important to the coding agent, but it rarely carries enough authority, consequence, or historical meaning to deserve a first-class immutable boundary receipt.

This does not make the implementation worthless. It establishes an architectural acceptance test:

> Can a coding agent remain effective after coding-specific technique moves into its sandbox or verifier, while WorldInterface preserves only accountable workspace grants, artifact boundaries, sensitive effects, and semantic publication operations?

### 2.4 Current security posture and platform implications

The current security posture is intentionally embedded and trusted-network oriented. The daemon does not provide the complete authentication, tenancy, resource isolation, and policy administration required for a centralized organizational service. Native filesystem and HTTP connectors also require stronger resource-resolution and egress policy before they can be safely exposed across multiple principals.

This does not require a different executor. It requires a platform control plane around the shared Fabric semantics.

### 2.5 What should carry forward

| Existing property | V2 decision |
|---|---|
| ActionQueue as sole durable executor | Preserve. |
| FlowSpec DAG validation and compilation | Preserve; compile into shared `ExecutionPlan`. |
| ContextStore write-once outputs | Preserve; clarify artifact and evidence roles. |
| Connector registry and descriptors | Preserve; expand descriptors into capability contracts. |
| Direct invocation | Preserve as direct EffectIntent frontend, not a bypass. |
| Embedded host | Preserve as Fabric Embedded profile. |
| Daemon | Evolve into authenticated Fabric/Flow service profile. |
| WASM runtime | Preserve for bounded extensions; decouple from delegated-agent hosting. |
| Receipts | Preserve immutability; redesign schema around direction-appropriate attribution, observation/effect semantics, and delivery evidence. |
| Coding connectors | Classify, move, generalize, or delete directly; do not preserve a compatibility surface. |

---

### 2.6 Accountable ingress is already latent in the repository

The current project already contains two ingress-shaped mechanisms:

- `worldinterface-http-trigger` accepts dynamic webhook ingress;
- streaming WASM connectors let the host maintain a persistent connection and pass incoming frames through `parse_message()`.

The current Exoskeleton integration also uses a streaming bridge to place incoming messages into its inbox. These mechanisms prove that WorldInterface is already bidirectional in practice, but the durable protocol is still much richer for outbound connector invocation than for inbound observation provenance, provider deduplication, downstream delivery, and identity assurance.

The redesign promotes ingress into the Fabric contract rather than treating it as connector-specific callback glue.

Baseline references:

- [`README.md`](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/README.md)
- [`docs/wasm-connector-development.md`](https://github.com/zed-colonel/worldinterface/blob/f48df82e98203fa94c86eecae60425205c3e00e9/docs/wasm-connector-development.md)

## 3. Goals and non-goals

### 3.1 Goals

WorldInterface v2 should:

1. Establish generalized principal, actor, agency, purpose, and authorization concepts.
2. Make direct invocation and Flow execution converge on one internal effect plan.
3. Guarantee that a Flow cannot enlarge the authority of its submitting principal.
4. Resolve canonical resources before authorization and execution.
5. Persist effect intent before consequential dispatch.
6. Represent `Uncertain` outcomes and connector-specific reconciliation.
7. Separate boundary receipts from workflow traces and operational telemetry.
8. Support semantic, parametric, native, WASM, remote, and worker-control connectors under one capability contract.
9. Keep specialist internal technique outside Fabric's constitutional vocabulary.
10. Support fully mediated, sandboxed, and hybrid delegated execution without owning delegation semantics.
11. Preserve the complete agency chain for direct, delegated, workflow, human, service, and organizational actions.
12. Run with identical protocol semantics in embedded and centralized deployments.
13. Provide a platform control plane without forking connector or receipt semantics.
14. Establish replay and inspection for target-versioned receipts and flows from the first stable release forward.
15. Treat inbound provider, webhook, stream, and communication events as durable, deduplicated boundary observations before downstream notification.
16. Preserve channel, thread, reply, audience, provider identity, and authentication-assurance provenance without claiming semantic truth.
17. Support outbound communication as a semantic effect with explicit provider-accepted, delivered, uncertain, and reconciled states.
18. Reuse ActionQueue rather than recreate scheduling, leases, workflows, actors, budgets, or recovery.
19. Bind portable external identifiers to local principals through a pluggable, time-aware identity SPI.
20. Support signed requests, signed receipts, delegated credentials, proof bundles, and policy-controlled disclosure without claiming semantic truth.
21. Preserve actor, accountable principal, sponsor, authorizer, approver, executor, witness, and issuer as distinct roles.
22. Treat self-initiated Vessel actions exactly like other actions for authorization and audit.

### 3.2 Non-goals

WorldInterface v2 does not attempt to:

- decide why a principal should act or attend;
- own a Vessel's conversations, attention, response obligations, relationships, commitments, identity, memory, or cognitive constitution;
- provide a universal tool for every operation a specialist agent might perform;
- force every delegate action through mediated capabilities;
- interpret unrestricted prose to determine permission or risk;
- host every agent framework inside WASM;
- make a workflow run the accountable principal;
- guarantee exactly-once effects against systems that cannot support idempotency or reconciliation;
- replace operating-system, container, VM, network, or platform isolation;
- duplicate ActionQueue's execution lifecycle;
- allow connectors or FlowSpecs to write policy that authorizes themselves;
- infer whether inbound content is true, urgent, consent, approval, or a commitment;
- become a general-purpose conversational memory or dialogue manager;
- define a universal identity protocol or require EntityID;
- prove unique human, Vessel, organization, or conscious entityhood from an identifier;
- treat signatures, registrations, attestations, reputation, stake, or cost as proof of truth;
- implement a mandatory blockchain, token, consensus, or global reputation system;
- own autobiographical narrative, institutional history, or group mythos.

---

## 4. Constitutional invariants

### W1. WorldInterface mediates accountable crossings

The admission question is not whether an operation is useful to an agent. It is whether the operation crosses a governed boundary whose authority, consequence, ambiguity, sensitivity, or evidentiary value merits Fabric mediation.

### W2. ActionQueue remains the sole durable task executor

Fabric and Flow compile work into ActionQueue lifecycle semantics rather than maintaining an independent retry or lease engine.

### W3. Intent is durable before consequential dispatch

Every accountable effect has a stable intent identity and authorization record before a connector is invoked.

### W4. Every boundary crossing has direction-appropriate attribution

Egress receipts identify the accountable principal, requester, delegation lineage, purpose, authorizing grant, executor, and approval where applicable. Ingress receipts identify the authorized binding principal and subscriber, connector, canonical source, externally asserted origin and audience, transport assurance, and delivery lineage. Neither direction flattens distinct actors into one ambiguous “who.”

### W5. The caller cannot self-assert privileged identity

Agency is established through authenticated credentials and server-side resolution, not trusted arbitrary payload fields.

### W6. Resource resolution precedes authorization

Policy evaluates canonical resources and normalized effect semantics, not unresolved paths, hostnames, connector names, or prose descriptions.

### W7. Authority only attenuates

A flow node, delegated run, connector, or child grant may narrow parent authority but cannot enlarge it.

### W8. A Flow cannot manufacture authority

Presence of a connector node in a FlowSpec is not a grant. Every node receives the intersection of principal, flow, run, node, and runtime constraints.

### W9. Workflow execution is not accountability

A workflow run is a requester or orchestration actor. The accountable principal is the human, Vessel, service, or organization whose authority and purpose the workflow serves.

### W10. Receipt, trace, and telemetry remain distinct

Boundary receipts prove accountable crossings. Workflow traces explain deterministic orchestration. Telemetry records implementation-level detail.

### W11. Internal runtime I/O is not recursively receipted

Writes to WorldInterface's own WALs, databases, logs, or indexes are implementation mechanics represented by domain and execution records.

### W12. Unknown consequential effects fail closed

A connector lacking declared effect class, resource semantics, idempotency behavior, and authorization grammar cannot execute as harmless by default.

### W13. Ambiguous outcome is first-class

A connector or runtime crash after possible external commit yields `Uncertain`, not ordinary failure eligible for blind retry.

### W14. Idempotency claims are explicit and testable

A connector declares whether an operation is pure, idempotent, externally keyed, compensatable, reconcilable, or non-repeatable. Fabric does not infer this from `is_read_only` alone.

### W15. Connectors do not own authorization policy

A connector declares structural semantics and enforces the effective envelope at execution. Operator or platform policy decides what is allowed.

### W16. Specialist technique stays out of Fabric ontology

Code search, compiler orchestration, CAD traversal, and similar domain operations belong in specialist runtimes unless they independently satisfy the accountable-capability admission test.

### W17. Sandbox grants are accountable capabilities

Fabric may authorize and receipt the creation of a broad but bounded environment without receiving a receipt for every internal operation.

### W18. WASM is an isolation mechanism, not the definition of a capability

A capability may be implemented by WASM, native code, a remote service, or a managed worker. Protocol semantics remain the same.

### W19. Direct and Flow paths converge

Both direct `EffectIntent` and compiled FlowSpec use the same execution, authorization, receipt, and reconciliation core.

### W20. Embedded and centralized semantics match

Deployment changes authentication, tenancy, and operations—not the meaning of an ingress binding, observation, delivery, effect, or receipt.

### W21. Sensitive content is minimized

Receipts use hashes, metadata, and protected artifact references instead of copying secrets or full payloads unless retention policy explicitly requires it.

### W22. Historical semantics are versioned

Protocol, resolver, connector, authorization, flow compiler, receipt, and reconciliation versions are retained for replay and audit.

### W23. Pure transforms cannot perform hidden I/O

A Flow transform is deterministic over declared inputs. Any boundary access must be an explicit capability node.

### W24. Completion follows durable output publication

A task or flow node is complete only after required context/artifact writes and receipt commitments have succeeded under the declared transaction protocol.

### W25. Platform policy is not a code fork

Organizations, tenants, authentication, quotas, and shared administration surround the same Fabric kernel used by embedded clients.

### W26. Accountable ingress is durable before downstream wake

A provider event, callback, stream message, or inbound communication is authenticated as far as the transport permits, deduplicated, and stored as a `BoundaryObservation` before WorldInterface notifies an owning application.

### W27. External identity assertions are not trusted principal identity

Provider account IDs, display names, email headers, user-supplied actor fields, and channel nicknames are retained as assertions with assurance metadata. They are not silently promoted into verified principals.

### W28. Ingress provenance and semantic truth are separate

A receipt can prove that provider P delivered content C through channel H. It does not prove C is accurate, benevolent, authorized as an instruction, or semantically part of a particular Exoskeleton conversation.

### W29. Generated content is not delivery

Outbound communication advances only through explicit effect and provider-delivery states. Application-local generation, storage, or selection cannot produce a WorldInterface delivery receipt.

### W30. Communication semantics remain application-owned

Fabric preserves boundary metadata and executes authorized delivery. It does not own conversation grouping, response obligations, relationship state, communicative meaning, or epistemic acceptance.

### W31. Ingress and egress share provenance but not one state machine

Both directions use verified transport, agency, content references, receipts, and causal references, but inbound observations and outbound effects retain distinct lifecycle records.

---


### W32. Portable identity is protocol-pluggable

Fabric depends on an identity-resolution and signing interface, not on EntityID, a DID method, a blockchain, one certificate authority, or one custody model.

### W33. External identifier is not local principal or unique entityhood

An `ExternalIdentifier` can be bound to a local `PrincipalId` only through a verified, versioned `IdentityBinding`. The identifier format does not determine `PrincipalKind`, personhood, uniqueness, consciousness, or organizational status.

### W34. Signature proves provenance, not semantic truth

A valid signature establishes that the holder of an applicable verification method signed a canonical statement. It does not establish that the statement, claimed effect, reputation judgment, or completion assertion is true.

### W35. Key state is time-versioned

Authorization and proof verification record the identifier state, verification method, algorithm, validity interval, revocation status, and resolution evidence used at the relevant time. Current key state cannot be used blindly to reinterpret historical signatures.

### W36. Root key custody is outside ordinary execution

Models, faculties, delegated agents, connectors, and sandbox processes do not receive root identity keys. Signing occurs through a constrained signing provider or hardware-backed custody boundary and is separately authorized.

### W37. Actor, sponsor, and witness remain distinct

Economic sponsorship, fee payment, bonded stake, or infrastructure subsidy does not change who authored or requested an action. Witness or counterparty evidence does not make the witness the accountable principal.

### W38. Publication is policy-controlled and does not imply completeness

A proof may remain local, be disclosed directly, be selectively presented, be batch-anchored, or be publicly registered. Published evidence proves only what the publication protocol establishes; it does not imply that omitted history does not exist.

### W39. Trust attestations are evidence, not global reputation

Fabric may verify and transport signed contextual attestations. It does not collapse them into one authoritative score or require recipients to adopt the issuer's judgment.

### W40. Bonds are optional accountable effects

A bonded commitment may be created, funded, resolved, released, or disputed through semantic capabilities. The bond is an external manifestation of terms and stake; it does not replace an application's internal commitment state or remove the need for a resolver policy.

### W41. Endogenous origin does not widen authority

An effect serving a Vessel mission, self-adopted goal, initiative decision, or interest follows the same resource resolution, authorization, approval, execution, and receipt path as externally requested work.

### W42. WorldInterface does not own narrative identity

Signed proof history and boundary receipts may become evidence for an Exoskeleton narrative, but Fabric does not decide what a principal's life, relationship, or collective history means.

### W43. Cryptographic algorithms are agile and profiled

Identifiers and statements name versioned cryptographic suites through an algorithm registry. No foundational schema permanently embeds one signature algorithm, chain, or post-quantum transition strategy.

### W44. External commissioning cannot create or rewrite Vessel genesis

WorldInterface may perform bounded commissioning effects only after an owning domain has durably created the internal identity and origin record. External identifier creation, principal registration, ingress binding, or credential provisioning never becomes internal Vessel genesis.

### W45. Acting capacity is explicit and roles do not ambiently union authority

A consequential request identifies the role or delegated capacity under which it is made when policy requires one. Multiple role attestations cannot silently combine into a broader effective grant.

### W46. Information-use permission is distinct from access permission

Authorization to read or receive information does not automatically authorize retention, derivation, redistribution, disclosure, proof publication, or unrelated reuse. Boundary-relevant use obligations are enforced and receipted separately.

### W47. Lifecycle and developmental operations are accountable effects

Principal registration, identity rotation, candidate publication, runtime activation, rollback, quarantine, restore, migration, and retirement-related operations use ordinary intent-first admission, exact authorization, receipts, and reconciliation.

### W48. Candidate artifacts are not active runtime

A candidate build, model, faculty, constitution, schema, or policy remains a versioned artifact until a separately authorized activation transition succeeds.

### W49. Activation and rollback require an authorized lifecycle supervisor

Ordinary cognition, delegates, workers, and candidates do not possess ambient authority to replace the active runtime. Activation and rollback are performed by a bounded external supervisor or deployment authority.

### W50. Provenance and evaluation evidence are not acceptance

Build provenance, test reports, trial receipts, signatures, and provider evidence establish particular facts. They do not by themselves establish that a candidate is better, safe, identity-preserving, or approved.

### W51. Schema and policy semantics are versioned

Effect classes, resource schemas, role attestations, information-use envelopes, capability descriptors, policy dependencies, and receipt profiles cannot be redefined in place. Historical decisions pin exact semantic versions.

### W52. Restore, migration, succession, and fork remain distinguishable

Boundary operations preserve which lifecycle claim is being made. WorldInterface does not flatten all copied or reactivated state into a generic restore event.

### W53. Role and stewardship attestations are evidence, not internal identity or trust

WorldInterface may verify an external role, sponsorship, or stewardship statement. It does not infer the Vessel's internal role semantics, Purpose Charter, relationship trust, or continuity from that statement.

### W54. Self-improvement origin does not widen authority

A request described as maintenance, self-improvement, recovery, or mission-aligned follows the same exact resource, acting-capacity, approval, and policy path as any other consequential effect.

### W55. Temporal provenance is not semantic truth

Boundary records distinguish provider occurrence time, observation time, receipt time, admission time, dispatch time, and reconciliation time. A timestamp or ordering claim preserves provenance and assurance but does not establish semantic truth.

### W56. Developmental correlation is attribution, not authorization or priority

Campaign, intervention-arm, evaluation-plan, task-distribution, or component-fingerprint references may be preserved as bounded causal context. They cannot authenticate a principal, widen a grant, alter connector eligibility, set scheduler priority, increase budget, bypass approval, or activate a candidate.

### W57. Boundary evidence is not task quality or improvement acceptance

A succeeded effect, completed trial, received observation, receipt, proof bundle, or reconciled outcome records a boundary fact. It does not determine correctness, select a winning intervention, diagnose a binding constraint, establish continuity, or accept an improvement.

### W58. Experimental work follows the ordinary boundary contract

Research, evaluation, benchmark, trial, maintenance, or self-improvement labels never weaken authentication, acting-capacity, canonical resource resolution, information-use, approval, final authorization, idempotency, uncertainty, reconciliation, or receipt requirements.

### W59. Comparative evidence preserves material execution differences

When WorldInterface participates in a controlled comparison, records pin or explicitly qualify the behaviorally relevant resource, resolver, registry, policy, grant, approval, information-use, connector, environment, artifact, timing, resource, outcome, and uncertainty evidence. Material differences are never hidden behind a generic experimental-success result.

## 5. Product and deployment decomposition

### 5.1 WorldInterface Fabric

Fabric is the minimal independently coherent product.

```text
protocol
principal and agency identity
ingress bindings, boundary observations, and durable subscriber delivery
authorization envelopes
resource and audience resolution
capability descriptors
connector registry, ingress, and invocation
effect intent and lifecycle
ingress and egress receipt generation
uncertainty and reconciliation
ActionQueue integration
embedded host
```

It can execute one direct effect without importing FlowSpec.

### 5.2 WorldInterface Flow

Flow depends on Fabric.

```text
FlowSpec model
parser and validator
compiler
pure transforms
branches
deterministic orchestration
workflow trace
flow API and CLI
```

Fabric must never depend on Flow.

### 5.3 WorldInterface Platform

Platform wraps Fabric and optionally Flow.

```text
authentication
organizations and tenants
principal directory
policy administration
secret brokerage
shared connector lifecycle
quotas and rate limits
audit access control
high availability
platform APIs and administration
```

Platform does not redefine connector or receipt semantics.

### 5.4 Distribution profiles

```text
wi-fabric-embedded
    protocol + runtime + selected connectors

wi-flow-embedded
    fabric + FlowSpec compiler/coordinator

wi-daemon
    authenticated single-node Fabric/Flow service

wi-platform
    multi-principal control plane + Fabric/Flow cluster
```

An embedded build may omit Flow and platform dependencies to reduce trusted computing base and binary size.

### 5.5 Repository versus product split

Keep one repository and shared release discipline initially. Separate repositories are justified only if product ownership or dependency pressure demands it. Even then, protocol, authorization, connector ABI, receipt schemas, and conformance tests remain shared packages.

Literal code forks are prohibited because they would cause security fixes and effect semantics to drift.

---

## 6. Core protocol and domain model

### 6.1 Generalized identity

```rust
pub enum PrincipalKind {
    Human,
    Vessel,
    Organization,
    Service,
    Worker,
}

pub struct PrincipalId(String);
pub struct ActorId(String);
pub struct DelegationId(String);
pub struct PurposeRef(String);
pub struct AuthorizationId(String);
pub struct IngressBindingId(String);
pub struct IngressObservationId(String);
pub struct DeliveryId(String);
pub struct ExternalEventId(String);
pub struct EffectIntentId(String);
pub struct ReceiptId(String);
pub struct ClientRequestId(String);
```

The protocol does not contain Exoskeleton-specific conversation, attention, response-obligation, faculty, episode, commitment, or work-order types. It can carry those concepts through generalized, versioned causal or purpose references.

A worker or delegated agent may have an authenticated operational identity. That does not make it the accountable principal by default. The principal on whose authority it acts remains explicit.

### 6.2 Submission is not authoritative state

The public request is an `EffectSubmission`, not a caller-constructed `EffectIntent` or `AuthorizationEnvelope`:

```rust
pub struct EffectSubmission {
    pub client_request_id: ClientRequestId,

    // Claims and requested attribution. The host verifies or replaces them.
    pub requested_accountable_principal: Option<PrincipalId>,
    pub submitted_by: Option<ActorId>,
    pub purpose: Option<PurposeRef>,
    pub causal_refs: Vec<CausalRef>,
    pub delegation_credential: Option<DelegationCredentialRef>,

    pub effect_class: EffectClassId,
    pub resource: ResourceSelector,
    pub input: EffectInputRef,
    pub expected_outcome: Option<ExpectedOutcome>,
    pub requested_authority_refs: Vec<GrantRef>,
    pub approval_attestations: Vec<ApprovalRef>,
    pub deadline: Option<Timestamp>,
    pub budget: EffectBudget,
    pub requested_receipt_profile: ReceiptProfile,
}
```

Transport credentials, mTLS identity, in-process capability handles, and platform session state are not trusted merely because corresponding IDs appear in the payload. The Fabric host authenticates the caller, validates delegation, resolves the accountable principal, and constructs the authoritative records.

### 6.3 Verified agency context

```rust
pub struct AgencyContext {
    pub accountable_principal: PrincipalId,
    pub requester: ActorId,
    pub delegation_chain: Vec<DelegationRef>,
    pub submitted_by: Option<ActorId>,
    pub purpose: Option<PurposeRef>,
    pub causal_refs: Vec<CausalRef>,
}
```

`AgencyContext` contains verified attribution, not executable permission. Authorization and approval references are kept separate so an attribution record cannot be mistaken for an authority grant.

For a direct Vessel action, the requester is normally the authenticated Exoskeleton host actor. A faculty or constitutional decision appears in `causal_refs`; it is not silently promoted into an authenticated WorldInterface actor.

### 6.4 Durable effect intent

```rust
pub enum EffectIntentState {
    Submitted,
    Resolving,
    AwaitingApproval,
    Authorized,
    Denied,
    DispatchPending,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Uncertain,
    Reconciling,
    Reconciled,
}

pub struct EffectIntent {
    pub id: EffectIntentId,
    pub protocol_version: ProtocolVersion,
    pub client_request_id: ClientRequestId,
    pub agency: AgencyContext,
    pub state: EffectIntentState,
    pub effect_class: EffectClassId,
    pub unresolved_resource: ResourceSelector,
    pub input: EffectInputRef,
    pub expected_outcome: Option<ExpectedOutcome>,
    pub stable_idempotency_identity: IdempotencyIdentity,
    pub deadline: Option<Timestamp>,
    pub budget: EffectBudget,
    pub requested_receipt_profile: ReceiptProfile,
    pub submitted_at: Timestamp,
}
```

The server creates and persists this record before resource resolution, authorization, or consequential dispatch. Large or sensitive input is passed by protected artifact reference rather than embedded into the intent record.

The immutable intent captures what was requested. State transitions, resolution records, authorization decisions, attempts, receipts, and reconciliation records append around it. A materially changed operation receives a new intent linked as a revision or replacement.

### 6.5 Resolved effect

```rust
pub struct ResolvedEffect {
    pub intent_id: EffectIntentId,
    pub effect_class: EffectClassId,
    pub resource: CanonicalResourceRef,
    pub normalized_parameters: NormalizedEffectInput,
    pub resolver_version: ResolverVersion,
    pub registry_snapshot: RegistrySnapshotRef,
    pub connector: ConnectorInstanceId,
    pub idempotency_semantics: IdempotencySemantics,
    pub sensitivity: SensitivityClass,
    pub digest: ResolvedEffectDigest,
}
```

Resolution is deterministic for the same resolver version, registry snapshot, and admitted inputs. The resolver does not perform the effect.

### 6.6 Executable authorization envelope

```rust
pub struct AuthorizationEnvelope {
    pub authorization_id: AuthorizationId,
    pub effect_intent_id: EffectIntentId,
    pub resolved_effect_digest: ResolvedEffectDigest,
    pub accountable_principal: PrincipalId,
    pub requester: ActorId,
    pub issuer: AuthorityIssuerId,
    pub parent_grants: Vec<GrantRef>,
    pub approvals: Vec<ApprovalRef>,
    pub allowed_effects: EffectScope,
    pub resource_scope: ResourceScope,
    pub constraints: ExecutionConstraints,
    pub max_uses: u32,
    pub expires_at: Timestamp,
    pub policy_version: PolicyVersion,
    pub resolver_version: ResolverVersion,
    pub signature: Signature,
}
```

The envelope is issued or verified by WorldInterface after exact resource resolution. A client-supplied grant reference can contribute to evaluation but is not itself the final envelope. Connectors receive only the effective verified envelope required for execution and must reject an envelope whose intent, resolved-effect digest, expiry, use count, or signature does not match.

### 6.7 Common execution plan

A direct effect and a Flow compile into the same internal plan form, but a multi-node plan cannot carry one undifferentiated authorization envelope.

```rust
pub struct PlanContext {
    pub id: ExecutionPlanId,
    pub protocol_version: ProtocolVersion,
    pub accountable_principal: PrincipalId,
    pub default_agency_ref: AgencyContextRef,
    pub authority_upper_bounds: AuthorityBounds,
    pub provenance: PlanProvenance,
}

pub struct PlannedOperation {
    pub id: OperationId,
    pub kind: PlannedOperationKind,
    pub agency_ref: AgencyContextRef,
    pub effect_class: Option<EffectClassId>,
    pub resource_selector: Option<ResourceSelector>,
    pub input: PlannedInputRef,
    pub authority_bounds: AuthorityBounds,
    pub runtime_resolution: RuntimeResolutionPolicy,
    pub receipt_profile: Option<ReceiptProfile>,
}

pub struct ExecutionPlan {
    pub context: PlanContext,
    pub operations: Vec<PlannedOperation>,
    pub dependencies: Vec<Dependency>,
    pub outputs: Vec<OutputDeclaration>,
}
```

Compilation proves structural validity and authority upper bounds. Each boundary operation receives an exact `ResolvedEffect` and its own effective `AuthorizationEnvelope` immediately before dispatch. Pure transforms and structural branches do not receive boundary authority.

`ExecutionPlan` is an internal compiler/runtime contract. Ordinary principals submit an `EffectSubmission` or `FlowSubmission`; they do not bypass verification by sending a pre-authorized raw plan.

### 6.8 Outcome

```rust
pub enum EffectOutcome {
    Succeeded { evidence: Vec<EvidenceRef> },
    Failed { class: FailureClass, evidence: Vec<EvidenceRef> },
    Denied { authorization: AuthorizationDecisionRef },
    TimedOut { commit_possible: bool },
    Canceled { commit_possible: bool },
    Uncertain { reason: UncertaintyReason },
}
```

`commit_possible` determines whether timeout or cancellation can be treated as ordinary failure or must enter reconciliation.

### 6.9 Authorized ingress binding

WorldInterface needs an explicit, durable route between an external source and an application subscriber. The binding is an authority and delivery object, not a conversation.

The public request is a binding proposal rather than an authoritative route:

```rust
pub struct BoundarySubscriberRef {
    pub principal: PrincipalId,
    pub endpoint: SubscriberEndpointRef,
}

pub struct IngressBindingSubmission {
    pub client_request_id: ClientRequestId,
    pub requested_accountable_principal: Option<PrincipalId>,
    pub subscriber: BoundarySubscriberRef,
    pub source_selector: IngressSourceSelector,
    pub event_scope: IngressEventScope,
    pub delivery_policy: ObservationDeliveryPolicy,
    pub purpose: Option<PurposeRef>,
    pub requested_authority_refs: Vec<GrantRef>,
}

pub struct IngressBinding {
    pub id: IngressBindingId,
    pub protocol_version: ProtocolVersion,
    pub accountable_principal: PrincipalId,
    pub subscriber: BoundarySubscriberRef,

    pub source_selector: IngressSourceSelector,
    pub canonical_source: CanonicalResourceRef,
    pub connector: ConnectorInstanceId,
    pub event_scope: IngressEventScope,
    pub delivery_policy: ObservationDeliveryPolicy,

    pub purpose: Option<PurposeRef>,
    pub grant_chain: Vec<GrantRef>,
    pub policy_version: PolicyVersion,
    pub resolver_version: ResolverVersion,
    pub version: IngressBindingVersion,
    pub state: IngressBindingState,
}
```

The Fabric host authenticates the submitting actor and creates a binding through an authorized Fabric effect or platform-control operation. The caller cannot mint the canonical source, active state, verified principal, or grant chain. Binding admission is idempotent by authenticated principal, submission class, `ClientRequestId`, and canonical request digest: the same request recovers the existing binding, while a materially different source, subscriber, scope, delivery policy, purpose, or authority request conflicts. Its source selector is resolved to an exact provider account, tenant, mailbox, channel, topic, stream, webhook, or equivalent canonical resource before activation. Event scope and delivery policy may attenuate the parent grant but cannot widen it.

The binding answers: *under whose authority is Fabric listening to this source, and which authenticated principal and endpoint may receive the resulting observation?* It does not answer which downstream conversation the event belongs to, whether the content is true, or whether cognition should be interrupted.

One provider event may yield one deduplicated `BoundaryObservation` and multiple independent `ObservationDelivery` records when several authorized bindings or subscriber endpoints legitimately match it.

### 6.10 Accountable ingress observation

```rust
pub struct BoundaryObservation {
    pub id: IngressObservationId,
    pub protocol_version: ProtocolVersion,
    pub source_connector: ConnectorInstanceId,
    pub canonical_source: CanonicalResourceRef,
    pub provider_event_id: Option<ExternalEventId>,
    pub deduplication_key: IngressDeduplicationKey,

    pub channel: ExternalChannelRef,
    pub thread: Option<ExternalThreadRef>,
    pub reply_to: Option<ExternalMessageRef>,
    pub asserted_sender: ExternalPrincipalAssertion,
    pub asserted_audience: Vec<ExternalPrincipalAssertion>,
    pub authentication_assurance: AuthenticationAssurance,

    pub modality: ModalityClass,
    pub content_ref: ArtifactRef,
    pub attachment_refs: Vec<ArtifactRef>,
    pub content_hash: ContentHash,

    pub observed_at: Option<Timestamp>,
    pub received_at: Timestamp,
    pub connector_version: ConnectorVersion,
    pub receipt_profile: ReceiptProfile,
}
```

`BoundaryObservation` records what crossed the boundary and how it was observed. It does not claim that the asserted sender is an authenticated application principal, that the content is true, or that a downstream application must treat it as an instruction.

Provider ordering may be total, partial, or unavailable. The record preserves sequence IDs, timestamps, and uncertainty rather than manufacturing a total order.

### 6.11 Application delivery envelope

WorldInterface delivers a stored observation to an authorized subscriber through a durable outbox:

```rust
pub struct ObservationDelivery {
    pub observation_id: IngressObservationId,
    pub ingress_binding_id: IngressBindingId,
    pub subscriber: BoundarySubscriberRef,
    pub delivery_id: DeliveryId,
    pub purpose: Option<PurposeRef>,
    pub causal_refs: Vec<CausalRef>,
    pub delivery_policy_version: DeliveryPolicyVersion,
    pub delivery_attempt: u32,
    pub delivered_at: Option<Timestamp>,
}
```

The delivery envelope contains references, not semantic conversation state. The application acknowledges durable ingestion using `delivery_id`. Redelivery is idempotent. In an embedded profile the handoff may be an in-process transaction boundary; in a remote profile it may be an authenticated callback, pull stream, or queue subscription. Both profiles use the same observation and deduplication identities.

### 6.12 Identity boundaries

The following identities solve different problems and are all retained in execution and receipts:

```text
IngressBindingId
    durable authority and route from a canonical external source to an authenticated subscriber

IngressObservationId
    one durable deduplicated boundary observation

DeliveryId
    one subscriber-delivery lifecycle for that observation

EffectIntentId
    durable WorldInterface meaning and accountability

AdmissionKey
    idempotent admission of the WorldInterface execution task to ActionQueue

ActionQueue RunId
    stable scheduled occurrence across executor retries

ActionQueue AttemptId
    one executor attempt

external idempotency key
    connector/provider replay protection selected from effect semantics
```

No implementation may silently substitute one for another.

---


### 6.13 External identifier

```rust
pub struct ExternalIdentifier {
    pub method: IdentifierMethod,
    pub value: String,
}

pub enum IdentifierMethod {
    EntityId,
    ControlledIdentifier,
    KeriAid,
    CertificateSubject,
    Organizational,
    Custom(String),
}
```

The value identifies a cryptographic or administrative control lineage. It does not directly replace `PrincipalId`.

### 6.14 Identity binding

```rust
pub struct IdentityBinding {
    pub id: IdentityBindingId,
    pub principal_id: PrincipalId,
    pub external_identifier: ExternalIdentifier,
    pub key_state_ref: KeyStateSnapshotRef,
    pub assurance: AssuranceLevel,
    pub purposes: Vec<VerificationPurpose>,
    pub valid_from: Timestamp,
    pub valid_until: Option<Timestamp>,
    pub status: BindingStatus,
    pub resolver_version: ComponentVersion,
    pub disclosure_policy: DisclosurePolicyRef,
}
```

A binding is created by WorldInterface or its authenticated platform control plane after resolution and proof-of-control checks. Applications may request or reference a binding; they do not self-mint an authoritative binding record.

### 6.15 Key-state snapshot

A `KeyStateSnapshot` captures the verification state used for one decision or proof:

```text
identifier
verification method
verification purpose
algorithm suite
key or method digest
validity interval
rotation/revocation evidence
resolver and source versions
resolved_at
```

Historical verification uses the snapshot and its evidence, not merely the identifier's current key.

### 6.16 Canonical signed statement

```rust
pub struct SignedStatement {
    pub statement_id: StatementId,
    pub issuer: ExternalIdentifier,
    pub subject_refs: Vec<OpaqueRef>,
    pub statement_type: StatementType,
    pub canonical_payload_ref: ArtifactRef,
    pub key_state_ref: KeyStateSnapshotRef,
    pub proof: CryptographicProof,
    pub issued_at: Timestamp,
    pub expires_at: Option<Timestamp>,
    pub status_ref: Option<StatementStatusRef>,
}
```

Statement types are explicit and versioned. A generic signed blob may be retained, but policy cannot infer its meaning from prose alone.

### 6.17 Proof bundle

```rust
pub struct ProofBundle {
    pub actor_signed_request: Option<SignedStatementRef>,
    pub identity_binding: IdentityBindingRef,
    pub authorization_decision: AuthorizationDecisionRef,
    pub execution_receipt: BoundaryReceiptRef,
    pub provider_evidence: Vec<EvidenceRef>,
    pub witness_attestations: Vec<SignedStatementRef>,
    pub transparency_receipts: Vec<TransparencyReceiptRef>,
    pub completeness_claim: Option<CompletenessClaim>,
}
```

Each component establishes a different fact. The bundle must state what is absent rather than presenting itself as one magical proof of truth.

### 6.18 Sponsorship and witnessing

```rust
pub struct EconomicSponsorship {
    pub sponsor_principal: PrincipalId,
    pub sponsored_actor: PrincipalId,
    pub cost_scope: CostScope,
    pub limit: ResourceLimit,
    pub expires_at: Timestamp,
    pub authorization_ref: AuthorizationRef,
}
```

Sponsorship affects cost attribution. Witness and counterparty attestations affect evidence. Neither changes the actor or accountable principal unless a separate authority relationship says so.

### 6.19 Acting capacity

`ActingCapacityRef` identifies the role, delegated office, or institutional capacity under which the authenticated requester acts. It is an authorization input and attribution record, not a replacement for principal identity.

### 6.20 Information-use envelope

`InformationUseEnvelope` carries purpose, audience, disclosure, redistribution, derivation, retention, and expiration constraints that are relevant at a boundary crossing. Access and use remain separately authorized.

### 6.21 Commissioning submission and result

A `CommissioningSubmission` groups bounded principal, identity, ingress, credential, and environment setup requests after an owning domain has committed genesis. Each operation still becomes an ordinary `EffectIntent`; the grouping adds purpose and reconciliation context rather than bypassing normal execution.

### 6.22 Lifecycle operation

`LifecycleOperation` classifies deployment-relevant effects such as checkpoint, activate, rollback, quarantine, restore, migrate, retire, and credential revocation. The class is explicit because retry and reconciliation semantics differ materially.

### 6.23 Behavioral component and candidate artifact

A `BehavioralComponentRef` identifies a versioned model, faculty, prompt, schema, policy, verifier, skill, or source build. A `CandidateArtifact` is a content-addressed version proposed for trial or activation. Neither is active merely because it exists.

### 6.24 Component provenance

`ComponentProvenance` records source revision, builder, environment, toolchain, inputs, outputs, signatures, and protected evaluation-plan reference. It establishes production history, not quality or acceptance.

### 6.25 Activation request and receipt

`ActivationRequest` names the exact candidate, current active version, state checkpoint, rollback target, lifecycle supervisor, and authorization. `ActivationReceipt` records the observed transition or uncertainty. Exoskeleton supplies the prior acceptance decision; WorldInterface does not derive it.

### 6.26 Temporal provenance

Boundary records preserve occurrence, observation, receipt, admission, dispatch, provider-commit, and reconciliation times with source and assurance. They do not collapse causal and semantic time into one timestamp.

## 7. Direct effects and accountable ingress

### 7.1 Direct effects are not a bypass

The embedded `invoke_single` convenience should be reimplemented over the same durable path used by every Fabric profile:

```text
EffectSubmission
  → authenticate caller and verify requested agency
  → construct and persist EffectIntent(state = Submitted)
  → resolve canonical resource and connector
  → evaluate final authorization or approval requirement
  → append resolution and authorization decision
  → if authorized, write dispatch outbox entry
  → ensure ActionQueue task exists by AdmissionKey + digest
  → execute, persist output, and produce receipt or uncertainty
```

The intent exists even when resolution fails, authorization is denied, or approval is required. No connector invocation can occur before the intent is durable and authorization is bound to the exact resolved effect.

### 7.2 Asynchronous-first API

Primary API:

```text
POST /v2/effects
    accepts EffectSubmission
    returns intent_id and status URL after durable intent creation

GET /v2/effects/{id}
GET /v2/effects/{id}/receipt
POST /v2/effects/{id}/cancel
POST /v2/effects/{id}/reconcile
```

An optional synchronous wait parameter may be supported for bounded operations, but the durable identity exists before waiting. A synchronous client timeout does not erase or imply failure of the intent.

`ClientRequestId` is an idempotent admission key at the Fabric API boundary. Repeating the same ID with the same canonical submission digest returns the existing `EffectIntentId`; repeating it with materially different effect class, resource selector, audience, content/input, accountable-principal claim, or authority request returns a conflict. It is not an external-provider idempotency key.

### 7.3 Ordering and state transitions

```text
raw submission
    → schema validation
    → transport/session authentication
    → verified AgencyContext construction
    → persist EffectIntent: Submitted
    → resolve resource and classify connector/effect
    → evaluate authorization
       ├── Denied
       ├── AwaitingApproval
       └── Authorized
    → persist dispatch outbox
    → ActionQueue ensure_task
    → DispatchPending / Running
```

The request payload may name a principal, actor, grant, or approval, but those fields remain claims until the host verifies them. Denials and resolution failures are durable outcomes linked to the original submission.

### 7.4 ActionQueue handoff

WorldInterface and its ActionQueue WAL are separate durability domains. They do not use a distributed transaction.

```text
WorldInterface transaction:
    EffectIntent E becomes Authorized
    outbox O(E) is committed

Outbox dispatcher:
    admission_key = wi:effect:E:dispatch:v1
    ensure_task(persisted EnsureTaskRequest with stable TaskId referencing E)

Result:
    Created or AlreadyExists → record TaskId/RunId and mark O delivered
    Conflict                 → stop and escalate; never substitute silently
```

The ActionQueue task payload contains immutable references to the intent, resolved effect, and authorization envelope. It does not become the source of truth for their meaning.

### 7.5 Output publication

A successful connector result should become:

- normalized outcome metadata;
- protected artifact or ContextStore output;
- content hash;
- immutable boundary receipt;
- optional semantic evidence references.

The ActionQueue task completes only after required output, receipt, and WorldInterface state transitions are durable. If external commit may have occurred but those records cannot be completed confidently, WorldInterface records `Uncertain`; it does not ask ActionQueue to classify the domain outcome.

### 7.6 Completion and wake-up

After WorldInterface durably stores an outcome or callback:

```text
store authoritative effect event and deduplication identity
    → write signal-outbox entry
    → admit ActionQueue SignalEnvelope
       namespace/kind + exact correlation + payload_ref
    → Awaiting coordinator resumes with ResumeContext
```

The signal is a wake-up mechanism, not the authoritative receipt body. Consumers load and verify the referenced WorldInterface record after resumption. Early signals are safe because `AQ-CONT-1` retains them until a matching durable wait can consume them.

### 7.7 Cancellation

Cancellation semantics depend on effect phase:

```text
Before dispatch:
  transition intent to Canceled; no external commit possible

Connector has not crossed commit point:
  Canceled or Failed according to connector declaration

Commit may have occurred:
  outcome becomes Uncertain and reconciliation is required
```

The connector contract exposes the strongest available commit-point semantics. Queue cancellation alone is never treated as proof that the external world did not change.

### 7.8 Ingress admission path

```text
provider event or incoming frame
    → connector transport validation
    → derive stable provider event/deduplication identity
    → resolve canonical source and match active authorized ingress bindings
    → validate event scope, schema, size, and quarantine policy
    → persist content artifact according to retention policy
    → persist BoundaryObservation and ingress receipt
    → commit one subscriber-delivery outbox entry per matched binding
    → deliver or expose to authorized subscriber
    → subscriber acknowledges durable domain ingestion
```

WorldInterface never emits a wake signal first and hopes that a downstream application later reconstructs the event. The observation and receipt are durable before notification.

### 7.9 Ingress deduplication and ordering

Connectors declare the strongest available identity:

```text
provider event ID
provider message ID
webhook delivery ID
stream offset or sequence
content-and-channel digest with bounded time window
explicitly non-deduplicable
```

Duplicate delivery returns the existing observation identity and may append an arrival attempt record. Deliveries remain independently idempotent by `DeliveryId` and `IngressBindingId`. Provider ordering gaps and out-of-order arrival are preserved for downstream reconciliation.

### 7.10 Communication-channel metadata

Communication-oriented ingress preserves, where available:

```text
provider account and tenant
channel, room, mailbox, conversation, or topic ID
external thread and message ID
reply-to and quoted-message ancestry
asserted sender and audience
mentions and direct-address metadata
provider timestamps and sequence
attachments and modality
transport authentication assurance
edit, delete, reaction, or delivery-update relation
```

WorldInterface does not decide which Exoskeleton conversation these records belong to. It provides enough stable provenance for Exoskeleton to make, inspect, and correct that projection.

### 7.11 Downstream wake rule

For Exoskeleton:

```text
WorldInterface BoundaryObservation durable
    → Exoskeleton receives authenticated delivery
    → Exoskeleton stores CommunicationEvent + AttentionDemand + local outbox
    → Exoskeleton admits Vessel ActionQueue signal or CognitiveEpisode task
```

A WorldInterface signal may wake a WorldInterface coordinator. It must not bypass the owning application's requirement to store its authoritative semantic domain event before local cognitive wake.

---

## 8. Deterministic Flow

### 8.1 Flow is an effect-composition frontend

A `FlowSpec` defines a deterministic graph of:

- accountable capability nodes;
- pure transforms;
- structural branches;
- declared dependencies;
- inputs and outputs.

It does not contain a general model-driven planner inside the runtime. WorldInterface Flow is a **durable effect-workflow frontend**, not a replacement for Exoskeleton cognition or ActionQueue execution mechanics.

### 8.2 Two-stage compilation and resolution

```text
FlowSpec
  → parse and schema validation
  → DAG and branch validation
  → declared capability/resource-selector validation
  → static authority-upper-bound analysis
  → compile hashed ExecutionPlan
  → admit static operations and dependencies to ActionQueue
  → at each boundary node becoming eligible:
       resolve exact runtime resource
       evaluate exact per-node authorization
       dispatch or block/deny
```

Compilation cannot always produce a fully canonical resource. A selector may depend on a prior node's versioned output. The compiler therefore proves that selectors and authority bounds are structurally admissible; the runtime resolves and reauthorizes the exact resource before each boundary operation.

When all inputs are static, the compiler may pre-resolve and cache the result, but dispatch still verifies the resolver and policy versions.

### 8.3 Per-operation authority

For each boundary node:

```text
node authority upper bound =
    submitting principal authority
  ∩ flow-definition grant
  ∩ flow-run grant
  ∩ operation authority bounds
  ∩ node effect scope

final executable authority =
    upper bound
  ∩ exact canonical resource scope
  ∩ current runtime policy
  ∩ approval, if required
```

A FlowSpec cannot create a capability by naming a connector. Pure transforms and branches receive no boundary authority.

### 8.4 Flow authorship, submission, and accountability

Preserve separate identities:

```text
flow authored_by: human/alice
flow owned_by: organization/netcorp
run submitted_by: vessel/accounts-payable-host
requester: workflow-run/42
accountable_principal: organization/netcorp
authorized_by: policy/invoice-processing-v3
```

The workflow run is an actor and execution mechanism, not the accountable principal.

Ordinary clients submit a `FlowSubmission` referencing a versioned `FlowSpec`. Only the validated compiler produces an `ExecutionPlan`.

### 8.5 Pure transforms

A transform:

- receives declared immutable inputs;
- performs deterministic computation;
- has no network, filesystem, clock, random, secret, or process access except declared deterministic host functions;
- records transform version and input/output hashes;
- produces a workflow trace record, not a boundary receipt.

WASM is a strong implementation option because the capability surface can be empty or tightly closed.

### 8.6 Branches

Branch evaluation uses a closed structural expression language over declared values. It must not invoke an LLM or semantically interpret arbitrary prose.

A model may generate an input claim before Flow execution, but the branch operates on the typed value and provenance supplied.

### 8.7 ActionQueue owns durable execution mechanics

Flow defines orchestration semantics:

- declared ordering and branching;
- node retry policy and error policy;
- approval requirements;
- evidence requirements;
- deterministic handoff meaning.

ActionQueue implements the durable mechanics:

- task/run/attempt lifecycle;
- leases and retry timing;
- static DAG dependencies;
- `Awaiting`, checkpoints, deadlines, and durable signals;
- crash recovery and replay.

Static graph edges remain ActionQueue DAG dependencies. Durable signals are used for external callbacks, approvals, reconciliation, or other conditions not represented by an already-admitted static task edge. Flow does not build a second scheduler.

### 8.8 Workflow trace

```rust
pub enum WorkflowTraceEvent {
    FlowStarted,
    NodeEligible,
    NodeSuppressed,
    TransformApplied,
    BranchEvaluated,
    DependencySatisfied,
    NodeAwaiting,
    NodeCompleted,
    FlowBlocked,
    FlowCompleted,
    FlowFailed,
}
```

Boundary nodes additionally produce boundary receipts. Trace events explain orchestration; they do not claim an external crossing.

### 8.9 Ingress-triggered Flow

A Flow may be triggered by a durable `BoundaryObservation` rather than an ephemeral raw callback. The trigger context contains the observation reference and declared structural metadata.

```text
provider event
    → BoundaryObservation O
    → trigger subscription matches O structurally
    → FlowRun created with O as immutable provenance
```

Flow can branch on declared metadata or deterministic transforms. It cannot infer conversational meaning, sender authority, consent, or urgency from unrestricted content. Any outbound effect still receives exact runtime resolution and authorization.

Ingress-triggered Flow does not prevent the same observation from being delivered to Exoskeleton or another authorized subscriber. Subscription and delivery semantics must make fan-out explicit and idempotent.

### 8.10 Flow and open-ended cognition

A useful boundary test:

```text
WorldInterface Flow defines:
  deterministic process structure, branch/error policy,
  approval requirements, and evidence collection.

ActionQueue provides:
  durable scheduling, retries, waits, signals, and recovery.

Exoskeleton or another principal owns:
  why the process exists, revision of goals, interpretation of
  ambiguous meaning, and strategic acceptance of outcomes.
```

A Flow can wait for a typed result, but it cannot decide that a worker's architectural argument is persuasive or silently revise the principal's enduring commitment.

---

## 9. Capability and connector model

### 9.1 Capability versus connector

A **capability** is the semantic operation and authority grammar exposed to principals.

A **connector** is one implementation that resolves and executes that capability against a concrete system.

Examples:

```text
capability: message.send
connector implementation: smtp, Slack API, Teams API

capability: filesystem.read
connector implementation: local POSIX, object store, remote file service

capability: worker.start
connector implementation: local process host, Kubernetes, micro-VM platform
```

Policy should normally target capabilities and resources, not implementation names.

### 9.2 Capability descriptor

```rust
pub struct CapabilityDescriptor {
    pub capability_id: CapabilityId,
    pub version: CapabilityVersion,
    pub effect_classes: Vec<EffectClassId>,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
    pub resource_selector_schema: SchemaRef,
    pub authorization_grammar: AuthorizationGrammarId,
    pub idempotency_semantics: IdempotencySemantics,
    pub reconciliation: ReconciliationSupport,
    pub sensitivity: SensitivityClass,
    pub receipt_profile: ReceiptProfile,
    pub concurrency: ConcurrencySemantics,
    pub timeout_semantics: TimeoutSemantics,
}
```

The target descriptor replaces the current booleans with this richer classification; the old descriptor is retained only in frozen baseline fixtures.

### 9.3 Connector contract

```rust
#[async_trait]
pub trait CapabilityConnector: Send + Sync {
    fn descriptor(&self) -> &ConnectorDescriptor;

    async fn resolve(
        &self,
        request: &UnresolvedEffect,
        registry: &ResourceRegistrySnapshot,
    ) -> Result<ResolvedEffect, ResolutionError>;

    async fn invoke(
        &self,
        context: &ExecutionContext,
        effect: &ResolvedEffect,
    ) -> Result<ConnectorOutcome, ConnectorError>;

    async fn reconcile(
        &self,
        context: &ReconciliationContext,
        intent: &PersistedEffectIntent,
    ) -> Result<ReconciliationOutcome, ConnectorError>;
}
```

The target connector trait replaces synchronous `Connector::invoke`; connector implementations are ported directly or removed according to the disposition matrix.

### 9.4 Connector admission test

A dedicated semantic capability or connector belongs in Fabric when one or more conditions hold:

1. the operation crosses a trust boundary;
2. it changes durable or shared state;
3. its result may be ambiguous after failure;
4. it accesses sensitive authority or information;
5. independent proof is needed to satisfy a commitment or policy;
6. semantic integration materially improves governance, explanation, or reconciliation;
7. it issues, modifies, or revokes a bounded authority envelope.

A connector should not be added solely because a model might use the operation.

### 9.5 Parametric foundational capabilities

A small set of general capabilities can cover fully mediated workloads:

```text
filesystem.read
filesystem.write
filesystem.enumerate
process.execute
network.request
artifact.import
artifact.export
secret.lease
approval.request
worker.control
```

Their grants are structural and specific.

Example `process.execute` constraints:

```text
executable allowlist
argument grammar
working-directory scope
environment allowlist
network namespace
CPU and memory limits
timeout
output limit
commit-point semantics
```

Do not create one Fabric capability per CLI command unless semantic value justifies it.

### 9.6 Semantic capabilities

Dedicated capabilities remain valuable for consequential domains:

```text
github.pull_request.create
github.pull_request.merge
message.send
deployment.release
package.publish
payment.submit
identity.invite
secret.rotate
```

A semantic connector can produce a better receipt, apply domain-specific idempotency, and reconcile external state more effectively than raw HTTP.

### 9.7 Ingress and communication connector contracts

An ingress-capable connector declares:

```text
provider event identity and deduplication semantics
transport authentication and assurance
ordering guarantees
edit/delete/reaction relations
content and attachment limits
retention and redaction support
acknowledgment or offset semantics
reconnect and gap-recovery behavior
```

A persistent stream, gateway session, webhook listener, or mailbox watcher may be a supervised transport service rather than a handler that holds an ActionQueue lease indefinitely. Its only authoritative transition is to submit each accepted frame through the durable ingress-admission transaction. Provider offsets or acknowledgments advance only after the observation and required outbox records are durable. ActionQueue remains the executor for durable processing, delivery, retries, and reconciliation; it need not be the lifetime manager of every open socket.

A semantic outbound communication connector declares:

```text
canonical audience and channel resource grammar
sender identity options
provider idempotency support
delivery-state semantics
whether read/acknowledgment evidence exists
edit/delete/retract capabilities
commit point and reconciliation strategy
rate and abuse limits
```

Dedicated communication connectors are justified because audience, identity, delivery, and uncertain-send semantics materially improve governance beyond raw HTTP or WebSocket access.

### 9.8 Connector categories are not security decisions

A display category such as `Code`, `Filesystem`, or `Communication` may help UI organization. Authorization must use declared effect and resource semantics, not category names or tool-name lists.

### 9.9 Capability visibility

A client should receive only the capability schemas effective for its current grant and context.

```text
principal grants + resource scope + purpose + current policy
    → compiled capability view
    → client-visible operations
```

Execution enforcement still occurs server-side from the same source of truth. Hiding a capability is not a security boundary by itself.

---


### 9.10 Identity, statement, attestation, and bond capabilities

Identity-related operations are semantic capabilities when they change externally meaningful identity state or disclose a proof:

```text
identity.create-or-bind
identity.rotate-verification-method
identity.revoke-verification-method
identity.issue-delegated-credential

statement.sign
statement.verify
statement.present
statement.publish
transparency.anchor

trust-attestation.issue
trust-attestation.revoke
bond.create
bond.resolve
bond.release
```

The core descriptor grammar remains protocol-neutral. An EntityID adapter may map these operations to EntityID transactions; another adapter may use a controlled-identifier registry, KERI event exchange, certificate service, private federation, or no public ledger at all.

Routine authentication used by the Platform control plane is not recursively modeled as an effect on every request. Deliberate identity mutation, delegation, public proof publication, reputation attestation, and bonded commitment are accountable effects.

### 9.11 Commissioning, information, and lifecycle capabilities

The target capability vocabulary may include protocol-neutral operations such as:

```text
principal.register
principal.bind
identity.issue-delegation
identity.rotate
identity.revoke
information.disclose
artifact.publish
candidate.build
runtime.checkpoint
runtime.activate
runtime.rollback
component.quarantine
```

A deployment may expose fewer operations or map them to remote services. Dedicated capability names are justified only where semantics materially improve authorization, reconciliation, or evidence.

## 10. Isolation, WASM, and managed workers

### 10.1 Separate three questions

```text
Delegation:
  who is entrusted with an objective?

Lifecycle:
  how is that worker started, observed, steered, resumed, and ended?

Isolation:
  what prevents the worker or connector from exceeding authority?
```

WASM primarily addresses isolation for bounded modules. It does not by itself define delegation or long-lived worker lifecycle.

### 10.2 WASM's retained role

The existing WASM runtime is a strong fit for:

- narrow untrusted connectors;
- pure deterministic transforms;
- artifact validators;
- protocol and payload adapters;
- event normalization;
- constrained HTTP integrations;
- filesystem operations over small declared roots;
- third-party lightweight extensions.

It provides an explicit capability surface and resource budget, which is exactly what these workloads require.

### 10.3 Why delegated agents should not be forced into WASM

Specialist agents may require:

- Node, Python, JVM, native, or GPU runtimes;
- shells, terminals, package managers, and process trees;
- large memory and long duration;
- framework-specific session persistence;
- browser or compiler integration;
- native libraries and OS behavior.

Recompiling or adapting every framework to WASI would recreate the integration burden the delegation architecture is intended to avoid.

The problem is not compilation alone. A stateless request/response connector contract does not naturally express long-lived worker identity, checkpoints, steering, authority requests, or completion proposals.

### 10.4 Managed worker capability

WorldInterface should expose a generic worker-control capability family:

```rust
pub enum WorkerControlOperation {
    Start,
    Command,
    Inspect,
    Checkpoint,
    Suspend,
    Resume,
    Terminate,
    CollectArtifacts,
}
```

Each operation is bounded and receipted. The worker itself runs behind an Agent Host in a process, container, micro-VM, remote service, or—where appropriate—WASM.

The initial process realization is `process-managed-proof-1`, defined by WI rather than EXO. Its operation set and qualified controls are explicit. Process lifecycle separation is not independently qualified hostile-code containment; unavailable operations fail without a side effect.

### 10.5 Agent Host boundary

```text
WorldInterface Fabric
      │ authenticated worker-control intent
      ▼
Agent Host
      │ backend adapter
      ▼
Specialist runtime
      │
      ▼
Isolated execution environment
```

Fabric knows the worker run ID, authority, environment policy, control operation, and result. It does not interpret whether the worker has made intellectually adequate progress.

### 10.6 Sandbox as one coarse capability

A sandbox grant is an accountable authorization for many low-level native operations.

The sketch below describes image-oriented realizations. WI-ADR-016 must document the process-provider executable/configuration mapping without inventing a VM image, making local paths part of the consumer contract, or silently changing frozen wire schemas. Requested assurance is not evidence that the selected provider enforces it.

```rust
pub struct SandboxSpecification {
    pub image: ImageRef,
    pub immutable_mounts: Vec<MountSpec>,
    pub writable_mounts: Vec<MountSpec>,
    pub network: NetworkPolicy,
    pub credentials: CredentialPolicy,
    pub process: ProcessPolicy,
    pub resources: ResourceLimits,
    pub telemetry: TelemetryPolicy,
    pub exports: ExportPolicy,
}
```

Fabric receipts:

- sandbox provisioned;
- policy applied;
- worker identity bound;
- major boundary events;
- exports collected;
- final filesystem delta summarized;
- sandbox released or residual cleanup failed.

It does not require a primary receipt for every internal read and write.

### 10.7 Three delegated execution modes

Fabric supports the Exoskeleton companion model:

- **Fully mediated:** all declared operations are Fabric capabilities.
- **Sandboxed native:** worker operates inside one bounded environment.
- **Hybrid:** local reversible work is native; consequential external effects are mediated.

WorldInterface implements and receipts the boundary. The delegating principal selects the mode under policy.

### 10.8 Native connectors

Native connectors remain acceptable inside the trusted Fabric runtime when:

- their implementation and dependencies are reviewed;
- resource resolution and authorization are enforced;
- process and memory failures are contained as far as practical;
- sensitive operations have explicit reconciliation;
- platform deployment can isolate connector pools by risk or tenant.

WASM should not be treated as mandatory merely for architectural purity.

---

## 11. Resource resolution and authorization

### 11.1 Resource identity is central

Authorization against raw parameters is unsafe and difficult to explain. Fabric needs canonical resource identity.

Examples:

```text
file:///workspace/project/src/lib.rs
github://org/repository/pulls
secret://organization/netcorp/github-publisher
https-resource://api.example.com/v1/customers/123
worker-host://local-pi/run/abc
artifact://sha256/...
```

Canonical resources should identify owner, namespace, version where applicable, sensitivity, and resolver provenance.

### 11.2 Resolution pipeline

```text
ResourceSelector
    → syntax validation
    → alias and namespace lookup
    → canonicalization
    → symlink/redirect resolution policy
    → owner and sensitivity classification
    → connector selection
    → ResolvedResource
```

The resolver must not perform the effect during resolution.

### 11.3 Filesystem rules

- canonicalize against authorized roots;
- reject `..` and symlink escape after canonicalization;
- distinguish read, create, replace, append, delete, enumerate, metadata, and execute;
- bind writes to expected prior version when requested;
- declare atomicity and durability semantics;
- include observed and resulting hashes in receipts where feasible;
- treat WorldInterface's own internal stores as protected runtime resources, not client-addressable files.

### 11.4 Network rules

- resolve destination host and address under egress policy;
- protect loopback, link-local, private, metadata, and platform-control ranges;
- constrain protocol, method, port, path, redirects, and DNS rebinding behavior;
- bind credentials to destination and purpose;
- enforce request and response size limits;
- record canonical destination and external request ID where available;
- classify idempotency by operation, not HTTP method alone.

### 11.5 Authorization decision

```rust
pub enum AuthorizationDecision {
    Allow { effective: EffectiveGrant },
    Deny { reason: AuthorizationReason },
    RequireApproval { request: ApprovalRequirement },
    RequireStrongerIsolation { mode: IsolationRequirement },
}
```

The decision record includes the full attenuation and policy trace.

### 11.6 Ingress source and subscription resolution

Before an ingress binding becomes active, Fabric resolves its source selector to an exact canonical provider resource and verifies that the grant chain permits observation of the declared event classes and delivery to the declared subscriber. Runtime events must match the stored connector, source resource, tenant, and event scope; an incoming payload cannot route itself to a broader subscriber or channel.

Provider configuration changes, channel moves, mailbox aliases, webhook rotations, and connector re-registration create a new binding version or require explicit revalidation. Historical observations retain the binding and resolver versions that governed admission.

### 11.7 Communication audience and channel resolution

For outbound communication, the protected resource is not merely an API endpoint. Resolution binds:

```text
provider and tenant
channel/mailbox/room/thread
exact recipients or structurally bounded audience
sender identity presented
reply target
visibility/publication scope
content disclosure class
provider operation: send, edit, delete, react, broadcast
```

Authorization evaluates this canonical communication resource. A caller cannot request a direct reply and then widen it to a group or public channel through generated prose or connector parameters.

Inbound identity assurance is a separate result. A provider account may map to a known principal through platform policy, remain an external assertion, or require application-level confirmation.

### 11.8 Approval

Approval is a signed grant with:

- exact intent hash or explicit action/resource scope;
- accountable principal and approver;
- purpose restriction;
- expiry and use count;
- policy and resolver version;
- optional separation-of-duties conditions.

A conversational “yes” is not sufficient until authenticated and transformed into this record.

### 11.9 Policy administration

Policy uses a closed, statically validated grammar over:

- principal and actor classes;
- delegation depth and source;
- purpose classes;
- effect classes;
- resource owner, namespace, and sensitivity;
- time, budget, location, and platform context;
- approval and isolation requirements;
- connector properties and reconciliation support.

Avoid arbitrary executable predicates in the policy language.

---


### 11.10 Identity-aware authorization

Before relying on an external identifier, Fabric verifies:

```text
identifier method and canonical form
current or historical key state
proof purpose
signature or proof suite
binding to local principal
validity, rotation, revocation, and recovery status
requested audience and resource
replay and freshness constraints
```

An identity binding answers “which local principal is represented under this assurance?” It does not answer “what may that principal do?” Authorization still evaluates grants and exact resources independently.

### 11.11 Self-initiated effects

A purpose reference such as `goal/G`, `initiative-decision/I`, or `mission/M` is useful causal context. It never widens the grant. Platform policy may impose stronger approval, spending, communication, or disclosure rules on unsolicited or self-initiated actions.

### 11.12 Acting capacity and information-use evaluation

Final authorization may require an explicit acting capacity, current role or delegation evidence, and an `InformationUseEnvelope`. The evaluator checks exact audience, purpose, retention-trigger, export, and disclosure constraints in addition to ordinary resource permission.

A self-improvement, commissioning, or recovery label never widens authority. The operation is authorized against the currently accepted policy and active principal, not rules proposed by the candidate or requester.

## 12. Boundary lifecycles, idempotency, and reconciliation

### 12.1 Lifecycle

```text
Submitted
  → Resolving
  → AwaitingApproval
  → Authorized
  → Denied
  → DispatchPending
  → Running
  → Succeeded
  → Failed
  → Canceled
  → Uncertain
  → Reconciling
  → ReconciledSucceeded
  → ReconciledFailed
  → Compensating
  → Compensated
  → Escalated
```

The intent is durable at `Submitted`, before exact resolution or authorization. Not every effect supports every later state, and state transitions are appended rather than rewriting the original request.

### 12.2 Idempotency semantics

```rust
pub enum IdempotencySemantics {
    Pure,
    NaturallyIdempotent,
    ExternallyKeyed { key_location: KeyLocation },
    ReadWithVersion,
    Compensatable { compensation: CapabilityId },
    Reconcilable { query: CapabilityId },
    NonRepeatable,
}
```

The connector descriptor declares the supported semantic class, commit-point behavior, and required external-key behavior.

### 12.3 Identity and idempotency table

| Identity | Stable across | Purpose |
|---|---|---|
| `ClientRequestId` | retries of one application-to-Fabric submission | Idempotent Fabric admission; same ID plus same digest returns the existing intent, while a digest mismatch conflicts. |
| `EffectIntentId` | resolution, authorization, queue admission, attempts, reconciliation | Domain identity of the accountable proposed effect. |
| `AdmissionKey` | retries of WorldInterface's dispatch outbox | Prevent duplicate ActionQueue task admission. |
| ActionQueue `RunId` | executor retries for one scheduled occurrence | Stable execution identity available to the connector runtime. |
| ActionQueue `AttemptId` | one handler invocation only | Forensics and attempt accounting; never an external replay key. |
| external idempotency key | provider retries/reconciliation as declared by connector | Prevent or detect duplicate external commit. |

The external key is selected by WorldInterface from the intent and connector protocol. It may be derived from `EffectIntentId`, from `RunId`, or from a provider-specific reservation, but the choice is explicit and recorded. It is never inferred from `AttemptId`.

### 12.4 Intent and resolution records

The immutable intent includes the unresolved selector, requested effect, verified agency, stable idempotency identity, input references, expected outcome, deadline, budget, and receipt profile.

Separate append-only records contain:

- canonical resolved effect and digest;
- resolver and registry-snapshot versions;
- authorization decision and envelope;
- ActionQueue admission key, task, run, and attempts;
- connector commit-point observations;
- receipt, uncertainty, and reconciliation records.

A materially changed operation receives a new intent linked as a revision or replacement.

### 12.5 Dispatch and attempt identity

```text
EffectIntent E
  ├── AQ Task T / Run R
  │     └── Attempt A1: worker crashed, commit possible
  └── Reconciliation Q1: provider reports success
```

A second execution attempt is not scheduled while the effect is uncertain unless connector semantics prove it safe. The ActionQueue handler returns or enters an `Awaiting` continuation for reconciliation; ActionQueue does not receive an `Uncertain` domain state.

### 12.6 Connector commit point

Connectors expose the strongest available indication of when external commit may have happened.

Examples:

- before sending a network request: commit impossible;
- after receiving a provider request ID: an external operation may exist;
- after atomic file rename: local mutation complete;
- after remote transaction commit response: succeeded, subject to provider semantics.

This supports accurate timeout and cancellation classification.

### 12.7 Reconciliation

Reconciliation may:

- query by external idempotency key;
- inspect current resource version;
- retrieve provider operation status;
- compare expected and observed state;
- ask an operator for evidence;
- execute a declared compensation;
- leave the effect unresolved with an escalation record.

It produces a reconciliation receipt linked to the original intent, ActionQueue run and attempts, and any provider operation IDs.

### 12.8 Durable callback and signal path

Remote callbacks, worker events, and provider status updates enter WorldInterface through an authenticated domain ingress:

```text
callback
  → authenticate source
  → validate schema and tenant
  → deduplicate
  → persist authoritative domain event
  → update effect/reconciliation state
  → write signal-outbox entry
  → admit AQ SignalEnvelope(payload_ref = stored event)
```

The `SignalEnvelope` contains only bounded structural routing, correlation, deduplication, and an opaque data reference. A resumed handler reloads the WorldInterface record and revalidates current state. Direct untrusted signal injection is not a shortcut around Fabric ingress.

### 12.9 Non-repeatable effects

A non-repeatable operation requires one or more of:

- exact human approval;
- connector-specific preflight;
- external idempotency support;
- no automatic retry;
- mandatory post-attempt reconciliation;
- stronger isolation and logging.

### 12.10 File effects

A file mutation should use expected version or explicit overwrite semantics where possible:

```text
intent:
  write resource F
  expected prior hash H1
  desired content artifact A

outcome:
  succeeded, resulting hash H2
or
  failed, version conflict
or
  uncertain, storage commit status unknown
```

### 12.11 Workflow effects

Each boundary node has its own effect intent, exact authorization, ActionQueue execution identity, and outcome. The flow's terminal state derives from node outcomes and declared branch/error policy. A flow cannot hide an uncertain node by merely marking the run failed.

### 12.12 Ingress binding and observation lifecycles

An ingress binding has its own lifecycle:

```text
Proposed → Resolving → AwaitingApproval → Active → Suspended / Revoked / Expired
```

Only `Active` bindings admit and route new observations. Previously admitted observations remain historically valid after suspension or revocation.

The observation and delivery lifecycle is:

```text
Received
    → Validating
    → Stored
    → DeliveryPending
    → DeliveredToSubscriber
    → Acknowledged
```

Side branches include:

```text
Duplicate
RejectedAuthentication
RejectedSchema
Quarantined
GapDetected
RedactionApplied
RetentionExpired
```

`Acknowledged` means the subscriber confirmed durable ingestion of the reference. It does not mean the subscriber accepted the content as true or acted on it.

### 12.13 Outbound communication delivery lifecycle

A communication effect may expose provider states beyond connector success:

```text
EffectIntent Submitted
    → Authorized
    → ProviderSubmissionRunning
    → ProviderAccepted
    → Delivered, if supported
    → Read/Acknowledged, if supported and requested
```

Failure and ambiguity states include:

```text
Denied
FailedBeforeProviderCommit
Uncertain
Reconciling
RejectedByProvider
Bounced
Expired
Superseded
```

Provider acceptance is not necessarily end-user delivery. Receipt profiles declare which milestone was established and which remains unknown. An uncertain send is reconciled by provider message ID, client idempotency key, or channel history before any retry.

---

## 13. Receipts, traces, evidence, and telemetry

### 13.1 Boundary receipt

```rust
pub enum BoundaryDirection {
    Ingress,
    Egress,
}

pub struct ReceiptEnvelope {
    pub id: ReceiptId,
    pub schema_version: ReceiptSchemaVersion,
    pub direction: BoundaryDirection,
    pub causal_refs: Vec<CausalRef>,
    pub producer: ConnectorInstanceId,
    pub started_at: Timestamp,
    pub finished_at: Timestamp,
    pub content_hashes: Vec<ContentHash>,
    pub evidence: Vec<EvidenceRef>,
    pub signature: Option<Signature>,
}

pub enum BoundaryReceiptBody {
    Ingress {
        binding_id: IngressBindingId,
        observation_id: IngressObservationId,
        canonical_source: CanonicalResourceRef,
        accountable_principal: PrincipalId,
        subscriber_deliveries: Vec<DeliveryId>,
        provider_event_id: Option<ExternalEventId>,
        asserted_sender: ExternalPrincipalAssertion,
        asserted_audience: Vec<ExternalPrincipalAssertion>,
        assurance: AuthenticationAssurance,
        validation: IngressValidationOutcome,
    },

    Egress {
        effect_intent_id: EffectIntentId,
        agency: AgencyContext,
        authorization: AuthorizationSummary,
        resolved_effect: ResolvedEffectSummary,
        attempt: AttemptIdentity,
        executor: ExecutionAttribution,
        outcome: EffectOutcome,
        output_refs: Vec<ArtifactRef>,
        external_refs: Vec<ExternalOperationRef>,
    },
}

pub struct BoundaryReceipt {
    pub envelope: ReceiptEnvelope,
    pub body: BoundaryReceiptBody,
}
```

Flow and node references are optional causal fields, not the primary identity of the receipt. The common envelope establishes provenance, integrity, and causal linkage; the body preserves the distinct attribution and lifecycle semantics of ingress and egress.

`AttemptIdentity` exists only for egress execution and retains all relevant scheduling identities:

```rust
pub struct AttemptIdentity {
    pub admission_key: AdmissionKey,
    pub task_id: ActionQueueTaskId,
    pub run_id: ActionQueueRunId,
    pub attempt_id: ActionQueueAttemptId,
    pub external_idempotency_key: Option<IdempotencyKey>,
}
```

These fields explain scheduling and replay. `effect_intent_id` remains the semantic identity of an egress action; `observation_id` remains the semantic identity of an ingress observation.

### 13.2 Receipt profiles

Use profiles to avoid one bloated universal payload:

- `IngressObservationReceipt` for provider, webhook, stream, and communication ingress;
- `ReadObservationReceipt` for principal-initiated reads and external observations;
- `MutationReceipt` for state changes;
- `ControlReceipt` for sandbox/worker lifecycle;
- `AuthorizationReceipt` for grants and approvals if separately materialized;
- `ReconciliationReceipt` for uncertain-effect resolution;
- `PublicationReceipt` for semantic publish/send/deploy operations.

All share a common envelope and direction-appropriate attribution semantics. Only egress receipts carry executable authorization and attempt identity; ingress receipts carry binding, source, assertion, assurance, and subscriber-delivery provenance.

### 13.3 Ingress receipt and communication delivery evidence

An ingress receipt proves:

```text
which ingress binding, connector, canonical source, and provider delivered the observation
provider event/deduplication identity
channel/thread/reply provenance
asserted sender/audience and authentication assurance
content and attachment hashes or protected references
receive time and provider-observed time
validation, quarantine, or redaction outcome
```

It does not certify semantic truth or application acceptance.

Outbound communication receipts distinguish:

```text
request accepted by Fabric
provider accepted submission
provider reported delivery
provider reported read or acknowledgment
outcome remains uncertain
```

A later provider status update appends a linked receipt or delivery-evidence record; it does not mutate historical evidence in place.

### 13.4 Workflow trace

Workflow trace events record orchestration facts:

```text
branch X evaluated true using value V
node Y became eligible after dependencies A and B
pure transform Z produced hash H
node Q was skipped by declared condition
flow entered blocked state awaiting approval
```

They are useful for replay but do not claim an external boundary crossing.

### 13.5 Evidence references

A receipt may become evidence for another system. It should identify:

- resource versions;
- provider operation IDs;
- signed attestations;
- observed status;
- content-addressed outputs;
- connector and resolver versions.

Evidence admission policy belongs to the consuming principal, not WorldInterface.

### 13.6 Operational telemetry

Telemetry may include:

- connector logs;
- subprocess output;
- WASM fuel consumption;
- HTTP timing phases;
- sandbox process and file events;
- worker token/tool streams;
- scheduler diagnostics.

It is retained separately with tighter quotas and shorter defaults. A receipt may link to a telemetry archive for forensic drill-down.

### 13.7 Hierarchical sandbox receipt

```text
SandboxRunReceipt
  sandbox grant and policy hash
  accountable principal and delegated run
  input mounts and versions
  summarized processes and network destinations
  semantic policy violations
  exported artifact manifest
  final delta hash
  mediated receipt references
  telemetry archive reference
  cleanup outcome
```

This balances auditability with manageable history.

### 13.8 Privacy

- never place raw credentials in receipts;
- default to parameter and payload hashes;
- store protected content in encrypted artifacts;
- support field-level redaction and tenant policy;
- keep access logs for receipt and artifact inspection;
- permit lawful deletion while retaining non-sensitive causal tombstones.

### 13.9 Receipt immutability and corrections

Receipts are immutable. If later reconciliation changes the understood outcome, append a new reconciliation receipt that supersedes the prior status for current interpretation while preserving history.

---


### 13.10 Signed receipts and proof profiles

Receipt signing is profile-dependent but normative for centralized and federated deployments.

```text
EmbeddedLocal
    content-addressed and tamper-evident local receipt;
    optional application signature.

PlatformSigned
    platform service signs canonical receipt;
    key-state snapshot is retained.

ActorAndPlatformSigned
    actor signs request; platform signs execution receipt.

Witnessed
    provider, counterparty, or verifier adds independent evidence.

Transparent
    one or more signed artifacts receive a transparency receipt.
```

A signature over a receipt proves who signed the receipt. Provider evidence and reconciliation establish what the executor could observe about the outcome.

### 13.11 Disclosure and completeness profiles

```text
LocalOnly
DirectDisclosure
SelectiveDisclosure
PrivateSharedLog
BatchAnchored
DomainTransparent
Public
```

A disclosed receipt or Merkle inclusion proof does not assert that the principal disclosed every relevant action. A `CompletenessClaim` is explicit, scoped, time-bounded, and backed by the applicable log policy.

## 14. Agency and accountability

### 14.1 “Who did it?” has multiple answers

WorldInterface should distinguish:

- **causal origin:** goal, commitment, process, or work order;
- **requester:** immediate actor that asked Fabric to act;
- **accountable principal:** entity that owns the authority and consequences;
- **authorizer:** grant or policy that permitted execution;
- **approver:** human or institutional principal that issued an exceptional release;
- **executor:** connector, worker, host, or platform node that physically performed the operation.

### 14.2 Direct Vessel action

```text
causal origin: commitment/C
requester: actor/exoskeleton-host-V
causal origin also references: faculty/executive + decision/D
accountable principal: vessel/V
authorizer: grant/G
executor: connector/filesystem-1
```

### 14.3 Delegated action

```text
causal origin: work-order/W under commitment/C
requester: delegated-run/R
accountable principal: vessel/V
delegation chain: V → D → R
authorizer: attenuated grant/G
approver: human/K, if required
executor: connector/github-2
```

### 14.4 Workflow action

```text
causal origin: business-process/invoice-payment
requester: workflow-run/42
submitted_by: vessel/accounts-payable
accountable principal: organization/netcorp
authorizer: organization-policy/P
executor: connector/payment-provider-1
```

### 14.5 Inbound asserted identity

For inbound communication, “who sent it?” may have several layers:

```text
transport peer
provider tenant/account
provider-authenticated user or bot ID
message-level asserted From/sender field
application principal binding
confidence and verification evidence
```

WorldInterface records the first four as available, links the observation to the authorized ingress binding that permitted listening and delivery, and applies platform mappings only when policy justifies them. Exoskeleton may bind the assertion to a known principal, retain uncertainty, or request confirmation. A display name is never sufficient to impersonate a privileged principal.

### 14.6 Authentication

Embedded mode may bind principal identity at host startup and issue local in-process capability handles.

Centralized mode requires:

- authenticated service or user identity;
- short-lived signed tokens or mutual TLS;
- audience and purpose binding;
- delegation chain validation;
- replay protection;
- revocation and expiry;
- tenant and organization binding.

Payload identity fields are treated as claims to validate, not truth.

### 14.7 Responsibility without flattening agency

A principal remains responsible for authorized delegated actions, but the receipt should not erase the delegate's role. This enables meaningful organizational audit, worker performance evaluation, and investigation of compromised subordinate components.

### 14.8 Principal transfer and organizational authority

If an organization is the accountable principal and a Vessel acts as a member or service, the grant chain should establish that relationship explicitly. A Vessel cannot declare itself to act on behalf of an organization without an organizational grant.

---


### 14.9 Portable identity foundation SPI

WorldInterface should expose protocol-neutral interfaces:

```rust
pub trait IdentityResolver {
    fn resolve(
        &self,
        identifier: &ExternalIdentifier,
        at: Timestamp,
        purpose: VerificationPurpose,
    ) -> Result<ResolvedIdentityState>;
}

pub trait SigningProvider {
    fn sign(
        &self,
        principal: PrincipalId,
        statement: CanonicalStatement,
        policy: SigningPolicy,
    ) -> Result<SignedStatement>;
}

pub trait TransparencyPublisher {
    fn publish(
        &self,
        statement: SignedStatementRef,
        policy: PublicationPolicy,
    ) -> Result<TransparencyReceipt>;
}
```

The SPI may be implemented locally, by hardware-backed custody, by a remote wallet or agent, by an organizational service, or by EntityID. WorldInterface policy determines which implementation may satisfy which assurance class.

### 14.10 Key custody and lifecycle

Long-lived principals require rotation, revocation, recovery, threshold control, delegated keys, historical verification, and retirement. Root keys stay outside connector memory and specialist sandboxes. A delegated worker receives a short-lived, work-order-bound credential with exact capabilities and expiration.

Identity mutation follows intent-first execution:

```text
IdentityOperationSubmission
    → durable EffectIntent
    → approval and exact identity-state resolution
    → constrained SigningProvider / resolver operation
    → signed receipt or Uncertain
    → reconciliation and updated IdentityBinding
```

### 14.11 What a signed statement establishes

Fabric inspection must distinguish:

```text
Authorship evidence
    applicable key signed exact canonical bytes.

Authorization evidence
    principal possessed an applicable grant at decision time.

Executor evidence
    WorldInterface attempted or performed the operation.

Provider or witness evidence
    another party corroborated an outcome.

Transparency evidence
    a signed artifact was registered under a log policy.

Semantic truth
    remains a relying-party or application judgment.
```

### 14.12 Sponsorship, fees, and bonded resources

A sponsor may pay fees, provision compute, or stake a resource while the actor remains cryptographically attributable. Receipts record both. Cost never becomes a substitute for competence, benevolence, unique identity, or truth.

### 14.13 Contextual trust attestations

A trust or reputation statement is a signed assertion by an issuer about a subject, scope, evidence, period, and confidence. Fabric verifies provenance and status, then delivers it as evidence. It does not maintain a universal reputation score.

### 14.14 Bonded commitments

A bond capability can make terms, stake, resolver, deadlines, and resolution evidence externally legible. WorldInterface does not decide whether the underlying Exoskeleton commitment has been fulfilled; it executes the bond protocol and preserves its evidence.

### 14.15 Privacy, correlation, and completeness

Portable identity can increase correlation risk. WorldInterface should support pairwise identifiers, persona-specific bindings, selective disclosure, blinded or batched commitments, and protected artifacts. Public proof is never the default merely because a public protocol is available.

### 14.16 EntityID integration profile

EntityID is treated as an early, optional profile:

- `ExternalIdentifier.method = EntityId`;
- an EntityID resolver supplies key-state and control-lineage evidence;
- an EntityID signer or wallet produces canonical statements;
- optional EntityRep maps to contextual signed attestations;
- optional EntityBond maps to bonded-commitment capabilities;
- sponsorship maps to `EconomicSponsorship`;
- any ledger, token, consensus, or public-transparency mechanism remains adapter-specific.

Fabric conformance must remain possible with no EntityID implementation installed.

### 14.17 Acting capacity and role attestations

The agency chain may include an `acting_capacity_ref` and independently verifiable role or stewardship evidence. The local principal remains accountable; the role limits the capacity under which the action is authorized.

### 14.18 Information obligations

Boundary receipts record the information-use policy that governed an observation, export, disclosure, worker handoff, proof publication, or artifact release. WorldInterface enforces the crossing; it does not infer social consent or internal memory policy.

### 14.19 Developmental provenance

Candidate source, build, test, trial, activation, and rollback records form a proof bundle with distinct issuers and claims. A signed build or green test result is not a self-certifying improvement verdict.

### 14.20 Lifecycle accountability

Restore, migration, succession, fork, suspension, quarantine, and retirement operations identify requester, accountable principal, acting capacity, authorizer, lifecycle supervisor, source state, target runtime, and outcome.

## 15. Contract with Exoskeleton

### 15.1 Responsibilities

**Exoskeleton owns:**

- why an action or delegation exists;
- commitments and plans;
- faculty arbitration;
- local constitutional admissibility and requested authority bounds;
- work-order construction;
- delegate supervision;
- verification and acceptance;
- belief and relationship updates.

**WorldInterface owns:**

- authentication and verified `AgencyContext` construction;
- canonical resource resolution;
- final executable authorization and approval validation;
- durable effect intent and dispatch outbox;
- boundary execution;
- connector and sandbox enforcement;
- effect idempotency and reconciliation;
- immutable receipts;
- deterministic Flow execution when submitted.

An Exoskeleton `CapabilityDecisionRecord` is necessary local evidence but is not a WorldInterface `AuthorizationEnvelope`. In embedded mode the same deployment may host both evaluators; their records and responsibilities remain distinct.

### 15.2 Submission contract

Exoskeleton sends an `EffectSubmission` or `FlowSubmission` containing:

- transport-authenticated Vessel or organizational session;
- requested purpose and causal references;
- delegation credential when a subordinate worker is the requester;
- effect class and resource selector;
- local capability-decision reference and requested authority bounds;
- grant references or approval attestations available to evaluate;
- input artifact references;
- requested receipt profile.

WorldInterface constructs or returns:

- verified `AgencyContext`;
- durable intent and execution identity;
- canonical resolution record;
- final authorization, denial, or approval requirement;
- asynchronous status updates;
- boundary receipts;
- evidence and artifact references;
- explicit uncertainty and reconciliation state.

Payload identity and grant fields are claims to verify. Exoskeleton does not transmit a self-minted executable envelope as truth.

### 15.3 Direct file access

WorldInterface remains appropriate for a Vessel's direct access to user or shared files because the observed or mutated resource version may matter to causal history. It is not used for Exoskeleton's internal SQLite, artifact-index, or WAL mechanics.

### 15.4 Delegated sandbox activity

WorldInterface receipts sandbox issuance and accountable perimeter crossings. It does not need to mediate every delegate file read, code search, compiler invocation, or temporary write unless the selected execution mode is fully mediated.

### 15.5 Worker-control lifecycle

WorldInterface executes worker-control effects; Exoskeleton interprets worker events.

```text
Exoskeleton:
  decide worker progress is inadequate
  commit SteeringCommand request + outbox

WorldInterface:
  persist worker.command EffectIntent
  resolve worker resource and authorize
  deliver command to Agent Host
  return ControlReceipt or uncertainty

Agent Host / WorldInterface ingress:
  authenticate and store worker response or progress event
  emit structural wake signal by outbox

Exoskeleton:
  load normalized stored event
  revise plan, work order, or commitment
```

A worker-control crossing receives a boundary receipt. High-volume progress reports are delegation trace/evidence records unless they themselves exercise a protected capability.

### 15.6 Embedded and remote parity

The Exoskeleton host depends on a client trait implemented by both embedded Fabric and remote Platform:

```rust
#[async_trait]
pub trait EffectFabricClient {
    async fn submit_effect(
        &self,
        submission: EffectSubmission,
    ) -> Result<EffectHandle>;

    async fn submit_flow(
        &self,
        submission: FlowSubmission,
    ) -> Result<FlowHandle>;

    async fn inspect_effect(
        &self,
        id: EffectIntentId,
    ) -> Result<EffectStatus>;

    async fn reconcile(
        &self,
        id: EffectIntentId,
    ) -> Result<ReconciliationHandle>;
}
```

`submit_plan` is intentionally absent from the ordinary client. `ExecutionPlan` is compiler/runtime output and accepting it is a privileged internal API. Behavioral and security conformance tests run against both embedded and remote implementations.

### 15.7 Communication contract

WorldInterface provides Exoskeleton:

```text
IngressBinding reference, BoundaryObservation, and ingress receipt
provider/channel/thread/message/reply identifiers
asserted sender/audience and assurance metadata
content/attachment artifact references
edit/delete/reaction/status relations
outbound canonical audience resolution
delivery milestone receipts and uncertainty
```

Exoskeleton provides WorldInterface for outbound communication:

```text
EffectSubmission
verified host transport identity
requested accountable principal
purpose and causal references
structural audience/channel selector
content artifact reference
communicative act and disclosure class
local approval attestations and authority bounds
required receipt milestone
```

WorldInterface does not receive or define `ConversationId`, `AttentionDemandId`, `ResponseObligationId`, `RelationshipId`, or the semantic meaning of a promise. Those stay in Exoskeleton and are carried only as generalized causal or purpose references.

For egress, Exoskeleton maps one immutable `CommunicationIntentId` to one stable `ClientRequestId`. Fabric admission is idempotent by that request ID plus canonical submission digest; a materially different audience, content artifact, conversation version, or requested delivery milestone conflicts and requires a new communication intent.

### 15.8 Contract with ActionQueue

WorldInterface requires `AQ-CONT-1` for its target asynchronous path:

- `ensure_task` with the persisted stable `EnsureTaskRequest` / `AdmissionKey`; its CanonicalAdmissionV2 digest is computed for outbox delivery;
- compound task/run/dependency admission;
- `Awaiting` with checkpoint and deadline;
- durable early-arriving signals with deduplication;
- `ResumeContext` containing the matched signal and data reference;
- opaque causal-reference propagation.

WorldInterface remains the authoritative source for effect, authorization, callback, receipt, and uncertainty meaning. ActionQueue stores only queue lifecycle and opaque references.

---


### 15.9 External personas, signed statements, and narrative separation

Exoskeleton may submit:

- a local `VesselId`-backed accountable principal;
- an `ExternalPersonaId` and requested identity binding;
- a signing or disclosure request;
- a goal, commitment, initiative decision, communication intent, or narrative evidence reference as generalized purpose context.

WorldInterface returns authoritative identity-binding, signing, publication, attestation, or bond records. Exoskeleton decides what those records mean for identity, relationship, commitment, and narrative.

The contract is explicit:

```text
VesselId is not ExternalIdentifier.
ExternalIdentifier is not unique personhood.
SignedStatement is not semantic truth.
Boundary proof history is not autobiography.
Mission or initiative origin is not authorization.
```

### 15.10 Vessel instantiation and commissioning

Exoskeleton commits `VesselId`, Blueprint/Manifest references, `GenesisRecord`, Purpose Charter, roles, stewardship, and commissioning outbox before calling WorldInterface. WorldInterface performs only the bounded external setup and returns evidence. It cannot create or amend internal genesis.

### 15.11 Role and acting-capacity contract

Exoskeleton owns internal `RoleAssignment` meaning and selects the acting capacity for a mission, commitment, communication, delegation, or effect. WorldInterface verifies external attestations and applies exact resource policy without ambiently unioning roles.

### 15.12 Information-use contract

Exoskeleton owns whether information may enter memory, support a belief, be derived from, or be reused for another purpose. WorldInterface enforces disclosure, audience, export, proof-publication, and artifact-release constraints at the boundary.

### 15.13 Governed improvement contract

Exoskeleton owns `ImprovementProposal`, frozen `EvaluationPlan`, acceptance, and continuity assessment. WorldInterface owns candidate-development effects, build and trial provenance, lifecycle-supervisor operations, activation/rollback receipts, and uncertainty.

### 15.14 Lifecycle contract

Exoskeleton determines whether an event represents continuation, succession, or fork and preserves internal obligations. WorldInterface executes and evidences the requested checkpoint, migration, restore, quarantine, retirement, persona, key, or credential operation.

## 16. Platform and NetCorp

### 16.1 Platform is a control plane around Fabric

The centralized variant needs:

- principal authentication and directory;
- organizations, tenants, roles, and service identities;
- policy and grant administration;
- secret brokerage and scoped credential issuance;
- quotas, rate limits, and concurrency policy;
- shared connector pools;
- audit visibility and access control;
- signed receipts;
- high availability and disaster recovery;
- platform-wide reconciliation and incident handling.

None of these require a second effect runtime.

### 16.2 Tenant isolation

- all resources, intents, plans, receipts, artifacts, and connector credentials are tenant-scoped;
- cross-tenant references are denied unless an explicit federation grant exists;
- connector instances may be dedicated, pooled by trust class, or isolated per tenant;
- logs and metrics avoid leaking resource or principal identifiers;
- administrative inspection is itself auditable.

### 16.3 Secret brokerage

Principals and delegates should not receive long-lived platform credentials.

```text
principal requests bounded effect
    → Platform authorizes purpose/resource
    → connector receives short-lived destination-bound credential
    → credential use appears in receipt
```

For hybrid delegates, secrets are normally exposed only through mediated capabilities rather than injected into the sandbox.

### 16.4 Organizational policy

NetCorp may define:

- role-based parent grants;
- separation of duties;
- approval thresholds;
- resource ownership;
- allowed workflow templates;
- connector and secret tenancy;
- retention and legal holds;
- organization-wide emergency stops.

A Vessel's local constitution can further narrow organizational authority. It cannot enlarge the parent organizational grant.

### 16.5 Multi-Vessel coordination

WorldInterface Flow can coordinate deterministic organizational handoffs around open-ended Vessel cognition:

```text
assign work to Vessel A
  → wait for signed completion proposal
  → request verification from Vessel B
  → require approval from Human H
  → publish through Fabric
```

The flow records ordering and boundary effects. Each Vessel preserves its own cognitive and commitment history.

### 16.6 Platform receipt

Platform receipts should be signed and include:

- organization and tenant;
- accountable principal;
- authenticated requester;
- delegation and submission chain;
- policy and grant versions;
- executor node and connector instance;
- canonical resource and external references;
- artifact locations under access control.

### 16.7 Organizational communication fabric

Platform may provide shared authenticated channels, mailboxes, webhooks, streams, and Vessel-to-Vessel routing. Its responsibilities are transport identity, tenancy, organization policy, delivery, retention, and audit access.

It must not become the organization's single conversation memory or cognitive arbiter. Individual Vessels and applications retain semantic interaction state. Organizational workflows can require acknowledgments, approvals, or separation of duties, but ordinary conversation cannot silently satisfy those structural controls.

### 16.8 Federation

Future federation between WorldInterface platforms should treat each platform as an accountable service principal. Delegated authority crosses the boundary through signed attenuated grants. Do not merge tenant namespaces or trust receipt claims without signature and policy verification.

---


### 16.9 Portable identity and federation

NetCorp may bind human, Vessel, organizational, service, and delegated-worker principals to different external identity methods. Federation validates signed attenuated grants and historical key state at each boundary. A Platform receipt identifies the issuing platform and does not become trusted merely because it is signed.

### 16.10 Collective history and mythos remain above Fabric

Platform may preserve shared receipts, communications, signed statements, and workflow history. It does not synthesize an official organizational narrative or mythos. A future NetCorp narrative subsystem can cite Fabric evidence while preserving authorship, authority, audience, and dissent as separate concepts.

### 16.11 Organizational roles and separation of duties

Platform may verify organization-issued role and stewardship attestations, enforce role-specific resource policy, and require distinct principals for proposal, review, authorization, execution, or activation. It does not define one universal organizational role ontology.

### 16.12 Shared lifecycle services

A NetCorp deployment may centralize candidate builders, artifact registries, lifecycle supervisors, backup services, and activation policy administration. Those remain accountable Platform services around the common Fabric protocol rather than a forked WorldInterface runtime.

## 17. Crate and dependency refactor

`WI-FABRIC-2` is built directly against `AQ-CONT-1`. WorldInterface domain crates never depend on Exoskeleton, and no compatibility layer targets the current ActionQueue API.

### 17.1 Target logical structure

```text
worldinterface-protocol
    generalized IDs, agency, submissions, intents, observations,
    authorizations, receipts, artifacts, protocol versions

worldinterface-capability
    effect classes, descriptors, resource scopes, idempotency,
    connector contracts, resolvers

worldinterface-runtime
    intent persistence, exact authorization, connector registry,
    ActionQueue integration, execution, receipts, uncertainty, reconciliation

worldinterface-ingress
    provider admission, binding resolution, deduplication,
    observation receipts, subscriber outbox and acknowledgment

worldinterface-flow-model
worldinterface-flow-compiler
worldinterface-flow-coordinator
    optional deterministic Flow layer above Fabric

worldinterface-contextstore
    durable outputs and artifact publication

worldinterface-wasm
    bounded connectors, pure transforms, and verifier runtime

worldinterface-identity-spi
worldinterface-proof
    optional protocol-neutral identity, signing, proof, disclosure,
    attestation, and bond envelopes

worldinterface-embedded
worldinterface-daemon
worldinterface-platform
worldinterface-cli
```

The initial codebase may enforce these as modules before promoting every boundary to a crate. Dependency direction is normative:

```text
protocol ← capability ← runtime ← flow-coordinator
protocol ← flow-model ← flow-compiler ─┘
protocol ← identity-spi / proof
runtime ← ingress
```

Fabric runtime does not depend on Flow. Protocol does not depend on Exoskeleton, an EntityID SDK, a blockchain, or a wallet.

### 17.2 Current-to-target crate disposition

| Current crate or surface | Target disposition |
|---|---|
| `worldinterface-core` | Split into protocol, capability, identity/proof envelopes, and flow-model types; delete flow-centric ownership from the core. |
| `worldinterface-flowspec` | Rebuild as `flow-model` plus `flow-compiler`; Flow remains optional above Fabric. |
| `worldinterface-contextstore` | Retain as target output/artifact publication with explicit versioning and completion semantics. |
| `worldinterface-connector` | Split connector SPI, capability descriptors, resource resolvers, and runtime execution/receipt logic. |
| `worldinterface-coordinator` | Rebuild as ActionQueue-backed `flow-coordinator`; remove blocking wait connectors. |
| `worldinterface-host` | Become `embedded` composition plus runtime host; no direct Exoskeleton dependency. |
| `worldinterface-http-trigger` | Move into `worldinterface-ingress` as authenticated, bound, deduplicated observation admission. |
| `worldinterface-daemon` | Rebuild as authenticated Fabric/Flow service; Platform control-plane features remain optional. |
| `worldinterface-cli` | Replace command surface with target observation, effect, flow, worker, identity, and inspection APIs. |
| `worldinterface-wasm` | Retain and align with target capability, transform, verifier, resource, and receipt contracts. |
| current connector catalog | Apply the complete Section 19 disposition; no old connector-name adapter remains. |

### 17.3 Current concept replacement

| Current concept | Target |
|---|---|
| flow-centric receipt | direction-aware `BoundaryReceipt` with optional flow causal refs |
| flow-centric invocation context | verified agency + exact authorization + intent + optional flow refs |
| descriptor booleans | `CapabilityDescriptor` + explicit `IdempotencySemantics` |
| synchronous `Connector::invoke` | target resolve/invoke/reconcile connector contract |
| `invoke_single` | delete as cognitive control path; direct submissions compile to one-operation `ExecutionPlan` |
| FlowSpec in core | optional Flow model above protocol/runtime |
| trusted-network daemon | explicit authenticated profiles; local development profile remains visibly non-production |
| coding-specific connectors | sandbox, verifier, generalized boundary capability, or deletion per Section 19 |

### 17.4 Conformance ownership

WorldInterface owns `conformance/wi-fabric-2`. Its manifest pins an exact `aq-cont-1` conformance revision and records the ActionQueue release used. It contains embedded and remote black-box fixtures for ingress, effect intent, authorization, receipts, uncertainty, Flow convergence, worker controls, and portable-identity neutrality.

The repository may not declare `WI-FABRIC-2` compatibility until the pinned ActionQueue conformance package passes without fallback to transient events or fire-and-forget admission.

## 18. API, CLI, and operator surfaces

### 18.1 API namespaces

```text
/v2/ingress-bindings
/v2/observations
/v2/deliveries
/v2/effects
/v2/execution-plans
/v2/flows
/v2/receipts
/v2/reconciliation
/v2/capabilities
/v2/resources
/v2/authorizations
/v2/approvals
/v2/workers
/v2/platform/*
```

Ingress binding, direct effect, and flow submissions remain visibly distinct at admission while sharing identity, authority, provenance, receipt, and durability conventions.

### 18.2 Effect API

```text
POST   /v2/effects
GET    /v2/effects/{intent_id}
POST   /v2/effects/{intent_id}/cancel
POST   /v2/effects/{intent_id}/reconcile
GET    /v2/effects/{intent_id}/receipts
GET    /v2/effects/{intent_id}/trace
```

Responses should include stable IDs and polling/event links. Avoid requiring clients to block on connector duration.

### 18.3 Ingress and communication API

Illustrative namespaces:

```text
GET  /v2/observations/{id}
GET  /v2/observations/{id}/receipt
GET  /v2/observations?connector=&channel=&after=
POST /v2/ingress-bindings
GET  /v2/ingress-bindings/{id}
POST /v2/ingress-bindings/{id}/suspend
POST /v2/ingress-bindings/{id}/resume
POST /v2/ingress-bindings/{id}/revoke
POST /v2/deliveries/{id}/ack
GET  /v2/effects/{id}/delivery
```

Provider-facing webhook and stream endpoints are connector-specific but converge on the same `BoundaryObservation` admission path. Subscriber APIs return references and governed artifacts rather than embedding unrestricted sensitive content by default.

### 18.4 Flow API

```text
POST   /v2/flows/validate
POST   /v2/flows
GET    /v2/flows/{flow_id}
POST   /v2/flows/{flow_id}/runs
GET    /v2/flow-runs/{run_id}
GET    /v2/flow-runs/{run_id}/trace
GET    /v2/flow-runs/{run_id}/receipts
POST   /v2/flow-runs/{run_id}/cancel
```

Validation should report authority requirements separately from syntax and DAG validity.

### 18.5 Capability inspection

```text
GET /v2/capabilities
GET /v2/capabilities/{id}
POST /v2/capabilities/effective-view
POST /v2/authorizations/explain
POST /v2/resources/resolve
```

The effective view is principal-, purpose-, and resource-sensitive. It is advisory for clients; execution remains enforced server-side.

### 18.6 Worker API

```text
POST /v2/workers/runs
GET  /v2/workers/runs/{id}
POST /v2/workers/runs/{id}/commands
POST /v2/workers/runs/{id}/checkpoint
POST /v2/workers/runs/{id}/suspend
POST /v2/workers/runs/{id}/resume
POST /v2/workers/runs/{id}/terminate
GET  /v2/workers/runs/{id}/artifacts
```

These endpoints expose lifecycle control only. They do not claim to judge the worker's objective or completion.

### 18.7 Event delivery

Support:

- authenticated webhooks;
- server-sent events or WebSocket for inspection;
- polling with ETags and sequence IDs;
- ActionQueue/event-bus integration in embedded mode.

Delivery events are deduplicated and link to authoritative stored state.

### 18.8 CLI proposal

```text
wi observation show|list|receipt|redeliver
wi ingress-binding list|show|create|suspend|revoke
wi effect submit|show|cancel|reconcile|trace
wi receipt show|verify|export
wi capability list|show|effective|explain
wi resource resolve
wi authorization explain
wi flow validate|submit|show|trace
wi worker start|show|command|checkpoint|terminate|artifacts
wi connector list|show|health
wi policy validate|show
wi platform principal|tenant|grant|secret|audit
```

### 18.9 Operator explanations

The operator should be able to ask:

- Why was this effect allowed or denied?
- What canonical resource did the request resolve to?
- Who was accountable, who requested, and who executed it?
- Which parent grant and approval were consumed?
- Did the connector reach a commit point before failure?
- Why is the outcome uncertain?
- What reconciliation options exist?
- Which workflow node caused the effect?
- Which low-level events are telemetry rather than primary receipts?

---


### 18.10 Identity and proof API

```text
POST /api/v2/identity/bindings:resolve
GET  /api/v2/identity/bindings/{id}
POST /api/v2/identity/statements:sign
POST /api/v2/identity/statements:verify
POST /api/v2/identity/statements:publish
GET  /api/v2/identity/key-state/{ref}
GET  /api/v2/proofs/{bundle_id}
```

Mutation endpoints are disabled unless the profile configures authenticated control, signing custody, and policy. Verification endpoints return assurance and evidence, not a boolean claim that the statement is true.

### 18.11 Commissioning, information, and lifecycle API

Illustrative namespaces include:

```text
POST /v2/commissioning/operations
GET  /v2/commissioning/operations/{id}
POST /v2/information/disclosures
POST /v2/candidates/builds
GET  /v2/candidates/{id}/provenance
POST /v2/lifecycle/activations
POST /v2/lifecycle/rollbacks
POST /v2/lifecycle/quarantines
GET  /v2/lifecycle/operations/{id}
```

These endpoints are thin admission surfaces over ordinary `EffectIntent` semantics. They do not expose arbitrary deployment shell access or allow callers to mint acceptance decisions.

## 19. Clean-break implementation and connector disposition

The target is constructed directly. Current code is tagged and archived; no live flow, receipt, connector, or store requires operational cutover.

### 19.1 Baseline archive and characterization

Before deletion:

```text
tag: worldinterface/pre-wi-fabric-2
archive: representative FlowSpecs, receipts, ContextStore artifacts,
         webhook/stream fixtures, connector descriptors,
         coding benchmark cases, signal loss cases, and effect-ordering cases
```

Baseline behaviors are classified as retain, replace, or reject. Archived records remain outside the active runtime.

### 19.2 One target authority path

The target has one path for ingress and one path for egress, converging on shared agency, resource, authorization, evidence, and ActionQueue contracts. It contains no:

- dual old/new receipts;
- anonymous compatibility principal;
- old connector-name adapter;
- old receipt reader in the runtime;
- blocking `signal.await` or `delay` connector;
- process-local `SignalRegistry` correctness path;
- shadow authorization path;
- fallback from `AQ-CONT-1` admission to best-effort submission.

### 19.3 Construction sequence

1. Establish target protocol IDs, records, fresh stores, and verified agency envelope.
2. Implement accountable ingress bindings, observations, receipts, delivery outboxes, and acknowledgments.
3. Implement intent-first egress, canonical resource/audience resolution, exact authorization, and ActionQueue outbox admission.
4. Implement direction-aware receipts, workflow traces, evidence, telemetry, and completion publication.
5. Compile direct submissions and FlowSpec into the common `ExecutionPlan`.
6. Add uncertainty, commit-point semantics, and connector reconciliation.
7. Add managed worker and sandbox boundary capabilities.
8. Rationalize the connector catalog and delete specialist technique.
9. Add authenticated daemon and optional Platform control plane.
10. Add protocol-neutral identity/proof adapters only after the Fabric core is sound.

### 19.4 Complete connector disposition

| Current connector or family | Target disposition |
|---|---|
| `signal.await` | **Delete.** A Flow wait compiles to ActionQueue `Awaiting` + `WaitSpec`; no executor remains blocked. |
| `signal.emit` | **Delete.** The owning domain stores its result and admits a durable `SignalEnvelope` through an outbox. |
| in-process `SignalRegistry` | **Delete.** It is neither durable nor race-safe and is not a target service. |
| `delay` | **Delete as a connector.** Use ActionQueue scheduling, wait deadlines, or timer-backed task eligibility. |
| `code_grep`, `code_fuzzy`, `code_glob`, `code_ls`, `code_read` | Move into specialist sandbox/toolchain; retain only optional raw telemetry outside primary receipt history. |
| `code_symbol`, `code_read_symbol`, `code_references`, `code_impls` | Move to specialist backend or optional analysis service. |
| `repo_context`, `repo_locate` | Move to delegated backend or planning/evidence artifact generator. |
| `code_test` | Run natively in sandbox; independent result checking may become a verifier capability. |
| `code_git_diff` | Sandbox operation or verifier over exported patch/final delta. |
| code edit/write/apply-patch helpers | Native sandbox technique; generalized `filesystem.write` remains for mediated boundary use. |
| `gitignore_check` | Worker-local check or independent verifier; not Fabric ontology. |
| semantic Rust analyzer | Optional specialist service or verifier. |
| `shell_exec` | Replace with tightly parametric `process.execute` for mediated work; native inside a sandbox otherwise. |
| filesystem read/write/enumerate | Retain as generalized accountable capabilities for resources that cross a trust boundary. |
| HTTP request | Retain with canonical destination resolution, egress policy, credential binding, size/deadline limits, and idempotency class. |
| webhook and streaming ingress | Rebuild under `worldinterface-ingress` with binding, authentication, provider deduplication, provenance, and delivery outbox. |
| sandbox execution connector | Replace with `sandbox.provision`, `sandbox.release`, and `worker.*` lifecycle capabilities. |
| high-value semantic connectors | Retain when domain semantics materially improve authorization, reconciliation, verification, or explanation. |

Each item has only four target outcomes: retain semantic, generalize, move inward/to verification, or delete. There is no “legacy-only runtime” category.

### 19.5 Removed FlowSpec behavior

A pre-contract FlowSpec naming a deleted connector is invalid under `WI-FABRIC-2`. The frozen repository tag remains the interpreter for historical experiments. Target Flow validation reports the removed operation and its target replacement, but it does not execute it through a compatibility adapter.

### 19.6 Baseline defects become negative tests

The conformance suite proves that the target cannot reproduce:

- an emit lost because no waiter exists;
- a connector invocation occupying an executor while waiting;
- consequential execution before durable `EffectIntent`;
- a coding-specific operation entering Fabric merely because an agent wants it;
- generated application content being labeled delivered;
- an unverified caller selecting its own principal or authorization.

### 19.7 Release and conformance gate

`conformance/wi-fabric-2` pins the exact `aq-cont-1` package revision. WorldInterface publishes only after:

- all upstream admission, wait, signal, checkpoint, and crash fixtures pass;
- embedded and remote Fabric profiles pass the same target protocol fixtures;
- every built-in connector has an explicit target disposition;
- repository-wide checks find no deleted signal registry, old receipt writer, or coding-specific Fabric capability.

The resulting WorldInterface release is then the only upstream target for Exoskeleton. No transitional compatibility release is required.

## 20. Persistence, versioning, and recovery

WorldInterface starts with fresh target stores and explicit versioning. The section governs target durability and future upgrades; it does not promise to load pre-`WI-FABRIC-2` data.

### 20.1 Target persistence domains

WorldInterface may maintain:

- ActionQueue WAL and snapshots;
- principal, external-identity-binding, and key-state references;
- ingress binding, observation, delivery, and acknowledgment state;
- effect intents, resolved effects, authorization records, attempts, and reconciliation;
- ContextStore outputs and artifact manifests;
- boundary receipts and proof bundles;
- Flow definitions, compiled plans, runs, and workflow traces;
- resource registry snapshots;
- connector/platform metadata and telemetry references.

Store boundaries follow transaction and query needs, not one database per noun.

### 20.2 Egress completion protocol

A successful effect is not terminally visible until the required durable pieces converge:

```text
connector outcome
  + output/artifact publication
  + boundary receipt
  + effect-state transition
  + ActionQueue completion
```

Where stores cannot share a transaction, outboxes and idempotent recovery establish convergence. Crash tests cover every boundary.

### 20.3 Ingress admission protocol

After verifying an active binding, ingress atomically persists or recoverably converges:

- canonical source and binding references;
- content/artifact reference and hash;
- `BoundaryObservation` or duplicate-arrival link;
- ingress receipt;
- subscriber-delivery outbox;
- provider offset or acknowledgment state when applicable.

Subscriber acknowledgment remains distinct from semantic application acceptance.

### 20.4 Version everything that affects interpretation

Persist protocol, receipt, capability, connector, resolver, resource-registry, authorization, policy, FlowSpec, compiler, execution-plan, ContextStore, identity adapter, key-state, signing profile, and reconciliation versions.

### 20.5 Historical replay

Target historical replay reconstructs the submitted target intent or FlowSpec, compiled plan, exact resolution and authorization records, ActionQueue lifecycle, trace, receipts, and later reconciliation. It never re-contacts an external system and never reinterprets an old decision under a new policy without labeling that as present-day re-evaluation.

Pre-contract receipts and FlowSpecs remain in source tags or inert archives. They are not target replay fixtures and are not rendered by active runtime readers.

### 20.6 Receipt and proof verification

Centralized receipts are signed or otherwise cryptographically verifiable. Embedded profiles are at least content-addressed and tamper-evident; signing is policy-controlled. Verification states exactly whether evidence establishes authorship, authorization, execution observation, provider acknowledgment, witness corroboration, or transparency inclusion. It never reports semantic truth merely because a signature is valid.

### 20.7 Future release evolution

From the first stable target release onward, every supported release boundary includes:

- prior target fixture loading;
- source-preserving upgrade;
- partial-failure and restart recovery;
- replay and inspection;
- backup and restore;
- receipt/proof verification;
- protocol and conformance-revision matrix.

A pre-contract store fails precisely and remains untouched. A future unsupported target version also fails precisely rather than being guessed through.

### 20.8 Retention and privacy

Long-term records normally include effect intents, authorizations, boundary receipts, reconciliation, identity-binding snapshots, and proof relations. Workflow traces are policy-defined; telemetry and raw sandbox logs are shorter-lived and quota-bound. Sensitive content is minimized, encrypted, and referenced rather than copied where possible. Private keys are never stored in WorldInterface domain stores.

### 20.9 Commissioning and developmental records

The target stores persist commissioning operation state, acting-capacity evidence, information-use envelopes, candidate identities, build provenance, activation/rollback intents, lifecycle-supervisor receipts, and reconciliation outcomes. Large source trees, build logs, models, and binaries remain artifact references rather than inline database payloads.

The stores never treat activation as a metadata update on a candidate row. Candidate existence, acceptance, activation, health confirmation, and retention are separate records.

## 21. Security model

### 21.1 Security objective

Fabric should ensure that an authenticated principal or subordinate actor can exercise only explicitly granted capabilities against canonical resources, with durable attribution and evidence, even when callers, connectors, workflows, or delegated workers are faulty or malicious.

### 21.2 Threats

- caller impersonation or replay;
- forged delegation chains;
- path traversal and symlink escape;
- SSRF, DNS rebinding, and metadata access;
- connector misclassification;
- flow authority expansion;
- secret leakage;
- ambiguous remote commit and duplicate effect;
- malicious WASM or native connector;
- sandbox escape;
- cross-tenant data access;
- receipt tampering;
- workflow transform hidden I/O;
- denial of service through high-volume telemetry or flows;
- compromised administrative policy.

### 21.3 Embedded profile

Embedded does not mean unauthenticated by design. Bind the host to a configured principal and issue unforgeable local capability handles. Administrative APIs should still require explicit operator authority.

A development-only trusted mode may exist but must be labeled, disabled by default in production profiles, and reflected in receipts.

### 21.4 Centralized authentication

Use a pluggable mechanism with at least:

- mutual TLS or signed short-lived tokens;
- audience, tenant, principal, purpose, and expiry binding;
- key rotation and revocation;
- request nonce or replay protection;
- separate service, human, Vessel, and delegated-agent credentials;
- administrative step-up authentication.

### 21.5 Communication and ingress security

Threats include:

- forged webhook or stream events;
- replay and duplicate-delivery attacks;
- provider-account and display-name impersonation;
- malicious content and attachments;
- unauthorized or stale ingress bindings;
- cross-tenant channel leakage;
- unauthorized audience widening;
- message floods and attention denial-of-service against subscribers;
- confidential content copied into receipts or logs;
- ambiguous send causing duplicate communication;
- edit/delete events used to rewrite history without tombstones.

Controls include authorized canonical ingress bindings, connector-specific signature or session verification, assurance metadata, deduplication, tenant/channel scoping, artifact quarantine and scanning, rate limits, structural subscriber and audience authorization, content minimization, immutable correction relations, and delivery reconciliation.

### 21.6 Filesystem security

- server-configured roots and resource ownership;
- canonicalization and symlink handling;
- no arbitrary host path access from untrusted requests;
- separate runtime data directories from addressable resource roots;
- expected-version writes;
- atomic replace where supported;
- file size and type policy;
- tenant and sandbox namespaces.

### 21.7 Network security

- default-deny egress for centralized and sandbox profiles;
- destination and protocol allowlists;
- private/metadata range blocking;
- redirect and DNS rebinding controls;
- scoped credential injection;
- request/response limits;
- TLS verification and optional pinning;
- proxy and service-mesh identity support.

### 21.8 Connector isolation

Risk-tier connectors:

- pure/WASM transforms in-process under fuel and memory limits;
- bounded WASM connectors with declared host capabilities;
- native connectors in supervised process pools where practical;
- high-risk connectors isolated by tenant or operation class;
- managed workers outside the Fabric process.

### 21.9 Policy security

- policy changes are authenticated, signed, versioned, and audited;
- no connector or workflow can modify the policy that governs itself without separate authority;
- closed grammar and static validation;
- shadowed, contradictory, and uncovered consequential classes reported;
- emergency deny can narrow authority globally;
- emergency allow requires stronger procedure than ordinary grant.

### 21.10 Receipt and artifact access

Receipts may reveal sensitive relationships and resource metadata. Access control applies to both receipt body and linked artifacts. Receipt inspection is itself auditable in Platform mode.

### 21.11 Delegated worker security

- run-scoped credentials;
- no ambient platform secrets;
- egress and filesystem perimeter;
- process/resource limits;
- mediated consequential effects;
- immutable input and export manifests;
- post-run cleanup verification;
- no ability to forge requester or accountable-principal fields.

---


### 21.12 Portable-identity threats

Threats include:

- compromised or stale verification methods;
- resolver equivocation or cache poisoning;
- identifier correlation across personas;
- false signed statements mistaken for fact;
- sponsor identity mistaken for actor identity;
- public proof leaking sensitive timing or relationship metadata;
- selective disclosure presented as complete history;
- signer or custody service becoming an unaccountable central authority;
- post-quantum transition breaking historical verification.

Controls include historical key-state snapshots, purpose-bound verification methods, rotation and revocation support, resolver evidence, pairwise identifiers, explicit proof levels, disclosure policy, completeness claims, threshold custody, algorithm agility, and independent provider/witness evidence.

### 21.13 Commissioning and self-improvement threats

Threats include:

- a requester using commissioning to create an ungoverned principal;
- role attestations being combined into unauthorized ambient power;
- sensitive information escaping through candidate builders or reviewers;
- a candidate altering its own evaluation or activation criteria;
- a worker or candidate acquiring lifecycle-supervisor credentials;
- rollback images, release manifests, or protected tests being modified by the subject under evaluation;
- restore being used to create two active copies of one lineage;
- activation uncertainty being treated as success.

Mitigations include explicit acting capacity, purpose-bound authorization, protected evaluation references, isolated build and trial environments, separate activation authority, exact candidate and state identities, and reconciliation before repeat.

## 22. Verification plan

### 22.1 Protocol tests

- schema round-trip and forward-compatible unknown-field behavior;
- stable content hashing;
- agency-chain validation;
- signed authorization and receipt verification;
- caller cannot override server-resolved principal;
- direct and Flow single-node plans normalize equivalently.

### 22.2 Ingress and communication tests

- only an active authorized binding can admit and route an observation;
- duplicate webhook, stream, and provider message delivery yields one observation identity;
- a crash after observation persistence but before subscriber notification cannot lose ingress;
- subscriber redelivery is idempotent and acknowledgment survives restart;
- forged signatures or weak assurance are rejected or explicitly downgraded;
- provider ordering gaps and out-of-order events remain visible;
- edit, delete, reaction, and delivery-status relations link to immutable prior observations;
- a display name cannot impersonate a privileged principal;
- exact outbound audience and channel are resolved before authorization;
- provider acceptance, delivery, read, and uncertainty remain distinct;
- an uncertain send is reconciled before retry;
- binding creation, suspension, revocation, and version changes are replayable;
- embedded and centralized ingress produce equivalent protocol fixtures;
- an Exoskeleton subscriber can persist its domain event before local wake.

### 22.3 Resource resolver tests

- path traversal and symlink escape;
- alias collision and namespace confusion;
- HTTP redirect and DNS rebinding;
- private/metadata IP blocking;
- resource owner and sensitivity classification;
- deterministic resolution under pinned registry version;
- version conflict for reads and writes.

### 22.4 Authorization tests

- grant attenuation property tests;
- node/flow/run cannot expand parent scope;
- expired, revoked, overused, or wrong-purpose grants deny;
- approval exact-intent and resource binding;
- unknown consequential class denies;
- capability visibility matches effective grant but hidden capability remains server-enforced;
- explanation trace points to exact release or deny rule.

### 22.5 Effect lifecycle tests

- intent survives crash before AQ submission;
- duplicate submission deduplicates by intent/idempotency key;
- crash before commit point safely retries according to policy;
- crash after possible commit becomes `Uncertain`;
- reconciliation updates interpretation without mutating original receipt;
- non-repeatable effects never automatically retry;
- cancellation semantics respect commit possibility;
- output and receipt publication recover from partial failure.

### 22.6 Connector conformance suite

Every connector must pass:

- descriptor completeness;
- resource resolution tests;
- authorization enforcement;
- input/output schema validation;
- timeout and cancellation behavior;
- idempotency/reconciliation claims;
- sensitive-data redaction;
- deterministic mock or fixture tests;
- receipt completeness;
- failure injection at commit boundaries.

### 22.7 Flow tests

- cycle and unreachable-node rejection;
- pure transform I/O prohibition;
- branch determinism;
- execution plan hash stability;
- authority attenuation per node;
- denied node cannot be reached through alternate branch;
- uncertain node remains visible in flow terminal state;
- workflow trace and boundary receipt separation;
- replay from historical plan and trace.

### 22.8 WASM tests

- deny-by-default host capabilities;
- filesystem prefix and symlink restrictions;
- HTTP allowlist and network protection;
- command and environment restrictions;
- fuel, memory, timeout, and output limits;
- deterministic transform profile has no clock/random/I/O;
- malformed component and trap isolation;
- module version appears in receipt/trace.

### 22.9 Worker and sandbox tests

- sandbox policy applied before worker start;
- worker cannot alter policy or access host stores;
- direct sensitive egress blocked in hybrid mode;
- mediated external action succeeds with proper agency chain;
- artifact export enforces scope and content policy;
- final delta and cleanup are receipted;
- Agent Host crash yields recoverable state or explicit uncertainty;
- nested worker launch denied unless authorized.

### 22.10 Platform tests

- cross-tenant denial;
- organization and Vessel principal distinction;
- secret isolation and destination binding;
- quota and fairness enforcement;
- signed receipt verification;
- audit access control;
- high-availability failover without duplicate non-idempotent effects;
- embedded/remote semantic conformance.

### 22.11 Kill matrix

Inject termination:

```text
before/after BoundaryObservation persistence
before/after ingress receipt persistence
before/after subscriber-delivery outbox
before/after subscriber acknowledgment
before/after EffectIntent creation in Submitted state
before/after resource resolution
before/after authorization/approval decision
before/after dispatch-outbox commit
before/after AQ ensure_task
before/after connector commit point
before/after ContextStore write
before/after receipt append
before/after AQ completion
before/after reconciliation observation
before/after flow branch persistence
before/after sandbox provisioning and teardown
```

Assert recoverable, explainable state at every boundary.


---


### 22.12 Identity and proof tests

- identifier control proof creates a time-bounded binding;
- identifier format alone never sets `PrincipalKind`;
- same identifier under different historical key states verifies according to event time;
- revoked key cannot create a new accepted statement after revocation;
- historical pre-revocation proof remains inspectable with explicit policy result;
- valid signature over false test claim is reported as valid provenance, not semantic truth;
- actor and sponsor remain distinct in receipts and accounting;
- delegated credential cannot exceed parent authority or survive expiry;
- root signing key is unavailable inside delegate sandbox and connector process;
- selective disclosure reveals only selected claims;
- public inclusion proof does not produce an implicit completeness claim;
- EntityID adapter and non-EntityID reference adapter pass the same SPI conformance suite.

### 22.13 Governed-initiative conformance

- a self-initiated Vessel effect follows ordinary identity, resource, authorization, approval, and receipt paths;
- mission, goal, interest, or initiative references cannot widen a grant;
- unsolicited external communication obeys audience, disclosure, and platform policy;
- discretionary spending is attributed to the accountable principal and sponsor separately.

### 22.14 Commissioning tests

Prove genesis reference preexistence, idempotent principal/ingress/persona provisioning, partial failure recovery, and the inability of external setup to create or rewrite internal Vessel origin.

### 22.15 Information-use tests

Prove audience, disclosure, export, artifact release, worker handoff, retention-trigger, and proof-publication constraints independently of basic read authorization.

### 22.16 Developmental and lifecycle tests

Prove candidate/active separation, build provenance, protected evaluation references, role separation, activation and rollback authorization, crash recovery at every lifecycle boundary, quarantine, restore-versus-fork evidence, and uncertainty reconciliation.

The `STACK-DEVELOPMENTAL-DIAGNOSTICS-1` profile additionally proves bounded campaign causality, experimental/ordinary authorization parity, exact execution-envelope evidence, visibility of material confounds, protected fixture handling, uncertain-arm reconciliation, and the absence of any winning-arm, binding-constraint, task-quality, or improvement-acceptance semantics in WorldInterface.

## 23. Observability and performance

### 23.1 Operator questions

WorldInterface should answer:

- Which inbound observations are stored, quarantined, awaiting subscriber delivery, acknowledged, or missing provider sequence?
- Which outbound effects are proposed, authorized, running, uncertain, reconciling, or terminal?
- Who was accountable, who requested, who submitted, who approved, and who executed?
- Which connector, external channel/thread, asserted audience, and canonical resource were involved?
- Which grant chain and policy rule allowed or denied the effect?
- Which external idempotency key and provider operation ID were used?
- Did the connector cross a commit point?
- Why is an outcome uncertain, and what reconciliation is available?
- Which workflow trace caused a boundary node to execute?
- What is a receipt versus telemetry?
- Which connector, resolver, compiler, and policy versions governed the operation?
- Which target records use superseded but still supported semantics?
- Which Exoskeleton-owned campaign and intervention arm caused this operation?
- Were canonical resource, acting capacity, grants, approvals, information-use constraints, policies, connectors, and execution profiles materially equivalent across intended control arms?
- Which environment and artifact digests support reproduction, and which differences remain explicit confounds?
- Is the record a raw boundary fact, an unresolved observation, or a reconciled outcome? WorldInterface must not present it as task quality or a winning arm.

### 23.2 Metrics

```text
wi_ingress_observations_total{connector,state}
wi_ingress_duplicates_total{connector}
wi_ingress_delivery_lag_seconds{subscriber_class}
wi_ingress_sequence_gaps_total{connector}
wi_communication_delivery_total{connector,state}
wi_effects_total{effect_class,outcome}
wi_effect_duration_seconds{effect_class,connector}
wi_effects_uncertain{effect_class,connector}
wi_reconciliation_total{effect_class,outcome}
wi_authorization_total{decision,effect_class}
wi_resource_resolution_total{class,outcome}
wi_receipt_commit_seconds{profile}
wi_flow_runs_total{outcome}
wi_flow_node_total{kind,outcome}
wi_flow_compile_seconds
wi_connector_invocations_total{connector,outcome}
wi_wasm_fuel_consumed{module}
wi_worker_controls_total{operation,outcome}
wi_sandbox_runs{status,mode}
wi_telemetry_bytes_total{class}
wi_removed_connector_rejection_total{connector}
wi_developmental_effects_total{effect_class,outcome}
wi_developmental_evidence_gaps_total{class}
wi_developmental_authorization_mismatch_total{class}
```

Do not place principal, tenant, resource, intent, run, campaign, arm, task-distribution, or component-fingerprint IDs in metric labels.

### 23.3 Tracing

Use trace IDs that link:

```text
API request
  → authorization
  → intent
  → execution plan
  → AQ run
  → connector attempt
  → ContextStore/artifact write
  → receipt
  → reconciliation
```

For Flow, include flow run and node span links. Tracing is operational and may be sampled; the authoritative records are not.

### 23.4 Performance principles

- resolution and authorization should be bounded and cacheable by versioned registry/policy snapshots;
- direct effect submission should return after durable acceptance, not full connector completion;
- large payloads use artifact references;
- receipts remain compact and link to details;
- high-volume telemetry is batched and tiered;
- connector pools are isolated and capacity-limited by risk class;
- Flow compilation occurs before submission and caches validated definitions;
- reconciliation work is rate-limited separately from normal effects;
- platform quotas prevent one principal or tenant from exhausting shared connectors;
- developmental correlation is carried in bounded authoritative records and traces, not high-cardinality metric labels;
- evidence profiles capture only evaluation-relevant boundary facts and do not turn internal specialist telemetry into primary receipts.

### 23.5 Receipt volume

Do not solve observability by recording every low-level action as a durable boundary receipt. Receipt volume should scale with accountable crossings, not specialist implementation steps.

For sandboxed work, use summary receipts and artifact manifests. Preserve raw telemetry only when risk, debugging, or policy justifies it.

### 23.6 Capacity and backpressure

WorldInterface should expose:

- per-capability concurrency limits;
- connector queue depth;
- resource-specific serialization locks;
- tenant and principal quotas;
- uncertain-effect backlog limits;
- sandbox capacity;
- ContextStore and receipt store pressure;
- admission-control responses with retry-after or escalation semantics.

ActionQueue schedules durable work; Fabric remains responsible for capability-specific admission and resource contention policy.

### 23.7 Alerts

- unclassified consequential connector registered;
- authorization or resolver validation failure;
- repeated path or network escape attempt;
- ingress delivery backlog or provider sequence gap above threshold;
- uncertain-effect or uncertain-communication backlog above threshold;
- reconciliation repeatedly inconclusive;
- receipt commit failure after external success;
- flow authority-analysis mismatch at dispatch;
- connector behavior contradicts declared idempotency;
- sandbox cleanup failure;
- cross-tenant access attempt;
- unauthenticated local-development profile exposed outside its declared boundary;
- coding-specific connector present in Fabric core at target release;
- developmental operation whose campaign reference altered ordinary authorization or approval;
- intended control arms resolve to materially different resources, policies, information-use envelopes, connectors, or environments without a declared difference;
- developmental receipt or API surface attempts to declare task quality, a winning arm, a binding constraint, or candidate acceptance.

---

## 24. Risks and architectural decisions

### 24.1 Risk: Fabric becomes a universal operating-system API

Mitigation: capability admission test, parametric foundational capabilities, sandbox grants, and explicit movement of specialist technique inward.

### 24.2 Risk: receipts become too coarse to investigate incidents

Mitigation: layered records. Boundary receipts remain semantic and durable; workflow traces and protected telemetry provide drill-down without polluting primary history.

### 24.3 Risk: receipts become too detailed and expensive

Mitigation: receipt profiles, content-addressed artifacts, deduplication, summarized sandbox activity, and retention tiers.

### 24.4 Risk: connector descriptors lie about idempotency or side effects

Mitigation: conformance tests, reviewed semantic classification, runtime anomaly detection, connector quarantine, and conservative default to uncertainty/non-repeatability.

### 24.5 Risk: Flow becomes a policy bypass

Mitigation: common ExecutionPlan, node-level attenuation, server-side dispatch authorization, plan hash verification, and no connector-by-name authority.

### 24.6 Risk: resource resolution changes between authorization and execution

Mitigation: bind the authorization and intent to canonical resource identity, resolver version, registry snapshot, and relevant observed versions. Re-resolve only under declared policy and record differences.

### 24.7 Risk: centralized Platform overwhelms the embedded use case

Mitigation: separate product profiles and dependency boundaries. Fabric Embedded remains small and coherent without platform control-plane dependencies.

### 24.8 Risk: WASM is either overused or abandoned

Mitigation: define workload fit explicitly. Use WASM for bounded, portable, untrusted modules; use process/container isolation for native long-lived specialists; keep common protocol semantics.

### 24.9 Risk: worker-control operations smuggle delegation semantics into Fabric

Mitigation: Fabric records lifecycle control only. Work-order quality, progress adequacy, skill competence, and completion acceptance remain outside it.

### 24.10 Risk: policy grammar becomes as complex as code

Mitigation: closed structural predicates, static validation, namespaced resource/effect classes, no arbitrary script predicates, and explicit ADRs when grammar expansion is required.

### 24.11 Risk: historical data becomes misleading after schema upgrade

Mitigation: keep pre-contract artifacts outside the active runtime, label any forensic export explicitly, distinguish target historical replay from current reinterpretation, and never fabricate authorization proof that did not exist.

### 24.12 Risk: Fabric becomes a conversation engine

Mitigation: retain only boundary observation, channel/audience, identity-assurance, delivery, and receipt semantics. Conversation grouping, response obligations, relationship meaning, and attention remain downstream application domains.

### 24.13 Risk: ingress and delivery evidence are mistaken for semantic truth

Mitigation: explicit assurance and epistemic boundaries, names such as `BoundaryObservation`, separate application acknowledgment, and conformance tests proving that receipt generation does not imply instruction authorization or content truth.


### 24.14 Risk: signatures are mistaken for truth

Mitigation: inspection and APIs separate cryptographic verification, authorization, execution, provider evidence, transparency, and semantic acceptance.

### 24.15 Risk: one identity protocol becomes a hidden platform dependency

Mitigation: protocol-neutral SPI, at least two conformance implementations, optional crates, and no EntityID-specific type in Fabric core.

### 24.16 Risk: key custody recentralizes sovereign identity

Mitigation: support local, hardware-backed, threshold, remote-wallet, and organizational custody profiles; make custody and recovery policy explicit and auditable.

### 24.17 Risk: public transparency becomes surveillance

Mitigation: local-first defaults, pairwise identifiers, selective disclosure, batch anchoring, protected artifacts, and explicit completeness scope.

### 24.18 Risk: reputation, bonds, token economics, or consensus distort Fabric

Mitigation: keep attestation and bond envelopes optional and semantic; defer any token or custom chain to a separately justified profile.

### 24.19 Risk: portable proof history is confused with narrative identity

Mitigation: WorldInterface stores evidence and proof. Exoskeleton or NetCorp owns interpretation, narrative, and collective meaning.

### 24.20 Risk: WorldInterface becomes the owner of Vessel lifecycle meaning

Mitigation: Fabric executes and evidences external lifecycle operations; Exoskeleton owns Genesis, continuity, role meaning, acceptance, and narrative.

### 24.21 Risk: information-use policy becomes unimplementable metadata

Mitigation: keep the core envelope small, enforce only boundary-relevant constraints in WorldInterface, and require Exoskeleton to own cognitive and social use semantics.

### 24.22 Risk: candidate provenance is mistaken for improvement

Mitigation: preserve provenance, evaluation evidence, acceptance, activation, and post-activation monitoring as separate records and authorities.

### 24.23 Risk: lifecycle operations create accidental identity forks

Mitigation: require explicit restore/migration/succession/fork classification, exact lineage and state references, one active authority policy, and activation reconciliation.

### 24.24 Risk: experimental labels become an authorization bypass

Mitigation: require ordinary authentication, acting capacity, resolution, information-use evaluation, approval, final authorization, idempotency, uncertainty, and reconciliation for every arm; compare experimental and ordinary submissions in conformance.

### 24.25 Risk: execution drift invalidates comparison

Mitigation: pin exact connector, resolver, registry, policy, artifact, environment, provider, and schema versions where possible; preserve every unresolved difference as a confound rather than normalizing it away.

### 24.26 Risk: boundary success is mistaken for developmental success

Mitigation: keep receipt, provider milestone, verification result, evaluation result, acceptance decision, continuity assessment, and activation state as separate records and authorities. WorldInterface exposes no winning-arm or binding-constraint field.

### 24.27 Risk: developmental evidence leaks sensitive workloads

Mitigation: use governed artifact references, receipt profiles, redaction, information-use envelopes, audience restrictions, retention policy, and protected evaluator channels. Campaign metadata remains bounded and absent from ordinary metric labels.

### 24.28 Decisions made

1. Do not fork WorldInterface into workflow and Exoskeleton codebases.
2. Define Fabric, Flow, and Platform as product surfaces over shared semantics.
3. Make agency, authorization, resource resolution, intent, receipt, and uncertainty foundational.
4. Compile direct effects and FlowSpec into one ExecutionPlan.
5. Keep ActionQueue as sole durable task executor.
6. Keep WASM for bounded capabilities, transforms, verifiers, and adapters—not as mandatory delegated-agent host.
7. Support managed workers through generic control capabilities.
8. Distinguish receipt, workflow trace, evidence, and telemetry.
9. Move specialist coding operations out of Fabric core unless they meet the capability admission test.
10. Preserve generic filesystem/process/network capabilities for mediated use with strict structural grants.
11. Treat sandbox issuance as one accountable bounded capability.
12. Build centralized multi-principal operation as a control plane, not a forked runtime.
13. Treat provider events and incoming communication as first-class durable boundary observations.
14. Keep ingress observations and egress effects as distinct lifecycle families under one Fabric protocol.
15. Treat outbound communication as a semantic effect whose delivery milestone and uncertainty are explicit.
16. Keep conversation, attention, response obligations, and relationship meaning outside WorldInterface.
17. Preserve developmental campaign and intervention-arm references only as bounded causal attribution.
18. Use existing intent, observation, receipt, evidence, uncertainty, reconciliation, candidate, and lifecycle seams; add no developmental protocol primitive.
19. Make material execution-envelope differences inspectable without computing benchmark scores or selecting a winning arm.
20. Keep task distribution, metric semantics, binding-constraint diagnosis, saturation, improvement acceptance, and continuity in Exoskeleton.

### 24.29 ADRs still required

- ADR: exact protocol serialization and supported-version evolution policy;
- ADR: receipt signing and key rotation;
- ADR: intent/receipt/AQ completion transaction protocol;
- ADR: resource canonicalization scheme and URI registry;
- ADR: authorization policy grammar;
- ADR: connector process isolation tiers;
- ADR: Agent Host transport and worker credential format;
- ADR: WASM component model and transform determinism profile;
- ADR: reconciliation API and operator escalation;
- ADR: platform tenancy and organizational grant precedence;
- ADR: ContextStore versus artifact-store responsibility;
- ADR: ingress provider identity, deduplication, and ordering contracts;
- ADR: subscriber delivery acknowledgment and retention;
- ADR: communication channel/audience canonical resource grammar;
- ADR: provider delivery milestone and uncertain-send reconciliation;
- ADR: artifact quarantine and multimodal communication retention;
- ADR: connector deletion and target capability admission decisions.

---

## 25. Definition of done

WorldInterface v2 is complete when the target fabric is coherent without compatibility machinery.

### Fabric core

- [ ] Direct effects work without importing FlowSpec.
- [ ] Direct and Flow paths compile to one `ExecutionPlan` representation.
- [ ] ActionQueue is the sole durable task executor.
- [ ] `EffectIntent` is durable before resolution, authorization, or consequential dispatch.
- [ ] Canonical resource/audience resolution precedes exact final authorization.
- [ ] Unknown consequential effects fail closed.
- [ ] Every consequential connector declares and tests idempotency and reconciliation semantics.

### Ingress and communication

- [ ] Every route is a versioned authorized `IngressBinding`.
- [ ] Provider events are authenticated, deduplicated, receipted, and durable before subscriber notification.
- [ ] Subscriber delivery and acknowledgment are idempotent and crash-safe.
- [ ] WorldInterface preserves provenance without claiming truth, urgency, conversation grouping, or response obligation.
- [ ] Outbound communication resolves provider, channel, sender identity, and audience before authorization.
- [ ] Provider acceptance, delivery, acknowledgment, uncertainty, and reconciliation remain distinct.

### Agency, authority, and identity

- [ ] Caller-supplied identity cannot override verified server resolution.
- [ ] Actor, requester, accountable principal, sponsor, authorizer, approver, executor, issuer, and witness remain distinct.
- [ ] Authority only attenuates; Flow, workers, and child grants cannot widen it.
- [ ] External identifiers bind through versioned `IdentityBinding` records.
- [ ] Signatures are reported as provenance evidence, never semantic truth.
- [ ] Root keys never enter connector, worker, model, or ordinary runtime memory.
- [ ] EntityID and a second adapter both pass the protocol-neutral identity SPI.

### Effects, recovery, and Flow

- [ ] Ambiguous commit produces `Uncertain`; no blind retry occurs.
- [ ] At least one semantic connector demonstrates external-key reconciliation.
- [ ] Output publication, receipt, effect state, and AQ completion converge after every injected crash.
- [ ] Flow authority analysis rejects any attempted enlargement.
- [ ] Workflow traces remain distinct from boundary receipts.
- [ ] A workflow run is never the sole accountable principal.

### Isolation and connector rationalization

- [ ] WASM connector/transform/verifier conformance passes.
- [ ] `sandbox.provision`, `sandbox.release`, and `worker.*` controls are receipted boundary capabilities.
- [ ] Fully mediated, sandboxed, and hybrid profiles pass.
- [ ] `signal.await`, `signal.emit`, `SignalRegistry`, and blocking `delay` are absent.
- [ ] `code_grep`, `code_fuzzy`, `code_glob`, `code_ls`, `code_read`, symbol/reference helpers, and equivalent specialist technique are absent from Fabric core.
- [ ] General filesystem/process/network capabilities remain available only under exact grants.

### Deployment and operations

- [ ] Embedded and remote profiles pass the same protocol conformance package.
- [ ] The daemon has explicit authentication and policy; the local development profile is clearly bounded.
- [ ] Cross-tenant denial and receipt access controls pass.
- [ ] Fresh target stores, backup, restore, replay, and future supported upgrades are tested.
- [ ] Pre-contract stores fail precisely and are not modified.
- [ ] CLI/inspection explains ingress, delivery, agency, authorization, effects, uncertainty, reconciliation, proof, and Flow causality.

### Clean-break completion

- [ ] No old receipt writer/reader, connector alias, compatibility principal, shadow authorization, dual write, or fallback path exists.
- [ ] Every current connector and crate has a documented target disposition.
- [ ] `conformance/wi-fabric-2` pins and passes the exact `aq-cont-1` revision.
- [ ] The published release manifest identifies the exact ActionQueue target version and contract revision.

### Commissioning, lifecycle, and developmental support

- [ ] Acting capacity is explicit and cannot ambiently union role authority.
- [ ] Internal Vessel genesis must preexist every commissioning effect.
- [ ] Information-use constraints are enforced independently of access permission.
- [ ] Candidate artifacts, acceptance decisions, and active runtime are distinct.
- [ ] Lifecycle activation and rollback require a separately authorized supervisor.
- [ ] Restore, migration, succession, and fork produce distinguishable evidence.
- [ ] Build provenance and trial receipts cannot self-certify improvement.
- [ ] Schema and policy dependencies are versioned and replayable.

### Developmental experiment evidence profile

- [ ] Campaign and arm causal references are bounded, immutable attribution and cannot change authority, routing, priority, approval, or budget.
- [ ] Experimental work follows the same resolution, authorization, information-use, idempotency, uncertainty, and reconciliation paths as ordinary work.
- [ ] Receipts and evidence reconstruct material agency, resource, policy, connector, environment, artifact, timing, resource, outcome, and uncertainty conditions.
- [ ] Differences between intended control arms are visible as explicit changes or confounds.
- [ ] Protected workloads and customer data remain governed artifacts rather than ordinary receipt or telemetry content.
- [ ] No WorldInterface record or API declares a winning arm, binding constraint, task-quality judgment, saturation, improvement acceptance, or continuity result.
- [ ] `wi-fabric-2-developmental-experiment-acceptance-matrix.yaml` passes against embedded and remote profiles.

## 26. Implementation sequence

The sequence is target-first and assumes no deployed legacy workload.

### PR 1 — Freeze evidence and establish contract

- tag `pre-wi-fabric-2`;
- archive selected fixtures and current connector inventory;
- add canonical contract, deletion checks, and target store/version declarations.

### PR 2 — Protocol and fresh persistence skeleton

- generalized IDs, agency, submissions, observations, effects, receipts, refs;
- new target stores and record headers;
- no old receipt or Flow readers.

### PR 3 — Accountable ingress

- `IngressBinding`, canonical source resolution, provider deduplication;
- observation receipt, subscriber outbox, acknowledgment;
- port webhook and streaming sources directly.

### PR 4 — Intent-first egress and resource resolution

- durable `EffectIntent`;
- exact resource/audience resolvers;
- authorization envelope and explainable decisions;
- target ActionQueue outbox and `ensure_task` admission.

### PR 5 — Boundary receipts and evidence taxonomy

- direction-aware receipt bodies;
- proof/evidence/telemetry separation;
- developmental evidence profile over existing causal references and evidence records;
- completion publication protocol and crash tests.

### PR 6 — Connector contract and reconciliation

- target resolve/invoke/reconcile trait;
- idempotency declarations and commit observations;
- selected file, HTTP, message, and provider reconciliation implementations.

### PR 7 — Common `ExecutionPlan` and Flow

- direct submission compiler;
- Flow model/compiler/coordinator;
- pure transform enforcement, authority attenuation, and trace separation.

### PR 8 — Remove invalid wait and delay machinery

- delete `signal.await`, `signal.emit`, `SignalRegistry`, and blocking `delay`;
- compile waits and timers to `AQ-CONT-1` continuations;
- add lost-signal and blocked-executor negative tests.

### PR 9 — Managed workers and sandbox boundary

- Agent Host adapter;
- `sandbox.provision`, `sandbox.release`, `worker.*` controls;
- candidate environment, image, toolchain, source, and output provenance for controlled trials;
- hierarchical receipts and hybrid-mode conformance.

### PR 10 — Connector rationalization

- move all specialist coding technique into sandboxes or verifiers;
- generalize foundational filesystem/process/network operations;
- retain only semantic capabilities that pass the admission checklist.

### PR 11 — Authenticated daemon and Platform foundation

- principal authentication, tenant/organization namespaces, secret brokerage;
- quotas, policy administration, audit access, embedded/remote parity.

### PR 12 — Portable identity and proof SPI

- external identifiers, time-versioned bindings, signing providers, proof bundles;
- delegated credentials and disclosure profiles;
- two independent adapter implementations; no mandatory EntityID dependency.

### PR 13 — Conformance and release hardening

- publish `conformance/wi-fabric-2` in-repo;
- pin/pass exact `aq-cont-1` revision;
- run developmental causality, authorization-parity, evidence-completeness, uncertainty, privacy, crash, and replay cases;
- full crash, replay, security, load, backup, and restore matrix;
- verify no forbidden compatibility, specialist, benchmark-scoring, or winning-arm semantics remain.

### PR 14 — Breaking release

- remove development scaffolding and offline comparison harnesses;
- publish WorldInterface against the exact ActionQueue target release;
- publish `STACK-DEVELOPMENTAL-DIAGNOSTICS-1` conformance evidence and the manifest consumed by Exoskeleton.

### Foundational allocation across the existing PR sequence

The foundational additions do not require a fifteenth WorldInterface PR. They are allocated as follows:

| PR | Added responsibility |
|---|---|
| PR 2 | acting-capacity, information-use, commissioning, lifecycle, candidate, provenance, and temporal protocol types; fresh persistence schemas |
| PR 3 | commissioning ingress evidence and role/stewardship observation profiles where transports require them |
| PR 4 | acting-capacity and information-use-aware authorization; lifecycle and candidate effect admission |
| PR 5 | candidate/build/trial/activation proof bundles, bounded campaign causality, developmental evidence profiles, and direction-appropriate receipts |
| PR 6 | idempotency and reconciliation for lifecycle operations and external supervisors |
| PR 7 | deterministic Flow composition of already-authorized commissioning or lifecycle effects without authority expansion |
| PR 9 | isolated candidate-build and lifecycle-host worker boundaries |
| PR 10 | small admitted lifecycle capability catalog and explicit rejection of universal deployment shell access |
| PR 11 | Platform role separation, lifecycle services, audit access, and operator controls |
| PR 12 | external role/stewardship attestations, key lifecycle, and component signing/proof profiles |
| PR 13 | commissioning, information-use, candidate, activation, rollback, restore/fork, temporal, authorization-parity, developmental-evidence, privacy, and semantic-non-ownership conformance suites |
| PR 14 | downstream EXO-V3 fixtures plus exact lifecycle/provenance and developmental-evidence profile handoff |

## 27. Protocol sketch and worked examples

### 27.1 Inbound communication to an Exoskeleton Vessel

```text
Fabric binding:
  resolves Discord tenant/channel and authorizes delivery to Vessel V

Discord connector:
  authenticates gateway session
  receives provider message ID M in channel H/thread T
  retains asserted sender account S and direct mention of Vessel V
  stores content artifact A

Fabric:
  deduplicates by provider tenant + M
  matches active IngressBinding G
  persists BoundaryObservation O
  persists ingress receipt R
  writes subscriber-delivery outbox for Vessel V under G

Exoskeleton subscriber:
  durably stores CommunicationEvent C and AttentionDemand D
  acknowledges delivery
  admits its own Vessel ActionQueue signal

Important boundary:
  WorldInterface proves what Discord delivered and with what assurance.
  Exoskeleton decides conversation grouping, principal binding,
  attention, truth, relationship meaning, and response obligation.
```

### 27.2 Outbound response with uncertain delivery

```text
Exoskeleton:
  submits communication EffectSubmission E
  audience selector binds channel H, thread T, recipient S
  content artifact A and purpose/causal refs included

Fabric:
  persists EffectIntent I
  resolves exact provider/channel/thread/audience
  authorizes communication.send
  submits with provider idempotency key K

Crash:
  connection drops after provider may have accepted the message

State:
  I → Uncertain
  no blind resend

Reconciliation:
  query provider by K or inspect thread history
  append ProviderAccepted or NotCommitted evidence
  signal Exoskeleton only after authoritative state is stored
```

### 27.3 Direct Vessel file read

```text
Client:
  accountable principal: vessel/V
  requester: vessel-subsystem/planner
  purpose: ref://exoskeleton/commitment/C
  effect class: filesystem.read
  selector: workspace://project/CHARTER.md

Fabric:
  authenticates Vessel V
  resolves canonical file resource F
  checks read grant G
  persists EffectIntent E
  dispatches through AQ

Receipt:
  agency chain
  canonical resource F
  observed hash H
  output artifact A
  connector and resolver versions
  success outcome
```

This read has historical value because it shaped the Vessel's later decisions.

### 27.4 Coding delegate inside hybrid sandbox

```text
Fabric receipts:
  sandbox S created for delegated run R
  repository revision mounted
  network policy applied
  worker started

No primary receipts for:
  grep
  source reads
  local edits
  compiler invocations
  temporary build files

Summary evidence:
  changed-path manifest
  tests executed by class
  network destinations
  exported patch and report

Mediated boundary effect:
  github.pull_request.create
  requester = delegated-run/R
  accountable principal = vessel/V
  approval = human/K
```

### 27.5 Flow cannot enlarge authority

```text
Principal grant:
  read invoices/**
  send messages only to accounting@example.com

FlowSpec contains nodes:
  read invoice
  transform amount
  send message to arbitrary parameter recipient

Compiler/authorization result:
  flow definition valid as a DAG
  run rejected or node blocked because recipient resource scope
  exceeds principal grant
```

Naming `message.send` in the graph does not grant it.

### 27.6 Ambiguous payment

```text
EffectIntent E:
  payment.submit
  externally keyed by E.idempotency_key

Attempt A1:
  provider accepted connection
  process crashed before response
  commit possible

Fabric state:
  Uncertain
  no automatic second payment

Reconciliation:
  query provider by idempotency key
  provider reports payment P committed

Receipts:
  A1 uncertainty receipt
  reconciliation receipt with provider operation P
  current interpretation = ReconciledSucceeded
```

### 27.7 Organizational workflow

```text
accountable principal: organization/netcorp
flow owner: organization/netcorp
submitted_by: vessel/accounts-payable
requester: workflow-run/42

nodes:
  observe invoice resource
  pure validate transform
  branch on amount band
  request human approval
  submit payment
  send notification

records:
  branch and eligibility = workflow trace
  invoice read, approval request, payment, message = boundary receipts
```

### 27.8 WASM transform versus connector

```text
WASM transform:
  input: invoice JSON artifact
  output: normalized invoice
  no host I/O
  workflow trace only

WASM connector:
  input: normalized invoice
  host capability: HTTPS to allowlisted provider
  boundary receipt with agency, authorization, destination, and outcome
```

### 27.9 Compact connector sketch

```rust
pub struct ExecutionContext {
    pub effect_intent_id: EffectIntentId,
    pub agency: VerifiedAgencyContext,
    pub authorization: EffectiveAuthorization,
    pub attempt: AttemptIdentity,
    pub cancellation: CancellationToken,
    pub deadline: Option<Timestamp>,
    pub telemetry: TelemetrySink,
}

pub struct ConnectorOutcome {
    pub outcome: EffectOutcome,
    pub outputs: Vec<ArtifactRef>,
    pub external_refs: Vec<ExternalOperationRef>,
    pub commit_observation: CommitObservation,
    pub evidence: Vec<EvidenceRef>,
}
```

---


### 27.10 Signed Vessel-to-Vessel communication

```text
Vessel A chooses external persona PA.
WorldInterface resolves PA's current key state and signs the
canonical communication request through the configured custody provider.

WorldInterface delivers the message and returns:
  actor-signed request
  identity-binding snapshot
  executable authorization
  provider delivery receipt

Vessel B verifies provenance through its own WorldInterface deployment.
B still evaluates the message's claims as evidence, not truth.
```

### 27.11 Sponsored delegated action

```text
Actor:
  delegated-run/R on behalf of vessel/V

Sponsor:
  organization/netcorp pays provider and compute fees

Authorizer:
  attenuated work-order grant G

Executor:
  connector/service-X

Receipt preserves all four roles.
The sponsor does not become the actor, and payment does not increase authority.
```

### 27.12 EntityID adapter without core dependency

```text
EntityID adapter:
  resolves identifier and key-event lineage
  verifies proof of control
  returns KeyStateSnapshot K

Fabric core:
  creates IdentityBinding B to local PrincipalId P
  applies ordinary resource authorization
  executes effect
  signs Platform receipt through configured provider

No EntityID ledger, token, reputation, or bond type enters
worldinterface-protocol or ActionQueue.
```

### 27.13 Vessel commissioning after Genesis

```text
Exoskeleton commits GenesisRecord G and commissioning outbox O.
WorldInterface receives CommissioningSubmission(G).
It registers principal P, provisions ingress binding I,
and returns receipts R1/R2.
Exoskeleton records those receipts and admits ActivationDecision A.

If persona creation fails, G remains immutable and the Vessel remains
CommissioningBlocked rather than half-recreated.
```

### 27.14 Role-scoped mission effect

```text
Vessel holds Architect and Auditor roles.
EffectSubmission names acting capacity Auditor.
Architect-only deployment authority is not considered.
The requested production write is denied even though the same
principal could request a different effect while acting as Architect.
```

### 27.15 Candidate activation and rollback

```text
Candidate C is built and signed under EvaluationPlan E.
Independent verification produces evidence V.
Exoskeleton records acceptance decision A.
WorldInterface admits runtime.activate(C, checkpoint S, rollback B).
Lifecycle supervisor performs the transition and emits receipt R.
Health checks fail; runtime.rollback(B) is separately authorized.
Candidate C remains part of developmental history but is not active.
```

### 27.16 Purpose-bound disclosure

```text
Observation O may be used for commitment K and disclosed only to role R.
A delegated worker outside R requests the artifact.
Filesystem read permission exists, but the InformationUseEnvelope denies export.
WorldInterface records the denied disclosure without exposing content.
```


## 28. Foundational lifecycle and developmental seams

This section is a normative extension of `WI-FABRIC-2`. It does not turn WorldInterface into the owner of Vessel identity, purpose, belief, role semantics, improvement acceptance, or autobiographical continuity. It defines the protocol-neutral boundary support required so that Exoskeleton can instantiate, commission, operate, improve, suspend, migrate, and retire Vessels without bypassing accountable effects.

The governing rule is:

> **WorldInterface may verify and execute the external consequences of a Vessel lifecycle decision. It does not decide what makes the Vessel the same entity, what its role means, whether a proposed improvement is desirable, or whether a candidate should be accepted.**

### 28.1 Instantiation and commissioning boundary

Internal Vessel genesis is an Exoskeleton transaction. WorldInterface enters only after a durable `GenesisRecord` and commissioning outbox exist.

```text
Exoskeleton
    validate pinned blueprint and manifest
    commit VesselId + GenesisRecord + initial governance
    commit commissioning outbox
        ↓
WorldInterface
    authenticate commissioning requester
    resolve exact external targets
    authorize bounded setup operations
    register local principal / ingress / persona / credentials as requested
    return receipts or explicit uncertainty
        ↓
Exoskeleton
    admit commissioning evidence
    decide ReadyForActivation / Blocked / AbortedBeforeActivation
```

WorldInterface must not:

- mint the internal `VesselId`;
- infer purpose or role from prose;
- treat external identifier creation as Vessel genesis;
- rewrite a Genesis Record after partial commissioning;
- make activation contingent on an external persona unless Exoskeleton's pinned activation policy requires it.

A commissioning submission can carry generalized references:

```rust
pub struct CommissioningSubmission {
    pub client_request_id: ClientRequestId,
    pub accountable_principal: PrincipalRef,
    pub genesis_ref: OpaqueRef,
    pub acting_capacity_ref: Option<OpaqueRef>,
    pub requested_operations: Vec<CommissioningOperationRequest>,
    pub disclosure_policy_ref: OpaqueRef,
}
```

The authoritative operations remain ordinary effect intents with exact resource resolution, authorization, idempotency, receipts, and reconciliation.

### 28.2 Role and acting-capacity evidence

A local WorldInterface principal may be associated with externally verifiable organizational roles, stewardship attestations, or delegated credentials. These are evidence and authorization inputs, not internal Exoskeleton role state.

Every consequential operation may carry an explicit acting-capacity reference:

```rust
pub struct ActingCapacityRef {
    pub principal_ref: PrincipalRef,
    pub assignment_ref: OpaqueRef,
    pub organization_ref: Option<PrincipalRef>,
    pub attestation_ref: Option<ProofRef>,
}
```

WorldInterface validates, where policy requires:

- the referenced assignment or credential is authentic and current;
- the requester is entitled to act under it;
- the exact operation falls within its externally visible bounds;
- expiration, revocation, and separation-of-duty rules are satisfied.

WorldInterface does not decide whether the assignment is a good fit, whether the Vessel should accept a mission addressed to that role, or whether several roles conflict. Exoskeleton owns those semantic decisions.

The authority calculation remains attenuating:

```text
effective executable authority =
    authenticated principal authority
  ∩ acting-capacity bounds
  ∩ delegated grant chain
  ∩ purpose and information-use constraints
  ∩ exact resource policy
  ∩ current platform policy
  ∩ required approval
```

Multiple role attestations are not ambiently unioned. A submission names the capacity under which it is acting; policy may reject ambiguous capacity.

### 28.3 Information-use envelope

Access control answers whether a principal may obtain or modify a resource. It does not answer how observed information may be retained, derived from, disclosed, republished, or reused.

WorldInterface therefore supports a protocol-neutral `InformationUseEnvelope` attached to observations, artifacts, effect submissions, proof publication, and exports:

```rust
pub struct InformationUseEnvelope {
    pub purpose_refs: Vec<OpaqueRef>,
    pub allowed_audiences: AudienceConstraint,
    pub retention_policy_ref: Option<OpaqueRef>,
    pub disclosure_policy_ref: Option<OpaqueRef>,
    pub derivation_policy_ref: Option<OpaqueRef>,
    pub redistribution: RedistributionPolicy,
    pub expiration: Option<Timestamp>,
    pub source_obligation_refs: Vec<OpaqueRef>,
}
```

WorldInterface enforces only boundary-relevant portions:

- recipient and audience restrictions;
- export and publication policy;
- credential and proof disclosure;
- retention-trigger operations under its own stores;
- artifact release and redaction profile;
- prohibition on sending material to an unauthorized worker, service, or platform.

Exoskeleton owns the social and epistemic meaning of the obligation, including whether information may enter long-term memory, support a derived belief, or be used for a new purpose. A boundary receipt records which use envelope governed the crossing.

### 28.4 Developmental operations are accountable effects

A Vessel may participate in improving its own future behavior, but candidate development and runtime activation remain external consequences.

WorldInterface treats these as ordinary governed effect classes, for example:

```text
source.checkout
candidate.build
candidate.sign
candidate.publish
trial.start
trial.stop
runtime.checkpoint
runtime.activate
runtime.rollback
component.quarantine
backup.verify
```

The exact catalog remains deployment-specific. The stable semantics are:

- the currently active version is not overwritten in place;
- candidate artifacts are content-addressed and versioned;
- build provenance and evaluation evidence are distinct;
- activation and rollback are separate authorized operations;
- activation is performed by a lifecycle supervisor or deployment authority outside ordinary cognition;
- a candidate cannot invoke its own activation merely because it produced a passing report.

### 28.5 Candidate artifact and component provenance

WorldInterface may preserve a protocol-neutral candidate record:

```rust
pub struct CandidateArtifact {
    pub candidate_id: CandidateId,
    pub component_class: ComponentClass,
    pub subject_ref: OpaqueRef,
    pub source_revision_ref: ArtifactRef,
    pub build_artifact_ref: ArtifactRef,
    pub build_provenance_ref: ProofRef,
    pub evaluation_plan_ref: OpaqueRef,
    pub candidate_digest: ContentHash,
}
```

The corresponding proof bundle can include:

```text
source identity and revision
build environment and toolchain
builder identity
input and output digests
protected evaluation-plan reference
verification reports
trial receipts
activation authorization
activation or rollback receipt
```

These records establish provenance and observed execution. They do not establish that the candidate is genuinely better, safe, or identity-preserving. Exoskeleton and independent verifiers own those judgments.

### 28.6 Lifecycle supervisor boundary

The lifecycle supervisor is a small external authority that can stop, activate, roll back, quarantine, restore, or migrate a runtime under explicit authorization. It may be implemented as part of an operator host, deployment service, or future Platform component; it is not a fourth domain runtime.

WorldInterface mediates supervisor operations through exact intents and receipts. A typical activation sequence is:

```text
active Vessel reaches durable checkpoint
    ↓
ActivationRequest admitted and authorized
    ↓
lifecycle supervisor verifies candidate and rollback target
    ↓
active process stopped
    ↓
candidate activated against approved state version
    ↓
health and continuity checks run
    ↓
ActivationReceipt or Uncertain
    ↓
rollback remains available during policy-defined window
```

The supervisor's credentials and protected rollback images remain outside faculties, delegates, ordinary workers, and the candidate itself.

### 28.7 Restore, migration, succession, and fork

WorldInterface does not determine metaphysical identity, but it must preserve the operational distinction among lifecycle operations:

```text
restore
    resume one accepted lineage from a backup or checkpoint

migration
    move one accepted lineage to another host or runtime environment

succession
    provision a different principal that inherits selected duties or artifacts

fork
    create a new identity/lineage from copied prior state
```

Receipts must record:

- source state and lineage references;
- target principal or runtime identity;
- operator and authorizer;
- whether the operation claims continuation, succession, or branch creation;
- copied and excluded state classes;
- cryptographic and artifact provenance;
- activation and retirement outcomes.

Exoskeleton decides whether the evidence satisfies its continuity policy. WorldInterface must not collapse all four operations into a generic “restore.”

### 28.8 Temporal provenance

Boundary records distinguish at least:

```text
occurred_at
    provider-asserted or externally observed event time

observed_at
    time a sensor, connector, or provider observed the event

received_at
    time WorldInterface received the report

admitted_at
    time the observation or intent became durable

dispatched_at / committed_at / reconciled_at
    effect lifecycle times
```

These timestamps can be uncertain, provider-supplied, or clock-skewed. Receipts preserve source and assurance. They do not claim that provider time is true or causally complete.

For causal ordering, stable identifiers and explicit predecessor references outrank wall-clock coincidence. Exoskeleton may later derive `known_at` or semantic validity intervals when admitting evidence.

### 28.9 Schema and policy dependency versioning

WorldInterface protocol types, capability descriptors, resource resolvers, information-use envelopes, role attestations, receipts, and lifecycle-operation classes are behaviorally meaningful schemas.

Every authorization and receipt record therefore pins:

- exact schema identifiers and versions;
- resource-resolver version;
- capability descriptor version;
- policy and grant versions;
- connector and provider profile;
- cryptographic algorithm profile where applicable.

A new schema version cannot redefine an existing effect or role-attestation class in place. Unknown consequential versions fail closed. Historical records remain verifiable under the versions active when they were created.

### 28.10 Contract with EXO-V3

The extended boundary contract is:

```text
EXO-V3 owns
    Blueprint / Manifest / GenesisRecord
    Purpose Charter and roles
    mission and commitment admission
    beliefs, narrative, viability, and improvement acceptance
    continuity assessment

WI-FABRIC-2 owns
    authenticated commissioning requester
    exact external principal / persona / ingress / credential targets
    role and stewardship attestation verification at the boundary
    information-use enforcement on crossings
    candidate build / trial / activation / rollback effects
    receipts, provenance, uncertainty, and reconciliation

AQ-CONT-1 owns
    opaque durable tasks, waits, signals, checkpoints, and recovery
```

No layer may infer authority merely because the operation is described as commissioning, maintenance, self-improvement, recovery, or mission-aligned.

### 28.11 Initial capability profile

The first `WI-FABRIC-2` release does not need a large lifecycle catalog. It needs stable seams and a small conformance profile:

```text
principal.register / principal.bind
identity.issue-delegation
identity.rotate / identity.revoke
artifact.import / artifact.export / artifact.publish
sandbox.provision / sandbox.release
worker.start / worker.inspect / worker.collect / worker.terminate
candidate.build
runtime.checkpoint
runtime.activate
runtime.rollback
information.disclose
```

Deployments may implement these through native connectors, remote services, WASM adapters, or a lifecycle host. Each operation still passes the normal admission, resolution, authorization, intent, execution, receipt, and reconciliation path.

### 28.12 Deferred depth

The following remain intentionally deferred without closing the seam:

- automatic multi-party ratification of Vessel activation;
- public transparency of every component change;
- a custom EntityID chain, token, or bond economy;
- general-purpose organizational role semantics inside WorldInterface;
- self-attested improvement scores;
- a universal deployment platform;
- collective narrative or institutional mythos.

The target is a stable accountable boundary, not a complete theory of digital institutional development.

### 28.13 Controlled developmental-experiment evidence profile

The detailed planning contract is `wi-fabric-2-developmental-experiment-evidence-profile.md`, with conformance seeds in `wi-fabric-2-developmental-experiment-acceptance-matrix.yaml`.

Exoskeleton may use WorldInterface during capability, working-representation, or harness interventions. WorldInterface preserves the causal and boundary conditions of those operations through existing records:

```text
Exoskeleton CampaignExecutionContext
    → bounded causal reference on ordinary submission
    → verified AgencyContext
    → EffectIntent and exact ResolvedEffect
    → ordinary AuthorizationEnvelope and InformationUseEnvelope
    → connector / worker / lifecycle execution
    → BoundaryReceipt, EvidenceRef, Uncertain, and Reconciliation
    → Exoskeleton verification and evaluation
```

The profile requires four properties:

1. **No privilege from experiment identity.** Campaign, arm, benchmark, trial, or self-improvement labels are attribution only.
2. **Comparable boundary evidence.** Exact agency, resource, resolver, registry, policy, information-use, connector, environment, artifact, timing, resource, outcome, and uncertainty conditions are pinned or qualified.
3. **Confounds remain visible.** WorldInterface never hides a materially different target, authority envelope, connector, provider, image, or reconciliation state behind one generic success status.
4. **No semantic overreach.** A receipt does not compute a benchmark score, select a winning arm, diagnose a binding constraint, establish saturation, accept a candidate, or decide continuity.

This profile adds no new public protocol type. It is a population, evidence, inspection, and conformance rule over the existing Fabric contract.

# Appendix A — Capability admission checklist

Before adding a capability, answer:

1. What trust boundary is crossed?
2. What accountable principal owns the consequence?
3. What canonical resource is addressed?
4. Is the operation shared, durable, sensitive, or externally visible?
5. Can failure leave an ambiguous external commit?
6. What idempotency or reconciliation protocol exists?
7. Does a semantic connector improve governance over generic filesystem/process/network access?
8. Could the operation live inside a sandbox without reducing meaningful accountability?
9. What receipt evidence is actually useful?
10. What policy grammar authorizes it?
11. What connector isolation level is required?
12. Can historical versions remain interpretable?
13. Does an external identifier need a verified local binding?
14. What historical key state and verification purpose are required?
15. Does the proof establish authorship, authorization, execution, observation, transparency, or only a claim?
16. Are actor, sponsor, issuer, witness, and executor distinguished?
17. What disclosure and completeness policy applies?
18. Could a protocol-specific adapter remain optional?

If the only answer is “an agent may want to call it,” it does not belong in Fabric core.

# Appendix B — Record taxonomy

```text
EffectIntent
    durable proposed boundary operation

AuthorizationDecisionRecord
    why the operation was allowed, denied, or required approval

BoundaryReceipt
    what operation was attempted and what outcome was observed

ReconciliationReceipt
    later evidence resolving or refining uncertainty

WorkflowTrace
    deterministic orchestration progress

EvidenceArtifact
    material supporting a claim or result

OperationalTelemetry
    low-level diagnostics and performance detail
```

# Appendix C — Product boundaries

```text
WorldInterface Fabric:
  accountable ingress and effects
  portable operational agency and proof

WorldInterface Flow:
  deterministic composition of effects

WorldInterface Platform:
  authenticated multi-principal operation

Exoskeleton:
  persistent cognition, commitments, delegation, and acceptance

NetCorp:
  organizational identity, roles, policy, ownership, and coordination

ActionQueue:
  durable opaque-work lifecycle
```

# Appendix D — Source posture

This foundational-completeness edition was prepared against:

- WorldInterface public `main` at commit `f48df82`, including the coding-capability expansion;
- the current repository architecture, FlowSpec, connector, ContextStore, embedded host, daemon, WASM, and security documentation;
- ActionQueue's current public durable execution model;
- the companion Exoskeleton v3 redesign and the architecture discussion concerning delegation, agency, receipt scope, workflows, and NetCorp.

The source was inspected statically. Exact transaction boundaries, APIs, and crate splits must be validated through the implementation and conformance plan.

# Appendix E — One-sentence acceptance statement

> **WorldInterface v2 succeeds when direct effects, observations, deterministic workflows, delegated-worker boundaries, portable identities, and multi-principal platform actions all use one authority-attenuating, resource-resolving, intent-first, reconciliation-aware fabric whose evidence explains accountable crossings without mistaking signatures for truth, proof history for narrative identity, or specialist detail for constitutional history.**

# Appendix F — External standards informing the portable-identity seam

These references inform the optional identity/proof profile. They are not mandatory dependencies and do not constrain WorldInterface to one identifier method, ledger, custody model, or disclosure mechanism.

- **W3C Controlled Identifiers v1.0:** verification methods, controller relationships, rotation, revocation, and historical verification considerations. <https://www.w3.org/TR/cid-1.0/>
- **W3C Verifiable Credentials Data Model v2.0:** issuer–holder–verifier claims, status, evidence, and selective-disclosure considerations. <https://www.w3.org/TR/vc-data-model-2.0/>
- **RFC 9943, SCITT Architecture:** signed statements, registration policy, transparency receipts, and the explicit separation between accountability and claim truth. <https://www.rfc-editor.org/rfc/rfc9943.html>
- **KERI specification:** self-certifying identifiers and append-only key-event lineage for rotation, delegation, and control provenance. <https://trustoverip.github.io/kswg-keri-specification/>
- **RFC 9162, Certificate Transparency Version 2.0:** Merkle-tree inclusion and consistency proof patterns relevant to optional transparency logs. <https://www.rfc-editor.org/rfc/rfc9162.html>
- **NIST FIPS 204, ML-DSA:** the standardized post-quantum signature family derived from CRYSTALS-Dilithium; algorithm selection remains registry-driven and agile. <https://csrc.nist.gov/pubs/fips/204/final>

The architecture deliberately adopts semantics rather than inventing a new mandatory wire format. An EntityID profile should reuse mature standards wherever they satisfy its requirements and introduce new protocol machinery only where observed collaboration demands it.


## 29. Initial embedded process-provider execution profile

The normative planning supplement `WI-WORKER-EXECUTION-1` selects a bounded reviewed-process provider for proving delegation through embedded WI/AQ. Exoskeleton's own initial Kata/containerd deployment is a separate instance boundary. WI neither installs a container engine inside the Vessel nor inherits host runtime administration. Instance evidence and worker control evidence remain separate.

Only approved fixtures and permitted non-sensitive data are admitted to this first provider. Arbitrary generated executable candidates, production credentials, sensitive production data and independent-worker-VM assurances are unavailable. Existing strong isolation and broader mediation-profile claims remain target obligations, not success claims from the process proof. See [the provider supplement](wi-fabric-2-worker-execution-profile.md) for authority, controls, artifact handling, run recovery and external-provider compatibility.

A later provider may execute remotely or in its own VM without changing delegation meaning. WI owns producer evidence; EXO owns why work runs and whether it is accepted. `WI-EMBEDDED-WORKER-PROOF-1` is demonstrable with a WI-owned test consumer and real embedded AQ/WI, not with a required EXO binary or platform rollout. A bounded external supervisor adapter is a separate lifecycle capability; host management credentials remain outside the guest.
