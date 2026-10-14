# WI-ADR-014: Common `ExecutionPlan`, plan hash, transform determinism, and Flow versioning

Status: accepted

Owner: operator

Due: WI-07

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals/WI-ADR-014.json), approval `95b07d70-97d4-43d3-a685-5a49993409db`, 2026-09-29T16:12:12.732Z. Decision packet SHA-256: `11c17826f9ed097a7fd0bd6e13638f5491609a3afca07a920d3bae2aef326894`.

Coverage: full.

Adopt one versioned immutable internal ExecutionPlan for both direct effect submissions and Flow submissions. A direct submission compiles to a one-operation plan; a Flow definition compiles to the same plan contract with canonical operations, stable operation identities, dependencies, outputs, resource selectors or bound resources, expected effect classes, per-operation authority upper bounds, transform and runtime identity hashes, and causal metadata. Only the validated compiler produces executable plans; ordinary clients submit effects or reference immutable versioned Flow definitions. Encode the plan using the accepted WI-FABRIC-2 wi-json-1 canonical envelope and compute its plan hash with domain-separated SHA-256 over canonical plan bytes, excluding the derived hash and run-specific identifiers. Persist and verify those bytes and the hash before execution and during recovery. Each run pins exactly one plan hash, any Flow definition version, a registry snapshot, and its submitting authority upper bound. Editing an activated Flow creates a new definition version and plan. Compilation validates graph structure, schemas, declared resources, bounded fan-out, branch possibilities and static authority bounds. Branches select only declared possibilities and cannot add effects or resource scope. Exact resource resolution and per-node authorization occur immediately before each boundary dispatch. Pure transforms and structural branches receive no boundary authority; transforms use declared immutable inputs, pinned implementation and runtime versions, and a deterministic execution profile without undeclared I/O, clock, randomness, secrets or process access. Record transform input/output hashes and branch outcomes in workflow traces, separately from boundary intents and receipts. ActionQueue remains the durable executor.

Rationale: I approve this recommended approach. WI should be the sole authority with regard to Execution.

Retained obligations: Preserve WI-07 domain and integration merge and verification gates, exact upstream AQ conformance, independent review, parent acceptance, final promotion, and the adopted policy's separate operator-owned remote publication and protection obligations.

Consumer applicability: []

Alternatives: Separate direct/Flow execution formats, dynamically expanded authority and transforms with undeclared I/O are excluded.

Invariant impact: Canonical immutable plans, stable operation identity and per-boundary authorization remain mandatory.

Verification: design authority only; implementation and adopted-check passage remain with the mapped WIP scopes. Approval establishes no test result.
