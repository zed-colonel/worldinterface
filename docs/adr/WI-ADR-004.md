# WI-ADR-004: Principal authentication, actor binding, accountable-principal derivation

Status: accepted

Owner: operator

Due: WI-04

Source: implementation plan §4; controller scope binding revision 4.

Approval: `e31d8d22-208c-4ba8-9b7a-fcfa6695fc2e`, recorded 2026-09-21T19:32:01.233Z. Exact accepted decision and provenance: [approval record](approvals/WI-ADR-004.json), SHA-256 `f0a51df2e5c8bc2321fee3b98337cfeb2a388253486c7462a0f81ccbf8e2f29e`.

WorldInterface constructs authoritative agency from an injected trusted authentication interface and versioned server-owned actor/principal/delegation bindings. Caller identity, requested principal, acting capacity and delegation references are claims, never trusted transport context. Require a unique valid tenant-scoped binding, a current acyclic attenuating delegation chain, and evidence that the requester may use the named accountable principal and acting capacity. Reject missing, expired, revoked, ambiguous or incompatible bindings. Preserve requester, accountable principal, submitted-by, sponsor, authorizer and approver as distinct roles; sponsorship and causal references grant no authority. Persist agency and binding provenance with the immutable intent, using existing records and bounded typed evidence artifacts. Preserve ADR-002's authenticated-principal admission namespace. Trusted context types are not caller-deserializable. Embedded hosts may supply trusted handles; centralized hosts must authenticate credentials before constructing these contexts. WI-04/domain supplies pure validation and test-only authentication/evidence ports; production transport and credential mechanisms remain their owning integration and daemon work.

This records approval, not implementation evidence. See the [inactive egress domain contract](../../conformance/wi-fabric-2/egress/README.md). WI-WORKER-G1, AQ integration, merged-target verification and parent acceptance remain independently due.
