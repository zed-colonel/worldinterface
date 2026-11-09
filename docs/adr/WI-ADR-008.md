# WI-ADR-008: Boundary receipt canonical form, content hashing, signing profile, and key rotation

Status: accepted

Owner: operator

Due: WI-05

Source: implementation plan §4; controller scope binding revision 4.

Full decision approval: `6702bed5-5dbb-4943-989d-24bef2c865b6`, recorded 2026-09-24T01:31:46.594Z. Exact accepted decision and provenance: [approval record](approvals/WI-ADR-008.json), SHA-256 `81c1e9d516c944c77988c40c0720c0baf4e7844acd95498b60fb085591d73141`. The accepted record was supplied by `craftingtable-scope-evidence.json` under `checkpoints[WI-WORKER-G1].decisions` with submission `8560d4ab-66bf-45f0-a6f5-dbdb48b6221c`.

## Decision

Adopt one immutable, direction-aware BoundaryReceipt encoded in the accepted WI-FABRIC-2 wi-json-1 typed, versioned canonical envelope. Its stable ID, receipt profile and versions, ingress or egress body, temporal provenance, evidence and causal references are covered by a domain-separated SHA-256 digest of canonical receipt bytes; derived digests and signatures are excluded from that input. Hash referenced artifact contents separately over exact bytes. Reject unknown fields, unsupported versions and noncanonical representations, and retain golden byte and digest fixtures. Keep signatures as separate immutable proofs linked to the receipt digest, so a receipt remains stable before and after signing. A signed profile binds the receipt digest, explicit signature suite/version, purpose, key-state snapshot reference and signing time in a domain-separated signing input; Ed25519 is the initial suite. Permit unsigned receipts only where the selected deployment profile allows them. Rotation creates a new key-state snapshot for new signatures without rewriting earlier receipts or proofs. Verify historical signatures against the applicable signing-time key state and effective revocation evidence, and display later revocation or compromise without silently changing recorded evidence. Signing is requested through an optional provider that keeps private key material outside connectors, workers and general Fabric memory. Signature validity establishes provenance only; it does not establish authorization, provider delivery, semantic truth or acceptance.

## Retained obligations

Implement and independently verify WI-05 receipt/publication behavior, WI-12 optional signing-provider and key-lifecycle behavior, applicable AQ and slice gates, parent acceptance, and release obligations under the adopted repository policy.

Approval rationale: “I approve of the recommended approach.” The accepted clauses reject mutable in-place signatures, hashing referenced artifacts as receipt bytes, and treating signature validity as authorization or acceptance. Their invariant is an immutable canonical receipt whose separate proofs preserve historical signing and revocation evidence. WI-05 and WI-12 must verify canonical golden bytes and digests, profile rules, signing-time key state, rotation, and revocation at their assigned gates.

This approval supplies design authority and full architectural coverage. Implementation and verification remain due at WI-05; WI-WORKER-G1 requires independent acceptance, and this decision alone does not qualify a provider or authorize runtime behavior.
