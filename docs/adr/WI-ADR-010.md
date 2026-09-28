# WI-ADR-010: Connector resolve/invoke/reconcile SPI and idempotency classes

Status: accepted

Owner: operator

Due: WI-06

Source: implementation plan §4; controller scope binding revision 4.

Full decision approval: `0dae9353-3319-4740-9798-c005f6a32253`, recorded 2026-09-24T01:32:29.668Z. Exact accepted decision and provenance: [approval record](approvals/WI-ADR-010.json), SHA-256 `858084bb5a3cc73d4b77be4839f507a44a4b9fc2d1f24629bd1be808955339f0`. The accepted record was supplied by `craftingtable-scope-evidence.json` under `checkpoints[WI-WORKER-G1].decisions` with submission `751fa25b-ef51-41c4-a1cd-b9288b2aeb36`.

## Decision

Adopt a versioned capability and connector contract with typed resolve, invoke, and reconcile operations. Resolve an unresolved request against a pinned resource registry snapshot before final authorization; invoke only the exact resolved effect under its bound authorization envelope; reconcile a persisted intent after possible external commit. Express preparation, commit observation, and meaningful cancellation as explicit phases or typed results. Fabric owns authorization, durable outcome classification, receipts, and reconciliation scheduling; connectors provide bounded execution and evidence and cannot widen authority. Every admitted descriptor declares effect class, resource grammar, schemas, isolation and secret requirements, idempotency class, provider-key mapping, commit observation, retry boundaries, reconciliation support, and evidence profile; missing or contradictory declarations fail registration. Classify operations as pure/read-only, naturally idempotent, externally keyed, compensatable, non-repeatable, or unknown; compensation or reconciliation alone never makes an effect repeatable, and unknown consequential connectors cannot register. Derive and record provider replay keys from stable effect or operation identity, never ActionQueue AttemptId. Distinguish pre-dispatch failure, confirmed non-commit, confirmed commit, terminal provider failure, and possible commit. Persist possible commit as Uncertain and reconcile it without blind re-invocation; a later invocation requires evidence of non-commit and explicit policy permission. Bind connector implementation version and descriptor hash to authorization and receipts. ActionQueue remains the sole durable executor, with Fabric transaction and AQ attempt recovery handled across their separate stores.

## Retained obligations

Implement the contract at the mapped work-item and slice gates, qualify providers, run WI-06 conformance and crash/recovery tests against exact AQ pins, complete review and integration, and satisfy separate release and remote-publication obligations. Full architecture approval is not implementation acceptance or passing test evidence.

Approval rationale: “I approve of the recommended decision.” The accepted clauses reject unknown consequential connector registration, AttemptId-derived replay keys, and blind re-invocation after possible commit. Their invariants are exact authorization over a resolved effect, bounded connector authority, and an inspectable Uncertain outcome pending reconciliation. WI-06 must verify descriptor registration, commit classification, replay-key stability, and crash/recovery behavior against the exact AQ pin at its assigned gates.

This approval supplies design authority and full architectural coverage. Implementation and verification remain due at WI-06; WI-WORKER-G1 requires independent acceptance, and this decision alone does not qualify a provider or authorize runtime behavior.
