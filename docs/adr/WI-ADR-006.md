# WI-ADR-006: Authorization grammar, attenuation rules, decision explanation schema

Status: accepted

Owner: operator

Due: WI-04

Source: implementation plan §4; controller scope binding revision 4.

Approval: `18f9998d-e0fd-4de5-ba37-168906cd5fe6`, recorded 2026-09-21T20:51:26.863Z. Exact accepted decision and provenance: [approval record](approvals/WI-ADR-006.json), SHA-256 `2a826c9234ab0aebf218f95b679f13fbbca6036762e21c9ade1f771586f0b4c0`.

Adopt a closed versioned typed authorization grammar with default denial and no executable predicates. Compute effective authority by intersecting authenticated principal authority, the explicitly selected acting capacity, each delegation bound, purpose and information-use constraints, exact resource policy, platform policy and required approval. Never union ambient roles or derive permission from descriptors, causal labels, signatures alone or caller assertions. Evaluate verified grant and approval evidence with expiry, revocation, issuer, scope, replay/use and separation-of-duty checks; cryptographic verification enters through trusted evidence interfaces. Produce Allow, Deny or NeedsApproval plus bounded stable reason codes and an attenuation trace; unavailable operations are explicit non-dispatchable results. Bind decisions to immutable intent/submission, agency, exact resolved resource and parameters, policy/grant/approval versions, connector/provider realization, evidence, expiry and logical-use limits through existing records and typed constraints artifacts. Required worker controls must be qualified and enforceable; profile exclusions cannot be waived by approval. Evaluation-only registrations cannot self-promote, and quarantine blocks new starts while preserving separately authorized cleanup. Persist use reservations atomically and distinguish identical retries from new uses. Revalidate before dispatch and invocation; drift requires explicit reevaluation, and material operation changes require a replacement intent. WI-04/domain issues no public executable authorization handle.

This records approval, not implementation evidence. See the [inactive egress domain contract](../../conformance/wi-fabric-2/egress/README.md). WI-WORKER-G1, AQ integration, merged-target verification and parent acceptance remain independently due.
