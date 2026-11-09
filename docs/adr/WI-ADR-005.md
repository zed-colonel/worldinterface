# WI-ADR-005: Canonical resource URI grammar and resolver registry

Status: accepted

Owner: operator

Due: WI-04

Source: implementation plan §4; controller scope binding revision 4.

Approval: `18a3a800-fcb5-421a-a717-3f6556cbf0af`, recorded 2026-09-21T20:48:33.965Z. Exact accepted decision and provenance: [approval record](approvals/WI-ADR-005.json), SHA-256 `2471be57c19a1540b8caa03164d0e1817c317c19fa24916c7a60afae43f8c659`.

Use canonical resource URIs of the form wi-resource:sha256:<64 lowercase hexadecimal digits>, referencing immutable bounded versioned canonical target documents. Keep the registered namespace and observed resource version explicit in CanonicalResourceRef. Target documents bind tenant, owner and exact namespace-specific identity, including filesystem roots/components, network destination constraints, communication audience/visibility, or worker/lifecycle resource identity. Hash exact canonical document bytes under a schema that includes its kind and version; references confer no access. Keep existing OpaqueRef and envelope encoding unchanged. Resolve selectors using an immutable registry snapshot that pins namespace grammar, resolver version and connector descriptor. Reject unknown namespaces, ambiguous mappings, tenant or audience widening, and insufficient symlink, redirect, DNS, ownership or version evidence. Resolution must not perform the requested effect. Retain resolver evidence and registry identity separately from stable resource identity and require dispatch-time comparison against the authorized target. WI-04/domain uses deterministic supplied facts and fixtures; live resolution and connector-specific enforcement remain later adapter work.

This records approval, not implementation evidence. See the [inactive egress domain contract](../../conformance/wi-fabric-2/egress/README.md). WI-WORKER-G1, AQ integration, merged-target verification and parent acceptance remain independently due.
