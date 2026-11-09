# WI-ADR-001: Protocol version envelope, canonical serialization, hash input, unknown-field rules

Status: accepted

Owner: operator

Due: WI-02

Source: implementation plan §4; controller scope binding revision 4.

Approval: `e4f66a95-3338-4c6e-8990-9b64e7a85dbf`, recorded 2026-09-19T22:20:04.952Z by
`0698a39b-dda2-4870-8f3f-97ef94784ae9`. Exact supplied decision and provenance:
[approval record](approvals/WI-ADR-001.json), SHA-256 `ffc652dc4aeb0af4708da435a3836d48e4f34bccd0759cd5df10bb5fcb83e099`.

Adopt the canonical JSON profile recommended in the attached WI-02 design investigation: a typed, versioned envelope identifying the contract, canonical profile, record kind, schema version and body; UTF-8 with deterministic key ordering, preserved array order and Unicode content, and a specified escaping convention; absent optional fields omitted and null rejected unless permitted; bounded integers and schema-defined decimal strings; UTC timestamps with nine fractional digits; rejection of duplicate keys, unknown fields, unsupported versions and invalid representations before hashing. Use domain-separated SHA-256 over canonical envelopes excluding their own derived digest/identity and signatures, and hash artifact contents as exact bytes. Specify exact hash inputs and golden bytes/digests for idempotency, authorization binding and receipts. Routine encoding details within this profile must be documented and fixture-tested during implementation. Detailed effect lifecycle semantics remain ADR-007 and final receipt/signing decisions remain ADR-008. This decision does not claim implementation or verification completion.

Retained obligations: Implementation tests, independent review, integration and parent acceptance remain separate obligations.

This records operator approval, not passing implementation evidence. See
[domain contract](../../conformance/wi-fabric-2/domain/README.md) for implemented encoding,
state-machine scope, store profile and remaining gates.
