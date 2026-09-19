# WI-ADR-003: Fresh store topology, SQLite transaction boundaries, backup/restore contract

Status: accepted

Owner: operator

Due: WI-02

Source: implementation plan §4; controller scope binding revision 4.

Approval: `d49b9e8a-f1b0-436f-b173-8e6333a37e7a`, recorded 2026-09-19T22:33:09.369Z by
`0698a39b-dda2-4870-8f3f-97ef94784ae9`. Exact supplied decision and provenance:
[approval record](approvals/WI-ADR-003.json), SHA-256 `1912ba6bcb31f0c4a32053fb5d6b0156458db1d5496ca92018881afee241de18`.

Choose separate schema-version-1 Fabric, Flow, ContextStore and optional Platform databases under one manifest-controlled root. This is an explicit choice among the layouts permitted by the plan. Validate root/store manifests before SQLite opening; require schema/profile/instance consistency and reject incomplete or unsupported initialization without modifying it. Commit authoritative records, projections, request mappings and the source-owned outbox atomically within each owning database, with expected revisions and uniqueness constraints. Preserve append-only history and deterministic replay. Publish durable artifact bytes before advertising references. Use exclusive offline backup/restore covering manifests, database and required WAL state, and referenced artifacts; validate the complete restore before publication. AQ-coordinated backup and runtime recovery remain integration work. Implementation and fault-injection/backup tests remain required.

Retained obligations: Implementation tests, independent review, integration and parent acceptance remain separate obligations.

This records operator approval, not passing implementation evidence. See
[domain contract](../../conformance/wi-fabric-2/domain/README.md) for implemented encoding,
state-machine scope, store profile and remaining gates.
