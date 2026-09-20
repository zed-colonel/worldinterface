# WI-ADR-002: Identifier generation and stable client-request idempotency mapping

Status: accepted

Owner: operator

Due: WI-02

Source: implementation plan §4; controller scope binding revision 4.

Approval: `1063789c-3852-46a4-9140-3fedeb689c54`, recorded 2026-09-19T22:27:11.622Z by
`0698a39b-dda2-4870-8f3f-97ef94784ae9`. Exact supplied decision and provenance:
[approval record](approvals/WI-ADR-002.json), SHA-256 `90fc2c3a245ecc3e2f007a284aa54b82a24858409e7b8b8353643bd138cfb326`.

Adopt server-generated UUIDv4 entity identities and content-addressed immutable evidence as recommended in the attached WI-02 investigation. Generate IDs outside pure reducers, persist them once and reuse them on retries. Keep artifact content identity separate from scoped access references. The unique admission namespace consists of deployment/tenant scope, authenticated submitting principal, submission class and ClientRequestId. Persist canonical submission digest and resulting domain identity atomically. Caller-claimed accountable identity is request content and cannot select the trusted admission namespace. Equal concurrent requests recover one identity; changed content under the same admission key fails without mutation. Implementation must test concurrent admission, conflicting reuse and restart stability. This decision grants no new authorization endpoint and claims no passing implementation evidence.

Retained obligations: Implementation tests, independent review, integration and parent acceptance remain separate obligations.

This records operator approval, not passing implementation evidence. See
[domain contract](../../conformance/wi-fabric-2/domain/README.md) for implemented encoding,
state-machine scope, store profile and remaining gates.
