# WI-ADR-011: HTTP redirect, DNS, credential, SSRF, and replay policy

Status: accepted

Owner: operator

Due: WI-06

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals/WI-ADR-011.json), approval `bb208f8d-50f6-42a3-b666-ca2d0c7d43f9`, 2026-09-29T16:19:14.088Z. Decision packet SHA-256: `66de4c9d1db930bdb1a0735386ee8374a6669bef05aa1d631e33fce76742866b`.

Coverage: full.

WorldInterface shall admit outbound HTTP through the versioned resolve, authorize, invoke and reconcile connector contract. Resolve and authorize the exact canonical scheme, host, port, path scope, method, tenant and credential purpose before dispatch. Centralized and sandbox egress is denied by default. Reject unsafe URL forms and loopback, link-local, private, metadata and platform-control destinations unless a separately governed capability authorizes a specific internal target. Validate all resolved addresses and connect only to an address that passed policy; re-evaluate changed resolution or connection targets to prevent DNS rebinding. Disable automatic redirects. Treat each redirect as a new target requiring fresh resolution and exact authorization within a bounded hop count; do not forward credentials, authorization headers or request bodies across origins by default. Inject credentials only through reference-based brokerage after authorization, bound to destination and purpose; keep secret values out of receipts, artifacts and diagnostics. Enforce TLS verification, protocol, port and path restrictions, request and streamed-response size limits and an overall deadline. Record canonical destination, resolver and policy versions, selected-address and redirect evidence, credential reference metadata, provider request identity and outcome without secret values. Derive provider replay keys from stable effect or operation identity under the declared connector mapping, never ActionQueue AttemptId; HTTP method alone does not establish idempotency. If timeout, connection loss, crash or incomplete response could follow external commit, persist Uncertain and reconcile without blind invocation. Repeat the effect only after evidence of non-commit, explicit policy permission and current exact authorization.

Rationale: I approve this recommended approach.

Retained obligations: Implement the approved policy in the mapped WI-06 slices and target HTTP adapters; complete descriptor admission, security and crash/reconciliation tests, exact AQ-pin and parent verification, independent review and controller integration at their mapped gates. Preserve separate release and operator-owned remote-protection/publication obligations under adopted policy version 1.

Consumer applicability: []

Alternatives: Automatic redirects, unchecked DNS targets, cross-origin credential forwarding and blind retry after possible external commit are excluded.

Invariant impact: Exact destination authorization, secret custody and stable effect replay identity are preserved.

Verification: design authority only; implementation and adopted-check passage remain with the mapped WIP scopes. Approval establishes no test result.
