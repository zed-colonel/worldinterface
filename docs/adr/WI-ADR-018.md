# WI-ADR-018: Daemon authentication profiles, tenancy, organizational grants, and secret brokerage

Status: accepted

Owner: operator

Due: WI-11

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals/WI-ADR-018.json), approval `e10f16b5-61f6-4d75-8e1b-9a5d913c58db`, 2026-09-29T16:30:53.721Z. Decision packet SHA-256: `14549785dd1a031f3762ae310e1c978a0fdd22650a4c7e58e73905cedefcf1a0`.

Coverage: full.

Adopt one shared Fabric application-service and authorization path for embedded, authenticated single-tenant daemon, and optional multi-tenant Platform profiles. Profile selection is explicit at startup. Local-development mode is visibly non-production, loopback-bound by default, and cannot issue production credentials or create Platform tenancy. Remote requests authenticate through a trusted adapter before construction of server-owned caller context; caller fields never establish principal or tenant identity. Resolve current tenant-scoped principal, actor, delegation, and acting-capacity bindings under WI-ADR-004, then apply WI-ADR-006's exact, default-deny authority intersection. Organization membership and administrative role labels confer no ambient effect authority. Scope principals, bindings, grants, resources, Flows, workers, signals, receipts, artifacts, and queries to tenant and applicable organization. Make policy and grant administration versioned, attributed, durable, and mediated by the same application services; enforce scoped receipt, evidence, and audit access. Keep quotas and connector concurrency in the Platform/Fabric control plane and scheduling fairness in ActionQueue. Use reference-based secret brokerage with custody outside ordinary domain rows: issue short-lived credentials only for exact authorized connector or worker audience, operation, resource, and run scope; persist custody-safe references and metadata, support rotation and revocation, and exclude secret material from receipts, telemetry, Flow definitions, worker transcripts, and artifact exports. Embedded and daemon routes and CLI commands use the same services without direct database mutation. The optional Platform metadata store follows WI-ADR-003's manifest-controlled layout. This decision fixes architecture, not credential-provider selection, implementation completion, provider qualification, or passing security evidence.

Rationale: I approve of this recommendation.

Retained obligations: Implement and verify the mapped WI-11 slices and complete parent integration, exact-upstream checks, review, and release gates. Remote protection and publication remain operator-owned at the policy milestone.

Consumer applicability: []

Alternatives: Caller-selected principal or tenant, ambient role authority and secret material in ordinary domain records are excluded.

Invariant impact: Tenant scoping, exact authority intersection and custody-safe secret references are preserved.

Verification: design authority only; implementation and adopted-check passage remain with the mapped WIP scopes. Approval establishes no test result.
