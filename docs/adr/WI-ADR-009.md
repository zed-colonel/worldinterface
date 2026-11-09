# WI-ADR-009: ContextStore versus artifact-store responsibility and publication atomicity

Status: accepted

Owner: operator

Due: WI-05

Source: implementation plan §4 and §5.6; controller scope binding revision 4.

Full decision approval `17643cb3-b956-4105-a34c-6fc23ad6a597` was recorded 2026-09-29T16:04:59.717Z for submission `0d8a5396-24a8-400f-8a2a-839c425cb639`. The decision-review packet digest is `219b91248a892178f4cec31b06543ad185ccd2a2ea18a02606caace3f6931abb`. The supplied `craftingtable-scope-evidence.json` and `design-recovery/context.json` record this approval; the earlier queued stub was stale.

## Decision

Context owns exact-byte immutable content, hashes, scoped availability, publication journal, retention and quarantine. Fabric owns boundary meaning, receipts, evidence links, outcomes, uncertainty and source outboxes. Artifact possession and availability grant neither access nor execution success.

Persist the scoped publication identity before writing and syncing bytes. Verify hash and size, then commit availability. Commit the Fabric outcome or uncertainty, receipt and downstream outbox atomically only after required outputs are durably published. Exact retries recover the same result; identity conflicts fail. Recovery resumes known publication obligations and never infers provider success from content. Corrections append linked records. Backup and restore must validate journals and cross-store references. Protected content requires a suitable backend; the local domain backend accepts non-sensitive fixtures.

Fabric and ActionQueue remain separate transaction domains. A durable Fabric terminal outcome may precede AQ completion without reinvocation. AQ completion without required Fabric state is corruption. Possible provider commit without durable outcome requires reconciliation.

## Retained obligations

This approval settles architecture, not implementation or verification. WI-05/integration and WI-AQ-G4 retain AQ completion convergence; WI-WORKER-G1, candidate and merged-tree checks, distinct security review, parent acceptance and release/publication obligations remain separately gated.
