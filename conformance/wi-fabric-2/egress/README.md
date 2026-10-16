# WI-04 inactive egress domain

Scope is `wi/WI-04/domain`, binding revision 4. This independent foundation
library implements server-owned agency, deterministic resolution/authorization,
and an atomic evidence journal with a test-only admission seam. It starts no
process, performs no network operation, activates no lifecycle, and implements
no production AQ adapter. WP-009–014 are fixture-plus-implementation evidence;
they do not qualify an ordinary provider or satisfy the parent exit gate.

## Trusted boundary and authority grammar

`Egress` accepts non-deserializable authenticated caller and verified snapshot
contexts. Their explicitly named trusted constructors are composition boundaries,
not credential verification implementations. Only test code supplies them here.
An eventual host must authenticate the caller and verify all binding, resolver,
grant, approval, qualification and execution attestations before construction.
No daemon route, ambient identity, caller JSON or production composition invokes
these constructors in this slice. Serializable journal snapshots are evidence,
not public executable authorization handles.

The version-1 policy grammar is deliberately closed and conservative: a finite
set of SHA-256 canonical **complete submission** demands at each authority layer.
It has no wildcards, ambient role union or executable predicates. The demand
includes class, selector, exact parameter artifact, purpose/information-use,
requested bounds and client request ID. Resource policy additionally lists exact
canonical target-document hashes. Grants intersect principal/capacity/delegation,
platform, resource and information-use sets. Every stage retains its attenuation
trace. Unknown versions, expired/revoked evidence, mismatched assertions and
missing authority deny. Approvals bind the entire intent, agency, resolution and
snapshot except approvals themselves; separation-of-duty checks compare requester,
accountable principal, sponsor and policy authorizer independently.

A unique live tenant/principal binding supplies actor, accountable principal,
sponsor and capacity. Delegations must form a bounded acyclic chain from the
accountable principal to the authenticated requester; every link attenuates
scope and expiry. Changing the agency of an existing intent requires replacement.
Exact demand sets are the simplest accepted typed grammar; broader selectors or
policy languages are intentionally absent, not implicitly supported.

## Resource and worker evidence

Bounded schema-1 target documents use
`wi-resource:sha256:<64 lowercase hexadecimal digits>`. Namespace and observed
version remain explicit. Filesystem roots/components/symlink chain, network
scheme/host/port/addresses/redirects/path, communication account/sender/audience/
visibility, and worker/lifecycle resource/operation identities are distinct types.
Registry and document attestations bind the entire target. Ambiguous aliases,
unknown namespace, stale evidence, cross-tenant facts, missing audience bounds,
or changed symlink/DNS/redirect evidence fail closed. No live resolver exists.

Worker fixture qualifications bind profile, provider/run/resource/operation,
executable/dependencies/adapter/configuration, image applicability, environment,
evidence, AQ/WI applicability, input/export manifests and controls. Requested,
advertised, applied and observed controls are separate fields. Each mandatory
control must be present, enforced and exact; the denial names the unmet control.
An approved fixture cannot waive unreviewed executables, protected input,
production credentials or cross-run test credentials. Unsupported operations
return `Unavailable` in a non-dispatchable decision. Qualifications expire and
can be revoked; every snapshot change invalidates previously prepared authority.
The only delivery API is `deliver_test`; there is no ordinary provider admission.

## Persistence and recovery

The current fresh-store feature profile is `local-flow-6`, schema 1; unsupported
profiles reject without modification or upgrade. The version-1 effect journal is
separate from historical generic batches and the ingress journal. Their replay
semantics remain intact. Generic records remain inert predecessor persistence;
new guarded effects cannot be mutated through generic batches or imported from
those records. No generic projection is consulted by the egress facade.

`BEGIN IMMEDIATE` atomically commits each canonical journal entry, derived tenant
state (including scoped request mappings and logical-use reservations), and source
outbox. The initial outbox has an empty dispatch list. Only exact current Allow
can reserve use and create a full immutable request. Request/task/key/execution
identity is persisted before test-port delivery. Stable grant/approval IDs retain
use counts across document revisions. Repeated preparation and lost acknowledgment
recovery use the identical persisted request and do not consume another use.
Acknowledgment sequence, task, key and digest must match before outbox completion.
Run discovery is separate and is not fabricated by this seam.

Reopen and rebuild replay journal commands deterministically and compare every
projection/outbox. Canonical entries are bounded to 64 KiB, tenant snapshots to
16 MiB, effects to 10,000, authority sets to canonical entry bounds, delegation
chains to 16, and attempts to 100. Capacity exhaustion rolls back without eviction.
Fault tests cover history, state and outbox writes for submit, resolve, authorize,
prepare and acknowledgment. Concurrency tests cover request identity and use races.
The integration target's journal recovery, maintenance locking and no-replace
backup publication safeguards are preserved from `03370fd5977f9ac92326f9b6c0b689cb24e2eca0`.
This slice's local tests do not reassert that target's AQ integration evidence.

## Lifecycle evidence

Phases summarize a typed aggregate. Inspection exposes the resolution failure,
authorization explanation, commit observation, detailed outcome/reconciliation,
admission acknowledgment and linked original intent. Queue admission is distinct
from execution; neither proves output publication or application acceptance.
Starting an attempt conservatively records possible commit. Failed/canceled
outcomes require non-commit evidence. Uncertainty requires reconciliation;
confirmed-committed reconciliation is still not `Succeeded`. Success requires the
exact authorized output manifest to be durably published plus boundary evidence.
Storage verifies scoped artifact availability before committing claims and again
on reopen/backup, including inputs, relied-on attestations and published outputs.
Pre-dispatch cancellation never fabricates Running. After preparation, queue
cancellation cannot prove non-commit. An uncertain retry requires provider-confirmed
non-commit, explicit policy permission and freshly evaluated exact authorization.
Compensation is a distinct linked intent with its own authorization and use.

## Verification and retained gates

`coverage.json` maps assigned cases and independent obligation tests. The domain
check runs all foundation tests, formatting, Clippy, build and all-features graph
isolation. The contract check and `wi-domain.yml` domain job remain required.
Native checks use the controller Cargo launcher; CI explicitly supplies its config.
Source/decision/receipt hashes in `source-evidence.json` are provenance, not test
results. Verification logs are retained in controller scratch, outside build caches.

Adopted repository policy version 1 / branch-settings version 1, adopted
2026-09-18T00:41:03.548Z, requires controller-local exact-revision review and
controller-mediated integration. Matching refs are observations, not protection
proof. Remote protections are operator-owned and due before first remote publication
and final release approval. ADR-004–007 are fully accepted. WI-WORKER-G1 remains
required for merge/verification; ADR-016's early clauses cannot substitute for it.
Retain AQ assumptions WI-AQ-001/002/004 and the applicable full register, WP-A01 and
WP-A02, production admission AB-WI-002/003/004, invocation/reconciliation adapters,
exact-profile provider qualification, actual merged-target/post-rebase checks,
integration verification and parent acceptance. This implementation advances none
of those controller-owned states and proposes no planning amendment.
