# WIP-13: embedded control-plane domain

Implements WI-11/domain under ADR-018 and ADR-004 in the independent AQ-free
foundation. No server, daemon, CLI route, production authentication adapter,
credential provider, runtime composition or listener is added. WIP-14 owns those
composition boundaries. Assigned WP/AB cases remain empty; the checked
[obligation matrix](../../conformance/wi-fabric-2/control-plane/coverage.json)
contains exact non-ignored foundation test references. Case owners and gate
statuses are unchanged.

## Trust and profiles

`Configuration::select` requires `Embedded` or `LocalDevelopment`, an explicit
host tenant and a credential lifetime in 1..=120000 milliseconds. There is no
default. Local development reports `non_production: true`, has no tenancy creation
command, and rejects production credentials. Unsupported daemon/Platform profiles
and unknown command fields fail deserialization. Loopback binding belongs to a
future transport; this item opens no sockets.

`Service` accepts an authentication port with an associated opaque session type
and a trusted clock. `AuthenticatedCaller` has no Deserialize implementation.
The extracted `agency::resolve_binding` helper preserves existing effect agency
semantics and hashes. Current server bindings select actor, accountable principal,
organization and capacity. Assertions can only match them. Unique current binding,
acyclic attenuating delegation and scoped evidence remain mandatory.

The facade returns metadata and scoped artifact references. It never returns a
StoreRoot, unrestricted snapshot or database handle. The underlying StoreRoot,
control-plane repository, bootstrap and registration methods remain trusted-host
persistence APIs. They must not become request handlers. A host registering an
object attests its authoritative ownership, digest and scoped artifact reference;
WIP-14 must obtain those from the owning subsystem, not from caller assertions.
All families include tenant and applicable organization. Exact read permissions
are required for each returned object, plus an enumeration permission for lists.
Filtering precedes counts and pagination. Cursors bind the authenticated caller,
organization, query, current authority and visible projection. Cross-tenant,
foreign-organization, inaccessible and missing references report Unavailable.

## Administration and durability

Existing Binding, Policy, Grant and authorization snapshot formats are preserved.
Administrative DTOs live in domain alongside those existing types; protocol
contains their scope, query and credential shapes. This avoids reversing the
existing protocol-to-domain dependency boundary.
Revision envelopes add stable identity, schema, revision, previous-envelope digest,
content digest, requester/actor/accountable principal/capacity/authorizer, approval
references, trusted time, request identity and exact command digest. Explicit
one-time trusted-host bootstrap records the operator and initial ownership facts.
Later grant/binding/policy references require already registered scoped principals.
Membership and role labels supply no permissions.

Administrative authority intersects agency, platform, resource, information-use
and current active grants. Ordinary commands must cover the exact mutation and
every authority hash being issued. Revoked, expired and exhausted grants supply
no authority and do not disable another current grant. All remaining grant bounds
still intersect; there is no union of roles or grants. Approval checks bind mutation, policy and binding, enforce expiry,
revocation, use limits and separation of duties. Revoke a grant or binding by
writing a new version with `revoked: true`; revoke a policy by attenuating its
sets. Stable tenant/issuer/grant keys retain use counts across revisions.
When new objects or operations need authority after bootstrap, the trusted host
calls `authorize_control_administration` with separately authenticated policy
authorizer and requester contexts. It journals one exact command under the current
policy and binding, expiring within 120 seconds. This path can extend policy,
binding and grant ceilings and recover a requester with no usable grant; it
confers no ambient permission. It cannot be invoked through `Service`, and caller
DTOs have no attestation field. Execution still validates scope, registered
principals, allowed grant issuers, expected revisions, approvals, separation of
duties and provenance. Resulting revisions reference the authorization in their
attribution. Policy or binding drift invalidates outstanding attestations.

Expected revisions, mutation request mappings, attributed history and logical-use
counts commit in one Fabric transaction. Reads consume no uses and retain no
retry outcomes or journal events; repeated reads evaluate the current projection.
Only ordinary administration and credential issuance reserve grant uses, plus
any required approval/permit reservations. A reserved last use remains valid for
its own delivery and identical retry; it cannot fund another issuance. Identical administrative retries acknowledge
the original outcome even after self-revocation, without authorizing another
mutation. Conflicting request reuse fails. Reads and credential retries require current access; no retry repeats custody
delivery.

The append-only control journal and its projection participate in initialization,
deterministic replay, comparison, rebuild, offline backup and restore. Store
profile advances from `local-worker-13` through `local-control-plane-14` to
`local-control-plane-15`, retaining
schema 1 and rejecting older layouts rather than silently migrating them. The
process catalog's expected profile advances with it; check definitions do not
change. The projection keeps a small header and indexed entries for history, objects,
permits, issuances, revocations, uses, mutation retries and authorizations. Each
entry retains the existing encoding bounds, while aggregate growth has no shared
16 MiB ceiling. Mutations update changed entries; reads add none. Replay, rebuild
and projection comparison include every entry. The pure reducer still evaluates
an in-memory aggregate; large-scale paging/index optimization is not claimed.
Existing stores require a fresh layout; no silent migration is provided.

## Custody and the WIP-14 contract

Trusted ingestion calls `prepare_permit` with the existing verified authorization
snapshot, immutable effect intent, host-selected recipient and authoritative run
deadline. It resolves the resource and reruns existing exact authorization,
including worker profile exclusions. Managed policy/binding and every usable grant must equal those facts. Retired
grants contribute no authorization evidence, but remain in the complete revision
binding so later changes still invalidate stale permits. A permit binds tenant, organization, principal, worker/connector
recipient and provider, operation, canonical resource/version, run, authorization
digest, managed revision digests, credential class, secret version and revocation
generation. Every request dimension must match; only expiry may attenuate. The revision set
must equal the complete current applicable binding/policy/grant set, including
newly added grants, both at intent creation and immediately before delivery.

WIP-14 must call trusted permit withdrawal when a run, approval or external
evidence is withdrawn; withdrawal is journaled and blocks new issuance and
pending delivery without discarding historical outcomes. Delivery retains the
original permit identity, including when several permits share one authorization.

The issuance authority hash covers the capability ceiling, excluding recursive
policy/authorization hashes, secret version/generation and expiry. Rotation
therefore preserves the authority ceiling, while the exact new version and
generation remain mandatory in the newly verified permit. Fresh exact revisions, authorization and
all expiry bounds are independently checked against the verified permit. The
120-second ceiling can be shortened and is bounded by policy, grants, bindings,
delegations, approved resolution, worker qualification, secret and run expiry.
Production worker credentials remain excluded by the current worker profiles,
independently of the control-plane profile. Production connector credentials
require Embedded plus an exact verified permit and explicit class authority.

The custody adapter delivers directly to the trusted recipient and enforces the
complete binding, expiry and generation; no API carries credential bytes. Only
bounded reason codes cross this port. Adapters must discard raw provider errors
and must not log tokens or secret-derived diagnostics. The new scope is richer
than worker `CredentialBindingV1`; adding a domain-to-worker dependency would
violate foundation layering. WIP-14 can translate the verified metadata at its
trusted delivery boundary without weakening the binding.

Intent commits before custody interaction. A second immediate Fabric transaction
revalidates current state and serializes delivery against administration and
rotation. Successful rotation/revocation invalidates prior generations at custody;
new generation delivery is blocked until custody confirms. Failed/uncertain
revocation stays unresolved. A concurrent identical retry observes Pending without changing it or competing
for delivery. Only the call that committed intent owns the delivery attempt.
Pending after interruption also remains Pending and never repeats automatically;
it requires host reconciliation and is not evidence of delivery. Confirmed
backend uncertainty is recorded as Unresolved. Duplicate pending revocation
requests likewise cannot invalidate the original call. An operator reconciliation mechanism
and provider qualification are future composition work; retry alone grants no
active capability. Custody must quarantine/invalidate possibly issued credentials
on uncertainty. WIP-14 must also compose this administration state and logical
use accounting with effect execution; this item does not replace runtime ports
or historical effect journals.

Secret-exclusion evidence injects a sentinel only inside a hostile test custody
adapter, then scans actual serialized public results, debug output, journal,
projection, artifact publication and offline export bytes. The broker has no
write path into receipts, telemetry, Flows or worker transcripts. Existing record
serialization/export paths are exercised with returned safe references. This
proves the brokerage boundary for those paths; it does not assert that arbitrary
host-supplied artifact bytes or identifiers are secret-free.

## Inventory and verification

The structural disposition proposal adds 56 inventory entries and removes none.
It retains the new foundation control-plane
models, trusted persistence adapter and tests in their owning foundation layers;
it extracts reusable agency claims without removing existing inventory entries.
This is an explicit WIP-13 inventory/disposition change for qualifying review,
not a source-hash-only refresh. Hash refresh follows that structural update.

Focused control-plane tests cover profiles, caller forgery, all scoped families,
queries/cursors, role denial, exact approvals, revisions and logical-use limits,
concurrent updates, worker/connector scope mismatches, stale authorization,
expiry, rotation/revocation, uncertainty, hostile custody errors, rollback,
projection tampering, replay and offline restore. Run all ten adopted checks on
the committed candidate through `ct-check --declared`; their authoritative
results belong to CraftingTable's exact-head records. Logs for this implementation
run are under `scratch/verification` in run
`8fce9c71-7dd8-4c4a-9095-a54647b700bf`. Qualifying review and CraftingTable
integration remain required. No gate closure, upstream result or production
provider qualification is asserted.


## Evaluation acceptance dependency

The adopted runtime suite on `43c8635` reached the existing reviewed process
fixture and rejected `evaluation source drift` in
`ab_wi_021_awaiting_release_hold_and_resume_keep_durable_worker_capacity`.
WIP-13 changes foundation files covered by that exact-source packet. The
candidate manifest/request is refreshed without modifying the previous approval
receipt or trusted host anchor. The source-bound non-live build is required
before presenting the candidate digest. Renewed operator acceptance of that
exact digest is required for positive worker execution and the adopted runtime
suite. No permission or result is inferred from earlier packet acceptance.


## Review remediation

F-001 adds exact attributed host authorization beyond bootstrap and verifies
post-bootstrap receipt reads/queries and issuance after rotation. F-002 removes
read persistence and replaces the aggregate blob with indexed bounded entries;
regressions cover 1,200 full pages, subsequent grant revocation, replay/reopen and
a projection larger than 16 MiB. Five new inventory entries retain their owning
foundation layers and regression evidence; no existing disposition is removed. F-003 preserves read budgets, excludes inactive
grants and verifies recovery plus the reserved last use. F-004 resolves only the
current caller's active bindings and tests 261 global bindings. F-005 pauses the
original issue between intent and delivery and proves an identical concurrent
retry leaves it Pending, then observes one successful custody call. F-006
corrects the Permit trust comment: serde is for trusted journal facts and does
not authenticate them. No additional public operation or listener is exposed.

The added declarations/tests and the storage profile revision are an explicit
inventory/disposition proposal for this remediation's review. The obligation
matrix preserves its authoritative WI-11/domain scope and exact test references.
