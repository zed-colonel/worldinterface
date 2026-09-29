# WI-03 ingress domain

This independent library implements the WI-03/domain scope. HTTP and streaming
validators open no sockets. `BindingAuthority`, authenticated caller contexts and
release authorization are trusted composition seams. The production composition
now lives in `worldinterface-runtime::ingress`, with its AQ admission and signal
adapter. This domain package alone does not assert integration acceptance or
parent completion.

## Transactions and recovery

`StoreRoot::ingress_transact` uses Fabric `BEGIN IMMEDIATE` to read the tenant
revision, run the pure reducer, append an immutable canonical command, and replace
the derived tenant projection. Provider admission contains the observation,
ingress receipt, immutable binding associations, delivery/wake obligations and
ordering evidence in one journal entry. There are two physical write boundaries;
fault tests cover both for binding, observation/fan-out, attempt and acknowledgment.
A partial pure reduction cannot publish any writes. Snapshot/rebuild/open validation
replays the entire journal and compares the resulting projection. SQL uniqueness
on (tenant, revision), serialized mutation and reducer uniqueness enforce scoped
provider and principal/request identities. This deliberately bounded local
projection is not a production throughput claim.

The current manifest profile is `local-receipt-1`, schema 1, introduced by
[WI-05](../receipts/README.md). Previous development profiles `local-domain-1`
and `local-ingress-1` are rejected without modification. Separate Fabric, Flow,
Context and optional Platform stores remain intact. Wire records keep the 64 KiB
canonical bound; local collection snapshots have a separate 16 MiB ceiling.
Profile capacities also bound bindings, admission tombstones and conflict evidence;
deliveries and release evidence have a 10,000-entry ceiling. Hitting any bound
rolls back and applies backpressure. There is no silent tombstone eviction or
unsupported physical-erasure promise. Artifact bytes must be durably published
before admission. Failed admission may leave an unreferenced artifact, which is
safe recoverable garbage. Offline backup/restore covers the complete store root.
Journal recovery and maintenance safeguards from WI-02 integration are preserved
by WI-04. These ingress fixtures inject transaction failures and restart at
committed boundaries. Shared storage also has WI-02 integration hot-journal
recovery and killed-process tests; see [integration coverage](../../../docs/work-items/wi-02-integration.md).
Those tests do not establish AQ wake integration for ingress or WI-04 runtime
crash conformance.

## Provider and transport contract

The trusted profile binds deployment/tenant, provider account, canonical source,
event namespace, identity strategy, replay window, attachment disposition and
capacity. Its scope cannot change profiles in place. Stable provider IDs and
stream positions are preferred. Explicit content-window profiles normalize
producer time to a bucket and omit unstable event IDs/offsets for content identity;
this approximation intentionally does not advance cursor state on a suppressed
retransmission. Non-deduplicable profiles allocate an observation identity for
each arrival. Equal identity/content recovers the original observation/deliveries;
conflicts preserve protected hashes and cannot replace or wake. Reuse of an
already committed offset with different producer content also records conflict,
even when a content-window identity matches a different earlier observation.
Binding revision and arrival time do not alter provider identity. First admission
captures every active matching binding in ID order. Retransmission never enrolls
a later subscription.

HTTP uses HMAC-SHA256 over exact request bytes; scope/source/time are inside the
signed body. Signature metadata is exactly 32 bytes. Streaming verifies a bounded
signed session handshake and each frame's canonical `(session ID, exact bytes)`
pair. Session expiry, source, structure, offsets and bounds are checked before
admission. Cumulative acknowledgment is the next contiguous committed offset,
returned only after commit; a high-water mark does not imply a committed prefix.
Arrival order, provider offset, gap, out-of-order and reconnect evidence remain
separate. Fixed signatures in `transport-vector.json` were independently generated
with Python's standard-library HMAC, using a public test key.

Content and attachments use a ContextStore availability namespace derived from the
complete provider/source scope. A provider cannot substitute another provider's
artifact reference within the same tenant merely by knowing its hash.
Attachments are governed references. A trusted `QuarantineAll` profile requires
its exact quarantine policy on every attachment; provider assertions cannot remove
it. Subscriber inspection omits quarantined references until matching, separately
authorized release evidence is durably recorded. Typed commissioning and
role/stewardship references describe evidence only, never lifecycle authority.
Every typed profile reference, including the nested role attestation, is checked
for scope and durable availability. Profile evidence uses the tenant namespace;
a provider-scoped attestation must be a declared attachment with the trusted
profile's disposition. Foreign and undeclared references fail validation.
Delivery views redact an unreleased optional attestation without rewriting stored
evidence. Required content aliases of quarantined attachments deny the view until
release. These checks include quarantine recorded by other admitted observations.

## Delivery and inspection contract

Attempts are committed before returning the subscriber token and bounded wake
locator. `QueueWakePort` remains an inactive seam; its test-only sink receives a
stable wake key, distinct from provider identity and any future AQ SignalId.
Wake outcome cannot acknowledge subscriber ingestion. Retry keeps DeliveryId,
pins explicit attempt/timeout/backoff/retention policy and stops in dead letter
after exhaustion. A delayed issued token can acknowledge during backoff or dead
letter; it cannot reopen a closed delivery. Repeated valid acknowledgment returns
the original durable result. The mock subscriber commits its own SQLite row keyed
by DeliveryId before acknowledgment and safely ignores redelivery.

Tokens contain two independently generated UUIDv4 values, are delivery/subscriber
bound when hashed, and have neither Debug nor Serialize. Only verification digests
enter the privileged journal. Inspection and wake payloads omit token material and
digests. Owner invalidation or logical expiration rejects acknowledgment, including
repeats after expiration. Revocation denies content access and future attempts;
a previously issued valid token may still report ingestion that already occurred.
Inspection rechecks current binding status, audience, expiry and tenant/subscriber
identity. Clocks are explicit UTC epoch milliseconds and cannot move backward.

`Ingress::bindings`, `inspect_delivery` and `due_deliveries` are the governed
queries. Delivery views include provenance, producer identity, captured binding,
ordering, receipt, attempt outcomes and acknowledgment without secrets. StoreRoot
snapshot/journal access is privileged offline persistence machinery, not a public
subscriber API. Receipts attest transport validation and admitting authorization
versions; they do not assert truth, urgency, conversation membership or permission
to act.

## Evidence boundaries

`coverage.json` maps every obligation to executable cases; `source-evidence.json`
records adopted policy, approvals, predecessor receipts and retained assumptions.
Those records are guidance and provenance, not test results. Run both repository
scripts through `ct-check`, then `ct-act` for the domain and changed contract jobs.
The controller must review the exact candidate and target, perform any authorized
integration, and obtain fresh post-rebase/merged-target evidence. WI-AQ-005/006,
AB-WI-006/007/009/022 have candidate runtime tests; independent case review and
WI-AQ-G3 attestation remain integration obligations. Remote protection
remains operator-owned before first publication and final release approval.

Implementation grouping follows existing layering: pure commands/reducers in
`domain/ingress.rs`, one Fabric transaction/replay module in `storage/ingress.rs`,
and admission/delivery facade plus nested validators in the new ingress crate.
This avoids parallel public append APIs; the design's four logical transaction
boundaries are retained inside the shared journal mechanism.

## Review remediation coverage

F-001 is covered by HTTP and streaming nested-reference regressions (foreign tenant,
foreign provider, undeclared attachment, unavailable evidence, quarantine, wrong
release, authorized release and replay), plus content-alias disclosure checks.
F-002 is covered by content-window/occupied-offset collision, legitimate duplicate,
unchanged observation/outbox and persisted conflict replay checks.
F-003 is covered by three property tests, each running 256 generated command
sequences against `apply_ingress`. Independent models track binding revisions,
content and position occupancy, closure, retry attempts, token invalidation and
expiration; serialized command replay must recover the same state. These are
local domain assertions, not upstream integration results.
