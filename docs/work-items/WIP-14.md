# WIP-14: minimal embedded worker composition

Implementation candidate for WI-11/embedded-runtime. WP-029, WP-030 and AB-WI-019
have exact non-ignored registrations in the [embedded coverage matrix](../../conformance/wi-fabric-2/worker/embedded-coverage.json).
Positive live verification and the adopted runtime suite require acceptance of
the revised exact-source evaluation packet. Registration identifies executable
coverage, not a passing runtime result. G4 remains open for WIP-16.

## Composition and consumer boundary

`worldinterface_runtime::embedded::EmbeddedHost::install` consumes a real
`StoreBundle`, explicit process or recovery configuration, and trusted adapters.
It retains exclusive local host ownership. `application` borrows that host and
owns one AQ adapter. The opaque application handle exposes PlanService submission,
authenticated worker operations, scoped inspection, artifact import/read,
control-plane administration, credential preparation and reconciliation.
`Application` is the narrower consumer trait. Stores, executable paths, provider,
bootstrap and AQ management handles are private. Compile-fail rustdoc examples
check field privacy with otherwise valid generic bounds.

One immutable dispatcher covers all six process operations and reconciliation.
It selects the connector from the executor-created envelope's persisted binding,
checks the complete registry digest and descriptor, then delegates to the existing
Flow/effect executor and worker connector. Existing singleton connector consumers
retain their exact binding check. Worker launch history records exactly six bindings in the selected registry
style: the combined registry for embedded composition, or the existing singleton
bindings for historical trusted consumers.

Advancement polls authoritative worker observations, commits WI events, drains the
existing outbox and advances AQ. It adds no scheduler, WAL or cross-store
transaction. AQ remains crates.io **0.3.0**, authorized by WIP-07; imported 0.2.0
baseline evidence is unchanged. Recovery construction needs no evaluation receipt
or executable files. A trusted host supplies a private empty recovery workspace
when the original workspace is unavailable; that directory establishes no process
ownership and releases no capacity.

The consumer helpers import only the application API and value contracts. Trusted
setup is separate. Both the entry point and helper are checked, including import
syntax and private-handle field paths. This establishes an embedded API boundary
inside a trusted native Rust process, not isolation from arbitrary native code.
WP-A07 is resolved only for this embedded scope: no daemon, EXO binary, Platform,
remote provider qualification or hostile-code assurance is claimed.

## Authority, disclosure and grant uses

The facade bridges one authenticated identity and clock into WIP-13. Current
managed policy, unique binding and active grants replace external snapshot copies.
Evaluation approval is refreshed at admission and dispatch. The final worker
provider fence rechecks managed revisions under the existing worker transaction's
writer lock; revocation cannot race through a stale dispatch fence.

Effect use reservations append to WIP-13's authoritative control journal in the
same Fabric transaction as effect preparation. Stable grant identities and exact
logical dispatch identities prevent separate administration, issuance and effect
counters from each spending the same last use. Idempotent replay reserves once;
a dispatch can validate its own reserved last use without funding another use.
The legacy effect journal remains independently replayable. Foundation remains
AQ-free. A transactional race test proves effect and credential reservations have
one winner, including rebuild and identical retry.

Owning Flow, effect and worker subsystems register metadata for Flows, workers,
receipts, artifacts and their signal obligations. Reads require WIP-13's exact
object disclosure in the current tenant and organization. Artifact bytes require
an independent exact artifact read and the configured artifact policy. Receipt
inspection returns Summary only. The test authorizer explicitly installs exact
unpredictable Flow/receipt disclosure after execution, then the consumer checks
those objects through application methods. It supplies no execution authority
through consumer input.

Credential permits derive from verified immutable intents, current managed
revisions, the existing live worker, and the original run's authoritative wall
bound. Recipient selection belongs to trusted host configuration. Expired,
quarantined, completed or withdrawn runs and invalid external approval/evidence
withdraw permits. The worker metadata translation retains the entire credential
scope alongside the legacy worker binding, including organization, canonical
resource/version, recipient, operation, revision set and secret generation.
Historical delivery alone is insufficient to validate an active credential.

## F-007 delivery ownership and recovery

A durable attempt claim commits before custody contact. It binds issuance,
exclusive ownership epoch, the exact metadata digest, scope, expiry and secret
version/generation. A local delivery lock covers claim, custody interaction and
completion. Ordinary identical retries observe status; they neither redeliver nor
cancel a live attempt. Custody delivery receives the exact durable attempt.
Delivery and revocation remain serialized against administrative rotation.

Recovery distinguishes intent without attempt, an in-flight owner and an
abandoned attempt. An unattempted intent is rejected without contacting custody.
An attempted Pending or Unresolved issuance requires exact custody fencing
attestation before recording abandonment as Unresolved/Uncertain. Invalidation
then targets that exact possibly issued capability. Uncertainty commits before
invalidation, so a second crash can repeat only idempotent invalidation, never
delivery or promotion. Missing fencing evidence leaves status unchanged and
returns Unresolved. Confirmed invalidation still does not promote a delivery.
A restored copy's filesystem locks cannot fence the original host: custody must
enforce fencing across every delivery endpoint, including late original calls.
Legacy custody adapters provide no fencing evidence by default.

Focused non-live tests kill real WI test processes after intent, attempt claim,
custody success and delivery commit. They check retries, rebuild, restored
history, live-delivery exclusion, uncertainty, expiry and rotation. Secret material
never enters this port; only existing custody-safe references and bounded reasons
are journaled.

## Offline restoration

The existing `StoreBundle::backup` and `restore` algorithms already coordinate WI
maintenance and AQ writer exclusion, validate full manifests and references, and
publish a validated destination atomically. They require no alternative backup
mechanism. The new state participates in their existing journal replay and copy
paths. Storage profile advances to `local-embedded-16`, with explicit delivery and
host lock members; incompatible older layouts are rejected without migration.
The process catalog's expected profile advances with it.

AB-WI-019 additionally exercises a second independent AQ store as the explicit
WI-owned substitute for EXO cognition. Each writer excludes another writer of
its store while allowing the other store's writer. Restore preserves identity,
complete WAL bytes, WI artifacts and control history. Snapshot-only input, missing
artifacts, incompatible WI/AQ profiles, swapped identities and busy writers are
rejected without publishing a partial destination. Live coverage additionally
restores published worker results without the original process workspace and
retains quarantine/capacity when cleanup ownership cannot be established.

## Inventory disposition proposal and verification

The structural proposal adds 56 inventory entries and removes none. New embedded
runtime modules and tests are retained in runtime. New custody
ownership value types stay in protocol/domain; their journal, shared reservations
and exclusive locks stay in storage. The proof checker owns the coverage and
consumer-boundary checks. Existing dispositions are preserved. This is an
explicit structural inventory/disposition proposal for qualifying review, followed
by the required source-hash refresh; no structural change is hidden in that
refresh. Generated build outputs and verification logs are excluded from source.

Non-live credential, restore, consumer-boundary and shared-use tests, compilation,
Clippy and contract checks are recorded under `scratch/verification` in run
`a0d79d7f-0d79-4550-a3fc-0043f0fcb16c`. The revised packet recipe compiles all test
targets and builds/measures the fixture and helper without executing them.
The exact canonical digest is reported by the refresh script and the source-bound
build report, avoiding recursive digest text in covered documentation. Receipt and
trusted anchor retain the earlier operator decision until renewed acceptance.
All ten adopted checks on the committed candidate and qualifying review remain
required; CraftingTable alone integrates reviewed work.
