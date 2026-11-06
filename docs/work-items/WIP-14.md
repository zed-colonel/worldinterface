# WIP-14: minimal embedded worker composition

Implementation candidate for WI-11/embedded-runtime. WP-029, WP-030 and AB-WI-019
have exact non-ignored registrations in the [embedded coverage matrix](../../conformance/wi-fabric-2/worker/embedded-coverage.json).
Packet `b57f32a001ff95ef14ac46d35e22a96c61c0f35896e17e0dc6e9762aebc18b88`
was accepted through 2026-11-11T00:00:00Z in run
`e191c2f5-10ab-43ce-993d-b25260c71985`. All 19 focused embedded tests and all ten
adopted checks passed on `7d93df6`; review found no blocking or major defect.
The remediation below changes covered source and needs its own exact-packet
acceptance before live evaluation. G4 remains open for WIP-16.

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
completion. Fresh deliveries wait for that lock without holding a database
transaction, then revalidate current authority and expiry. Contention with another
issuance or recovery operation cannot discard the fresh delivery opportunity.
Ordinary identical retries observe status; they neither redeliver nor
cancel a live attempt. Custody delivery receives the exact durable attempt.
Delivery and revocation remain serialized against administrative rotation.

Recovery distinguishes intent without attempt, an in-flight owner and an
abandoned attempt. An unattempted intent also needs a global custody fence and invalidation: its
original store may have progressed after the backup. Without that evidence it
remains Pending and recovery returns Unresolved. Confirmed evidence journals
Unresolved/Uncertain; it never proves that no delivery occurred elsewhere.
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
build report, avoiding recursive digest text in covered documentation. The separate
receipt and trusted anchor record actual operator acceptance; covered documentation
records historical accepted packets and results without claiming acceptance of
its own changed bytes.
All ten adopted checks on the committed candidate and qualifying review remain
required; CraftingTable alone integrates reviewed work.

## Accepted-packet evaluation follow-up

The operator accepted packet `4931dbe1e63e4532a0aed6e25d649a30b9fa67cc4dbfa51fa8806863fe023d6c`
through 2026-11-11T00:00:00Z in run
`59aa52f7-73cc-435c-ae1a-faca6eebff17`. The focused evaluation passed all six
credential tests and both offline-restore tests. Four embedded live cases failed
at admission: the trusted shared evidence adapter reused one placeholder
resolution reference across six different operation targets, returning the first
target's attestation for later operations. No successful live result is claimed.
The adopted runtime check reproduced the evidence failures and additionally
timed out its crash child while the private clone compiled the reviewed binaries.
The crash test now calls the existing source-only `prebuild` helper before
starting its boundary observation deadline, matching the other worker crash tests.

The follow-up publishes each exact target document as its own immutable evidence
artifact and rejects a reference registered for conflicting documents. A non-live
regression checks all six operation attestations through `verify_snapshot` and
rejects a substituted reference. This changes no production verifier, provider,
task, operation or limit. Inventory structure and dispositions are unchanged;
only source hashes needed refresh. The operator subsequently accepted replacement
packet `b57f32a0…ebc18b88`
in run `e191c2f5-10ab-43ce-993d-b25260c71985`. The complete focused evaluation
and all ten adopted checks passed on `7d93df6`, including coding/transform
execution and the cold-build crash case. The acceptance is recorded separately
in the receipt and host anchor; the prior failed evaluation above is historical.

## Review remediation: F-001, F-002 and F-003

F-001 corrects the packet and verification history in this note, the embedded
coverage matrix, the active ledger and WIP-13's successor note. The accepted
`b57f32a0…ebc18b88` packet and passing `7d93df6` checks remain historical evidence;
they do not authorize this remediation's changed bytes.

F-002 makes fresh delivery wait for exclusive delivery ownership. A deterministic
storage test commits two distinct issuance intents, holds the first inside
custody, and verifies that the second waits and then delivers exactly once.
Identical retries only observe the delivered result, and rebuild preserves both
attempts. Recovery retains its non-blocking lock so it cannot cancel live delivery.

F-003 scans both complete consumer sources, excluding only the scanner function's
marked region. Tests following that function are now checked as well. The adopted
contract check continues to parse both complete files using its syn visitor.

The explicit inventory/disposition proposal adds one test-only `PausedVault`
struct under the new storage regression, retained with a test-evidence disposition as AQ-free storage test
support, assigned to the existing `fabric-support` infrastructure family.
Existing entries and dispositions are preserved. No worker operation,
task, fixture bound or supervisor source changes. Source hashes and the packet
manifest are refreshed after this structural update.
The remediation's non-live build and checks are recorded under
`scratch/verification` in run `6e4e0635-a0a8-46f9-a988-4a46c09d61c9`; live
verification requires acceptance of the exact digest in the source-bound build
report, under the unchanged evaluation limits.
