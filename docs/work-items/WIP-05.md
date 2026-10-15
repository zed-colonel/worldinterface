# WIP-05 — Connector SPI, outcomes and reconciliation

Scope: WI-06 domain and embedded-proof integration, on published ActionQueue
0.2.0. WI-AQ-G4 remains open for WIP-07. This implementation is integration-only;
CraftingTable performs review and integration of the exact committed head.

## Implementation and decisions

The versioned protocol descriptor declares all six idempotency classes, resource
and schema contracts, isolation and secret references, provider key mapping,
retry boundaries, observation/cancellation support, and bounded evidence.
Capability registration rejects incomplete, contradictory and unknown
consequential declarations. Registry snapshots sort descriptors canonically and
bind their hashes and implementation versions into resolution and authorization.
The executor checks the installed descriptor and registry immediately before
invocation. Legacy declarations replay but cannot dispatch through the SPI.

Provider keys use the versioned `wi-provider-key:1` mapping over tenant, provider
scope, operation and stable effect identity. Resolution persists the selected
key before Start. Attempts, queue runs and dispatch generations are excluded.
An explicit retry after authoritative noncommit retains that key; a changed
operation requires a new linked intent.

The executor's invocation envelopes have private constructors and expose only
shared references. Preparation cannot dispatch. Current exact authorization,
resource observations, cancellation and the durable attempt fence are checked
after preparation. Execution records and receipts carry the pinned connector
execution and outcome classification. A terminal provider error with unknown
commit becomes Uncertain. Confirmed commit with failed required publication
also stays Uncertain, retaining the known commit; neither permits reinvocation.
AQ admission retains WIP-04's three attempts and Idempotent handler safety.

Reconciliation cases are part of the existing effect journal projection. The
uncertain outcome transaction creates the case, immutable execution receipt and
outbox obligation, including the complete serialized AQ admission request.
This avoids a second independent mutable journal. Original dispatch, attempt,
source revision, provider key, registry, policy hash, deadline, eligibility time,
observation budget, evidence, escalation and linked reconciliation receipts are
persisted. Admission retries reuse the stored bytes and identity after an
acknowledgment loss. There are no replacement generations.

Reconciliation runs as a separate AQ task. It spends observation budget before
a provider query, publishes evidence before appending its transition and linked
receipt, and uses deadline continuations between queries. Interruption spends
the recorded observation, without claiming noncommit. Deadline/budget exhaustion
closes automatic polling as StillUnknown while preserving effect uncertainty.
Terminal AQ recovery escalates an unfinished case. Confirmed commit/noncommit
cannot be overwritten by late observations. Only authoritative noncommit,
explicit retry policy, connector permission, remaining dispatch budget and fresh
exact authorization can permit another invocation. Compensation is a separately
authorized effect.

The generic execution profile is versioned and pinned by hashing its connector
execution binding. Existing worker profile checks are preserved. Original
execution receipts are located by dispatch, attempt and source revision;
reconciliation receipts have a separate versioned representation in the case,
linked to that immutable source revision. They never replace the original
uncertain receipt or its AQ disposition. New outcome/case journal entries use
version 2; absent additive fields serialize away, preserving historical version
1 journal digests and canonical receipts. Reopen, projection validation and
bundle backup/restore use the existing authoritative effect journal. Inspection
requires authenticated ownership and full evidence disclosure.

Evidence is reference-based. Connectors receive broker references, not secret
values. Automatic observations accept at most two bounded evidence blobs and
32 observations, so the accumulated case evidence has a 64-reference ceiling.
Original evidence remains reachable from the linked execution receipt.

## Proof and verification

`connectors/coverage.json` records WI-06 core obligations and the full-parent
obligations that remain deferred. The proof checker enforces exact runtime
manifest/package/non-ignored test references, and the case registry retains the
source matrix digests for AB-WI-012/014/020/023. Dedicated deterministic SPI
connectors cover externally keyed, naturally idempotent and non-repeatable
operations. Their SQLite provider state survives process death independently of
WI and counts actual commits.

The runtime suites cover classification, resource substitution and installation
drift, immutable envelopes, authorization revocation during preparation,
cancellation before/after dispatch, retry permission, bounded polling and
escalation, tenant/owner isolation, original receipt preservation, reopen and
backup/restore. Process cuts cover provider commit, the WI receipt/case
transaction, reconciliation admission acknowledgment, evidence publication,
case transition and AQ disposition. The AQ contract suite separately injects a
real uncertain WAL flush acknowledgment, replays the durable AQ events, and
retries the same admission identity. It also covers local invalid-proposal
fallback, remote correction with the same fence, wait capacity, lowered limits,
stale/uncertain writes and append-free terminal cancellation.

Verification logs live in the run's `scratch/verification`. Before handoff, run
all ten adopted checks through `ct-check --declared` on the committed head;
the final implementation report records their results. Supplemental focused
runs do not replace those checks. No check definition or AQ dependency pin is
changed.

## Structural inventory and deferred work

The structural disposition change retains the new protocol, capability, domain,
storage and runtime types in their existing foundation/runtime packages and marks
new test helpers as test evidence. It removes the superseded completion port
trait from the inventory. New source/test surfaces are added to the existing
catalog disposition with selective retention treatment; existing family owners,
destinations, removal gates and historical source coverage remain intact. Hashes
are refreshed only after that explicit structural update.

Reference filesystem/HTTP/communication connectors, HTTP adapter security policy,
WASM alignment, worker supervisor integration and legacy workspace deletion
remain deferred under the active plan. No gate closure, qualification or remote
publication is asserted here. Independent CraftingTable review is still required.
