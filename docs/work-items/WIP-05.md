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
acknowledgment loss. The acknowledgment pins the sole AQ run as well as the
admission sequence; copied payloads on other runs cannot spend observation
budget. There are no replacement generations.

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

## Review remediation

- **F-001 (fixed for reviewer verification):** `VerifiedCaseAction` is a
  non-deserializable host assertion, following `VerifiedReconciliation`. Only
  trusted runtime admission, observation and recovery paths attest transitions.
  Storage and domain commands require that assertion; the principal-facing
  `Egress::apply` rejects case transitions even if incorrectly attested. Raw
  Start actions and deserialization fail to compile. The regression
  `reconciliation_principal_cannot_forge_case_transitions_or_enable_retry`
  rejects Start, every observation result, admission preparation and acknowledgment,
  keeps PermitRetry unavailable, and verifies one non-repeatable provider commit.
- **F-002 (fixed for reviewer verification):** reconciliation journal appends retry
  bounded optimistic/SQLite contention while preserving the original case
  revision. Journal ordering uses the later of the host sample and the last
  journal timestamp. Reads and evidence publication also retry contention.
  Transient storage/clock errors return retryable AQ failure; binding and integrity
  errors remain terminal. Persistence errors cannot become provider-evidence
  rejection. `reconciliation_journal_contention_preserves_provider_confirmation`
  forces real same-tenant writes between validation and append at Start and
  Observe, then asserts one confirmed observation, AQ Completed and no escalation.
  `reconciliation_retries_transient_clock_and_rejects_binding_errors` verifies
  an admitted AQ run retries once after clock failure and completes the same case.
- **F-003 (fixed for reviewer verification):** when cooperative invocation returns
  under cancellation, the executor calls the connector's cancel phase if its
  pinned descriptor supports it. Connectors must return promptly on the token.
  The typed cancel result is persisted as scoped `wi.connector-cancel:1` evidence.
  Requested, unsupported and unavailable cancellation preserve uncertainty;
  only ConfirmedNotCommitted can establish cancellation before commit. No cancel
  result can replace a previously confirmed commit. The regression
  `spi_cancel_phase_records_observation_and_preserves_commit_truth` covers these
  results, unsupported declarations, cancellation before dispatch and replay.
- **F-004 (fixed for reviewer verification):** Phase::Failed describes a terminal
  request failure; inspection's separate `commit` field remains
  ConfirmedCommitted when the provider committed. The immutable receipt records
  TerminalFailure and `committed_at`; AQ returns
  `wi.effect.terminal_provider_failure_committed`. Reopen convergence checks that
  code. `spi_terminal_failure_after_commit_is_visible_and_cannot_reinvoke` checks
  receipt, inspection, AQ failure, blocked retry/start, replay and one actual
  non-repeatable provider commit.
- **F-005 (fixed for reviewer verification):** `ProviderKeyMapping.parameter`
  documents the provider request field/header carrying the key. It is transport
  metadata; `provider_scope` identifies the key namespace. The existing key test
  confirms that renaming the parameter preserves replay identity while changing
  the pinned descriptor hash.

The structural inventory change adds `VerifiedCaseAction`, the private
`TransientReconciliation` error classifier and the regression's `FlakyClock`
fixture to their existing foundation/runtime/test dispositions and catalog
family. It preserves family ownership, destinations and removal gates. The
source-hash refresh follows these explicit structural additions.

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
