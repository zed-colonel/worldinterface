# WIP-11 — Scoped artifacts, quarantine and provider seam candidate

This candidate implements WP-027, WP-028 and WP-031. The exact non-ignored
manifest/package/test bindings are in
`conformance/wi-fabric-2/worker/artifact-seam-coverage.json` and the proof ledger.
Mappings identify implemented evidence; they do not assert evaluated passage.
WI-WORKER-G4 remains open for WIP-16. Ordinary provider use remains unqualified.

## Artifact and publication boundary

The consumer port uses `ArtifactLocatorV2`: provider, run, admitted realization,
logical reference, content digest and size. The process adapter no longer scans
runs by logical name or prohibits another run from using the same logical name.
WI binds the exact input references and derived export/verifier policy before
start. The authorized effect contains the exact nested input references; admission
checks its input artifact and authorization digest before resolving those bytes,
then checks the request's input digest before import. Retries must reproduce the
same policy. References confer no authority independently of trusted caller scope.

The common collector checks strict bounded manifests against that immutable
policy, validates the entire batch, computes findings using the WI registry, and
only then retains bytes. Registry version 1 supports content integrity, a bounded
strict JSON object schema, and UTF-8. Provider `passed_verifiers` strings do not
establish passage; unknown claims and failed required verification reject. These
checks do not judge application work quality. Flat process filenames use retained
directory capabilities, no-follow opens, regular-file/UID/link/device checks,
before/after inode and metadata comparisons, and a final directory-entry check.
Collection also rejects changed directories or unexpected entries. Tree cleanup
must be confirmed first. Same-user hostile-code containment is not claimed.

The collection state is Absent → Prepared → Published, or Quarantined. Prepared
binds operation, policy and manifest digests, findings, and exact private snapshots.
Snapshots use an immutable, run-keyed Fabric SQLite BLOB table, outside the worker
workspace and normal artifact index. This implements the proposed bounded private
snapshot without another filesystem namespace. Raw bytes never enlarge canonical
record bounds. Lifecycle boundary evidence advances to `wi-worker-lifecycle:2`
because its aggregate now includes required collection fields; the deterministic
acknowledgement and public receipt envelopes retain their existing encodings.
The fresh-store profile advances `local-worker-11` to
`local-worker-12`; prior profiles require fresh stores. Foundation storage remains
independent of worker and AQ crates.

Publication identities include the complete WorkerBinding, prepared result and
logical reference. WI stages and publishes every export, then a manifest binding
owner/run/realization, references and findings. Only available publications can be
committed as Published. The Fabric transaction rechecks cancellation/quarantine
and writes the collection plus its durable event/outbox obligation atomically.
Completion requires Published and successful exit. Even zero exports need an
explicit validated collection. The generic deterministic acknowledgement and
executor publication checks remain intact.

Recovery reads exact private snapshots, never mutable workspaces, and resumes
idempotent staged publication. Missing/corrupt snapshots quarantine. Interrupted
runs without a prepared obligation remain conservatively quarantined. Reports
retain binding, operation, policy/manifest digests, fixed rejection codes and
bounded verifier findings, without echoing arbitrary manifest content. Cleanup
releases capacity without deleting quarantine reports or unsafe allocations.
Consumer export access checks exact tenant, principal, provider, run and realization
against the published collection, including after provider release/reopen.

## WP-A05 and seam simulation

`AdmittedProvider` is a nonblocking internal boundary for admitted submission,
acknowledgement observation, lifecycle observations, scoped bytes, manifests,
stop and cleanup evidence. The asynchronous WorkerPort contract remains separate
from acknowledgement. No async runtime is blocked by a synchronous connector.
An uncertain submission is reconciled by querying acknowledgement, never by a
replacement launch. Prepared collections can reconcile without provider memory.

`tests/support/worker_seam.rs` is WI-owned **seam simulation**. It exchanges bounded
messages with a delayed background receiver and opaque provider/run handles. It
receives no path, PID, store handle or workspace. Duplicate/reordered observations
use the same Fabric intake/outbox and real AQ composition as process fixtures.
Its provenance is the reviewed fixture source plus an explicit no-helper marker,
verified independently by test-host composition. The existing realization
manifest's executable slot carries this source artifact in test composition; it
is not represented as an ELF or process helper. No production remote backend,
remote-provider qualification or isolation qualification is claimed. This
resolves WP-A05 at the contract seam only.

## Verification and approval checkpoint

Non-live validation covers malformed/oversized/wrong-scope manifests, missing and
unexpected exports, conflicting names, traversal, digests/sizes, unknown and failed
verifiers, cancellation, cross-scope reads, replay/reopen and snapshot corruption.
Deterministic descriptor tests cover replacement/write races, links and FIFO.
Nine non-live crash cuts cover snapshot retention through collection commit.
Eleven additional positive-evaluation cuts cover real collection, acknowledgement,
receipt and AQ disposition ordering. Positive process/seam tests are compiled but
must not execute until the revised packet is explicitly accepted.

The implementation uses crates.io AQ 0.3.0 under the recorded WIP-07 migration;
imported 0.2.0 source/baseline records remain unchanged. Structural inventory
additions have explicit candidate dispositions for this item. The changed process
supervisor source has a proposed exact-source disposition in
`conformance/wi-fabric-2/worker/supervisor-disposition.json`. The candidate contract
tool pin matches those exact bytes for review. Neither that pin nor a passing
contract check grants evaluation approval; activation remains conditional on
explicit exact-packet acceptance and qualifying source review.

The revised `evaluation-manifest.json` includes WP-027/028/031 and all candidate
source/build/check data. Its exact canonical digest is recorded by the packet
refresh/build output in run scratch. The old receipt and trusted approval anchor
are unchanged and do not authorize this candidate. See `evaluation-request.md`.
The two derived inventory files that contain the separate anchor's source hash
are also excluded from the packet to avoid an indirect approval cycle. Static
inventory dispositions remain bound; the contract checker still validates the
complete generated inventories after actual trust provisioning.
After exact-digest acceptance, record only the actual decision in the separate
receipt/anchor before positive evaluation. Any further covered source change
requires a new packet and acceptance. All ten adopted checks on the committed candidate, independent
focused security review and CraftingTable integration remain required. Only
CraftingTable integrates; this branch is not pushed or merged by the implementer.

## Follow-up after authorized evaluation, 2026-11-04

The operator explicitly accepted packet
`703c30858557e209c9843e1f19a037c9c48f955c58bbc79b2bb67f0193adcec2`, including
its supervisor disposition, for up to seven days. Commit `1c8cc27` records that
actual guidance in the separate receipt and test-host anchor. Its five focused
live tests passed, including the eleven collection/acknowledgement/receipt/AQ
crash cuts and the asynchronous seam simulation. Nine adopted checks passed.
The adopted runtime suite failed AB-WI-021; continuing the remaining targets
also exposed a stop-before-start allocation cleanup regression. Both failures
were reproduced alone under hard timeouts. Evaluation reports are retained in
the implementation run's verification directory; no whole-suite pass is claimed.

This follow-up keeps AB-WI-021's two concurrency-key wait policies and replacement
capacity checks, but no longer asks a stopped worker to publish a collection as
successful work. It explicitly checks stopped/cleaned-up state, absent publication
and absent completion around release. The process release path again removes a
confirmed unlaunched, unquarantined allocation. It still retains quarantined
allocations and stopped allocations that may have executed. The existing
stop-before-start test retains its original cleanup assertion.

These changes require a new exact packet and proposed supervisor source pin.
The prior actual receipt/anchor remain unchanged and do not authorize these
follow-up bytes. Non-live validation and compilation are permitted; positive
evaluation and the adopted runtime suite await renewed exact-digest acceptance.
The case mappings, task/operation/bound/environment scope and ordinary-use
exclusions are unchanged. Qualifying review and CraftingTable integration remain
due, and WI-WORKER-G4 remains open for WIP-16.

## Cleanup-confirmation assertion follow-up, 2026-11-04

The operator accepted follow-up packet
`2b8be6d80b522c4b3bf6776009067ef58030f97ed97ef8a04b7f0704148fc921`, including
its updated supervisor disposition, for up to seven days. Commit `1e369de` records
the actual guidance in the separate receipt and test-host anchor. All five focused
live tests passed, as did the stop-before-start cleanup regression. The adopted
runtime check and an isolated reproduction found that AB-WI-021's added assertion
expected WI durable cleanup confirmation before Release. The provider cleanup
report and Exited observation at that boundary do not commit CleanupConfirmed;
Release does. This was a test assertion error, not missing provider cleanup.

The corrected test checks provider exit/cleanup, durable StopRequested/Exited and
continued WI capacity reservation before Release. It verifies durable cleanup
confirmation and released capacity after Release. It keeps both AQ wait policies,
replacement-capacity rejection before release and acceptance afterward, and the
assertions that stopped work has no collection publication or completion. Runtime
and supervisor sources are unchanged. This correction changes packet-bound test
bytes, so positive re-evaluation and the full adopted runtime check require renewed
exact-digest acceptance. The actual receipt/anchor remain on the accepted packet.

The remaining approved runtime targets completed. A receipt-proof fixture once
observed RetryWait instead of Completed; its isolated reproduction and full
eight-test target recheck passed. That transient result is retained in the run evidence and is not asserted as a suite
pass. Qualifying review and CraftingTable integration remain due. No ordinary-use,
remote-provider, isolation or WI-WORKER-G4 closure claim is added.
