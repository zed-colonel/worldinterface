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
record bounds. The fresh-store profile advances `local-worker-11` to
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
After exact-digest acceptance, record only the actual decision in the separate
receipt/anchor before positive evaluation. Any further covered source change
requires a new packet and acceptance. All ten adopted checks on the committed candidate, independent
focused security review and CraftingTable integration remain required. Only
CraftingTable integrates; this branch is not pushed or merged by the implementer.
