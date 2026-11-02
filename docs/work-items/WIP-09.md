# WIP-09 — Local process Agent Host adapter

Candidate implementation of the accepted Linux `process-managed-proof-1` design
under WI-ADR-016 (approval `10a13b9c`). It implements the existing WorkerPort;
foundation stays AQ-free and its worker wire types remain unchanged. This note
records implementation evidence, not independent review or live evaluation approval.
Only CraftingTable integrates reviewed work. AQ remains crates.io 0.3.0; imported
0.2.0 provenance and adopted check definitions are unchanged.

## Host composition and authority

`worker_process::ProcessWorkerPort` is bound to one authenticated HostScope
(tenant and principal). A trusted host supplies a private owned directory,
digest-reviewed helper and executable registry, a trusted Unix clock and an
AuthorizedRun. The latter contains the exact request/ResolvedWorkerV1 pair for
each allowed operation, scoped input bytes/digests, typed task, exports, controls
and non-secret credential binding. Registry membership and profile eligibility
alone grant no execution authority. Every supported submission revalidates exact
request, executable, adapter source, dependency lock, configuration, input,
authorization and controls bindings; observation and artifact reads reauthorize
scope, expiry and revocation. Credentials cannot widen the admitted operations
or export scope. WIP-10 must construct these capabilities from WI admission;
this API is trusted composition, never a request-body authorization parser.

Executable review opens without following symlinks, bounds the ELF bytes to
64 MiB, hashes the bytes actually read, copies them into a sealed memfd and
executes through that descriptor. Replacing or modifying the source pathname
cannot change the executable capability. The helper and fixture entry points
compile the same adapter source directly, keeping unrelated AQ/store code out
of the helper executable. ResolvedWorkerV1 records the observed fixture digest;
its dependency hash binds the runtime lock and adapter hash binds adapter sources.
Host setup separately verifies the helper's actual executable digest.

Provision allocates an unpredictable private directory under the approved root;
run/request identifiers are never path components. Input, work and export areas
are separate. Only fixed fixture input destinations are written. The reviewed
coding fixture edits a copy of the imported sample Rust project, exporting a
rename patch/report; the byte fixture applies a bounded XOR transform.
The sample is mirrored as runtime data assets and tested byte-for-byte against
the imported archive, whose production inclusion is forbidden by the existing
source-isolation checker. This is a code-forced placement detail of the accepted
copying task; the checker and archive remain unchanged. Neither
uses a shell, network or consumer dependency. Task arguments are typed, finite
and constructed by the adapter. Only WI_PROFILE is installed after clearing the
environment. Standard streams are explicit. Child-side close_range(CLOEXEC) and
dup2 preserve the exec-error pipe until exec and pass only the deliberate
bootstrap descriptor (plus the sealed fixture capability to the helper).

## Bootstrap, supervision and ownership

Parent-created socketpairs carry version 1 / big-endian u32 length frames.
The incremental decoder validates the five-byte header before reserving payload
memory. Limits are 32 KiB per frame, eight received messages, 128 KiB received
traffic per decoder, two seconds per handshake and 200 ms per send. Fragmented,
truncated, malformed and unsupported frames fail closed. Logs use separate pipes.
There is no unbounded message queue. One 256-bit OS-random credential is issued
at Start, carried only over bootstrap and retained only in memory; authorization
requires at most a two-minute remaining lifetime. Its run-bound proof is checked
before work starts. The independent ownership nonce is also OS-random. No secret
is persisted or included in diagnostics, records or consumer artifacts.

The adapter pins its direct helper before sending launch authority. The helper
becomes a subreaper, pins the fixture before enabling its work, then discovers
and pins descendants. Records bind provider/run, allocation, nonce, boot and
birth identity and the stable pidfd's observed target. Ownership is revalidated
before adoption or signalling. Signalling exclusively uses pidfd_send_signal;
direct-helper reaping uses waitid(P_PIDFD), so PID recycling cannot retarget a
wait either. There is no numeric kill or process-group fallback in production. Only the
standalone helper changes subreaper state and reaps its own children.

A supervisor thread runs independently of observation polling and futures. The
helper drains stdout and stderr fairly with nonblocking reads, including pipes
held open by descendants. Stop first requests graceful shutdown, then signals
verified members with SIGTERM and escalates after 200 ms to SIGKILL, within a
one-second tree cleanup budget. Discovery/handle retention is capped at 256
members. Confirmed leader exit and confirmed cleanup are separate report fields.
Unknown ownership, exhausted discovery/deadlines and unconfirmed exits produce
explicit bounded findings and quarantine. Parent failure handling gives the
helper a bounded cleanup interval before escalation against its own stable handle.
Child guards cover registration, bootstrap and thread-creation failures. Port
drop requests stop and joins monitors; no detached monitors remain.

## Bounds, artifacts and operations

Wall time is finite and at most 60 seconds. Retained logs plus collected exports
share the admitted output bound, at most 4 MiB. Protocol traffic has separate
fixed bounds above; it is not user log output. Inputs are at most 1 MiB, exports
at most four files, and a port retains at most 64 admitted runs/tombstones.
Output accounting is not a filesystem quota. CPU, memory, process-count,
filesystem, network and independent-VM controls remain unsupported. No same-UID
or hostile-code isolation is claimed.

Start is single-shot per exact admitted run, including ambiguous outcomes;
conflicting requests reject and a prior stop prevents a later start. Acceptance
and observed Running are distinct. Inspect returns opaque bounded observations.
Terminate accepts a request; only observation confirms exit/cleanup. Collection
requires confirmed cleanup, opens single-component approved names relative to a
retained directory descriptor with NOFOLLOW/NONBLOCK, rejects links and special
files, checks metadata, aggregate size and expected digest, then exposes scoped
in-memory artifact references. Release requires confirmed cleanup and required
collection, removes the owned allocation and retains an opaque replay tombstone.
Quarantine is sticky and retained on release/drop. There is no automatic
rehabilitation of an ambiguous allocation. General verifier findings, staged WI
artifact publication and portable provider qualification remain WIP-11 work.

Command, checkpoint, suspend and resume return Unavailable before request
validation, registry access, allocation, credential issue, event publication or
launch. There is no process-memory restore and no termination substitution.
Private process IDs, boot/birth facts, ownership nonce, paths and descriptors
have no public getters, serialization into consumer artifacts or ordinary Debug.
Host constructors accept paths; WorkerPort consumers never receive them. All
adapter I/O errors exposed to consumers are bounded opaque messages.

## Evidence and retained work

`conformance/wi-fabric-2/worker/process-adapter-coverage.json` records exact
manifest/package/target/test references: 14 integration tests and seven focused
unit tests. They cover functional exports, executable replacement/sealing,
startup drift, environment/non-CLOEXEC sentinels, malformed/fragmented bootstrap,
credential restrictions, deadlines without polling, simultaneous floods, normal
children/grandchildren, early leader exit, session changes, TERM resistance,
KILL escalation, drop cleanup and safe artifact reads/quarantine. Every process
fixture has bounded waits and failure cleanup. The recycled-PID test deliberately
substitutes a live unrelated child into mismatched boot/birth/nonce/run/allocation
or handle records and checks responsiveness after every refused action. It is a
deterministic PID-reuse simulation, not forced kernel PID recycling. A separate
isolated real-process test injects ownership mismatch and proves the target stays
alive while cleanup remains explicitly unconfirmed.

These tests resolve WP-A04 at the adapter mechanics boundary: advertised wall and
output limits are enforced for the approved fixtures; descendants are cleaned or
uncertainty is reported without signalling unverified targets. WP-020–023 and
WP-026 stay planned, owned by WIP-10, with no executable evidence added to
proof/cases.json. WIP-10 retains durable start intent, restart convergence,
authoritative WI events/AQ integration, AB-WI-021, evaluation registration and
WP-017–026 closure. WIP-11 retains portable artifact/seam qualification.

Linux with readable procfs, pidfd_open/send_signal, close_range(CLOEXEC), sealed
executable memfds and PR_SET_CHILD_SUBREAPER is required (normally Linux 5.11+
with these facilities permitted). Initialization probes required primitives and
fails closed; subreaper setup is checked in the helper before work. Host child
handling must not steal helper wait status; lost status produces uncertainty.
The mechanism is an approved-fixture implementation profile, not portable OS,
network, independent VM or deployment/supervisor qualification.

The 54 added inventory dispositions explicitly assign adapter sources to runtime mechanics
and test declarations to test evidence under this accepted design. These are
reviewable candidate dispositions, not an independent security sign-off.
Security review should inspect sealed-executable launch, child descriptor setup,
ownership acquisition/descendant discovery and every post-spawn cleanup path.
The imported assumption/source matrices remain immutable historical records.
Targeted tests use hard timeouts. All ten adopted checks must pass on the committed
candidate; exact commit receipts and logs live in the run's scratch/verification.

## Pending verification policy disposition

The unmodified WIP-08 `continuations` checker scans all runtime sources and bans
JoinHandle, sleeps and timed polling, including WIP-09's required independent
host-process supervisor and its reviewed fixtures. `contract-check` therefore
rejects this candidate with CONTINUATION-LOCAL-WAIT even though the process
mechanics tests pass. This is an unresolved scope conflict, not an adopted-check
pass. No checker or adopted definition has been weakened. The proposed operator
resolution is an exact-source-pinned host-supervisor exception, preserving every
AQ continuation and source-route check. A concrete unapplied patch and rationale
are retained in the run's scratch/review. Operator disposition and a fresh full
committed-head check run are required before the WIP-09 exit gate can pass.
