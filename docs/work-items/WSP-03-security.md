# WSP-03 security pass

Separate implementation security pass after the adapter and negative regressions
were written. Scope: public protocol, runtime server/transport/journal/connector,
process fixture, proof scope and packaged consumers. This is an implementation
review; CraftingTable's qualifying review remains separate.

Authentication comes exclusively from an operator-provisioned anonymous Unix
endpoint and its server-owned session. Wire types contain no principal, tenant,
engine options or deployment credential. Strict decoding rejects unknown fields,
duplicate keys and oversized input. Submit and inspect both check revocation and
exact operator grants. Worker credentials have no supervisor authentication route.
The fixture clears guest environment and verifies that only stdio is inherited,
after a bounded stdio echo proves guest code has executed. The fixed readiness
message conveys no supervisor authority or credential (F-001 remediation).

Complete bindings, stable keys and generation claims precede dispatch. The journal
owner lock remains held by an outstanding handler across Server drop, fencing
another owner. A crashed supervisor can only inspect retained commands. No crash
window blindly reactivates a backend. Authoritative backend noncommit must exclude
late activation. A consumer reporting unsupported recovery cannot open a server.
Terminal records cannot change; corrupt files/checksums fail reopen. Atomic result
publication uses file/directory fsync and fail-stop on persistence error.

Bounds are enforced in host code: frame size and total I/O time, grant count,
retained command count, evidence size, one outstanding backend call, exact command
deadline and immutable escalation for unfinished observations after expiry.
Authoritative read-only inspect remains available after the execution deadline
to recover retained backend completion; it never reactivates the target. Pending calls return promptly to
WI. Arbitrary trusted handler code cannot be safely killed by Rust; consumers
must enforce engine execution/cancellation bounds outside the guest and isolate
their supervisor process. A stuck handler retains the slot/owner, denying further
activation rather than allowing concurrent dispatch.

The journal root must be provisioned outside WI/guest storage with distinct
operator OS access for hostile guests. The server rejects unsafe ownership,
permissions, symlinks and hard-linked journal files. Checksums detect corruption,
not a malicious operator rewriting its own journal. Rolling back the operator
journal while an engine may remain active is unsupported. These are explicit
composition requirements, not VM isolation claims.

Findings addressed during this pass: generation reuse under another key is
rejected; deadline escalation fences late completion; partial transport failures
close the client channel; journal I/O failure requires recovery. Negative
regressions cover these paths along with stale replies, cross-tenant inspect,
revocation, corrupt journal, concurrent duplicate submission and unsupported
backend admission. Process regressions exercise guest loss, lost acknowledgment,
ambiguous dispatch and fresh host reconnection after WI/supervisor restart.

F-002/F-003 remediation pass: idle polling has a separate finite bound from the
frame read. A per-call nonblocking receive drains already buffered bytes without
changing shared socket flags, then any blocking read uses the remainder of the
original frame budget. Progress never resets that budget; the frame-size cap
bounds buffered work. Server I/O failures now shut down the channel, preventing
partial-frame suffixes from being reinterpreted as new commands. Fresh channels
retain the same exact operator grants and recover through authenticated inspect;
neither a timeout nor reconnection grants dispatch authority. No authentication,
journal, worker operation or guest descriptor path changes. Process fixture time
is restored from durable WI history and advances only to recorded AQ timers.
Distinct polling waits are paced in real time so asynchronous inspect cannot
exhaust a virtual deadline through idle ticks. WP-038 includes delayed inspection
without relaxing convergence, receipt, activation, isolation or deadline assertions.
The fixture's AQ attempt budget explicitly matches the existing 15,000 ms
supervisor ceiling, rather than inheriting a one-second generic egress watchdog
that can cancel before Submit. Frame and child-process cutoffs remain unchanged.

No open blocking or major finding identified within this bounded trusted-host
contract. Real backend, OS isolation and Kata qualification remain downstream
consumer work, as specified by the accepted WSP-03 scope. Retained pass results
and source revision are in this run's scratch/review and scratch/verification.

F-004 pass: supervisor authentication, journal and worker authority paths are
unchanged. The effect validator reuses a digest computed immediately from the
same immutable state inside a private helper. Public replay computes the digest
anew; storage still verifies the typed command's base digest and all canonical
journal bytes. No digest is accepted from an untrusted caller or cached across
commands. Every reducer action mutates only the effect ID it returns, plus tenant
revision/use accounting. Storage persists the full tenant state and journal but
updates only that effect's derived dispatch/index rows, in the same transaction.
Run-binding collision checks remain on every affected binding. Recovery applies
every command and reconstructs identical projections; full reopen comparison and
receipt/fault atomicity checks remain. A regression forbids rewriting an unrelated
outbox and checks fresh replay equivalence.

The live compiler-artifact accessor performs a fresh complete committed packet
check before consulting the artifact cache, then checks its packet digest;
removing the immediately repeated non-live reconstruction caches no validation
result. The external-root diagnostic path still validates its own root and the
compiler root. The existing non-live build test is counted once before the
bounded shared pool. Exact test selection, before/after executable measurements,
complete outcomes and all deadlines remain. Compiler preparation is included in
complete adopted-check timing, and every actual worker admission and dispatch
still independently verifies the packet.

Follow-up F-004 and crash-helper pass (2026-11-12): artifact verification now runs
inside the existing Fabric writer transaction against exactly the reduced state
being published. It performs local artifact reads as the receipt path already
does; it cannot publish a command that failed artifact verification. Stale-command
fencing, receipt-only outcome publication, accounting and transaction rollback
remain enforced. The focused storage regressions exercise rejection, stale
commands, injected write failures and identical replayed projections. No new
supervisor or worker authority is introduced.

The deliberate supervisor abort disables dumpability and both core size limits
only at its crash cut, preserving earlier procfs credential/descriptor probes.
WP-038 checks in-child setting assertions, evidence, SIGABRT and the parent's
no-core wait status. No other deliberate process abort was found in the repository.
This follow-up implementation security pass has no open blocking or major
finding; independent qualifying review remains CraftingTable-owned.

The follow-up typed-command reduction computes the complete current-state digest
inside `apply_to`, compares it with the private validated command's base, and then
reuses that digest only for reduction of the same immutable state. It decodes and
checks canonical tenant-bound command bytes and re-runs all domain checks. It
returns an optimistic conflict for any base drift, including a divergent state
with the same revision. Storage preserves the typed contention error. Independent
byte-journal replay computes its own digest and shares only the strict decoder.
No digest or cached reduction can be supplied by a caller. The new domain
regression checks equivalent reduction and both forms of base drift.

The lost-ack helper retains its main thread for up to three seconds after
activation when interrupted dispatch is requested. This closes the normal-exit
race with the aborting dispatch thread; lack of SIGABRT fails the existing parent
assertion. No additional wait is taken on non-crash paths. WP-038 injects a 250 ms delay
before the abort settings to expose the race deterministically; dumpability and
core limits are still changed immediately before aborting. All earlier procfs probes precede the abort settings.
