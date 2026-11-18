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

The final scheduling pass changes only execution order and phase concurrency:
counted build preparation runs alone, the two history-heavy regressions overlap
on two workers without other peers, and the remaining complete suite uses the
existing twelve-worker bound. Every exact executable/result binding, outcome
partition, full coverage requirement and 600-second test deadline remains. The
barrier regression proves isolation, two-way and twelve-way overlap, exactly-once
execution and binary/log digest retention; a serial negative control fails.

Separate F-005/F-006 implementation security pass (2026-11-13): reviewed the
bounded effect read set against every JournalAction. Only Submit reads a second
intent (its linked original) or request identity; all other actions read/mutate
the named intent. The writer reconstructs the read set from canonical typed
command bytes under the Fabric writer transaction. It compares the current
revision, previous-entry hash, count, timestamp, shared uses and selected facts
before reducing, then verifies artifacts and publishes the journal, metadata,
changed aggregate, request/outbox/run projections and events atomically. The
chain head covers skipped committed history; it is never supplied independently
by an executable caller. Legacy entries still verify the complete state; v3
replay independently derives and checks its bounded digest. Existing schema and
complete replay comparisons cover corruption of derived projections on open.

Per-tenant grant uses are retained together, preserving last-use arbitration;
the retained count preserves capacity despite sparse views. Request indexes are
tenant scoped and duplicate submissions still compare complete submission,
link and owner. Owner lookups are tenant/flow/node indexed and their callers
retain principal and child bindings. Receipt/outcome atomicity and all artifact
checks are unchanged. Regression evidence includes constant read/commit counts,
stale command rejection, mixed legacy/v3 replay equivalence, rollback and receipt
crash recovery. Supervisor authentication, grants, bounds, channel handling and
guest authority paths have no changes. No open blocking or major finding was
identified in this implementation pass; qualifying independent review remains
CraftingTable-owned.

The receipt-source reader now recognizes v3 journal entries alongside v1/v2;
unknown versions still fail even with canonical bytes and a matching digest.
Indexed intent/tenant selection, exact receipt revision, admission/Start/run
bindings and outcome checks remain unchanged. Mixed-version and unknown-version
regressions cover the allowlist extension.

Catalog decoding rejects nonempty aggregate/request maps. Corruption fixtures now
target the normalized rows, preserving their original negative controls; a focused
count regression also rejects an aggregate injected into the catalog. The genuine
1,000-entry completion fixture persists the normalized projection and keeps all
original completion, redelivery, writer-delay and single-invocation assertions.
