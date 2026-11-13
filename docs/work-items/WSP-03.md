# WSP-03 — Separate bounded host-supervisor adapter

Candidate implementation for WI-SUPERVISOR-PROOF-1, based on WSP-02 at
55c802026aa7387cc73026825be5fb3a9bbbdbe8. Maturity: integration-only.
Reviewed merge and adopted checks establish integration; tests grant no closure.

The public `worldinterface_worker::supervisor` protocol and consumer handler are
AQ-free and separate from the six worker operations. The public runtime module
supplies a private Unix channel, authenticated server, independently rooted
journal and lifecycle connector. No engine client, guest activation credential,
production credential or additional worker authority is introduced.

The simplest accepted transport is an anonymous Unix socket pair provisioned by
the operator. Channel possession authenticates a server-owned session. An exact
operator grant binds tenant/principal, supervisor audience, authority generation,
policy revision, instance/generation, operation, target/image digests, deadline
and evidence bound. No identity or engine option is accepted from the wire.
Host composition is trusted; guests must not share the supervisor's OS identity
or receive its descriptors/storage if they are hostile. The fixture uses separate
processes under the test identity and proves noninheritance and independence,
not filesystem/VM isolation. Kata qualification belongs to Exoskeleton.

Commands persist with file and directory fsync before acceptance or dispatch.
An exclusive journal owner lock, an atomic in-process claim, stable provider keys,
exact replay comparison and instance-generation claims fence concurrent work.
Existing records only inspect after restart. Backend authoritative inspection
must exclude late activation before reporting noncommit; unsupported recovery
is rejected. Corruption fails closed. Interrupted atomic writes are discarded;
I/O failure disables the journal until reopen. Terminal results are immutable.
Operator journal rollback while a backend may still run is unsupported.

The server admits one outstanding trusted backend call and no unbounded queue;
retains at most 256 commands, 32 grants/channel, 65,536-byte frames and 4,096-byte
backend evidence; bounds complete frame I/O to one second and command deadlines
to 15,000 ms. Pending requests return promptly. Deadline expiry escalates unfinished observations and late dispatch results;
authoritative inspect can recover retained completion after the deadline. The consumer must execute bounded
backend commands outside the target; trusted Rust handlers are not preemptible
sandboxes. A stuck handler keeps its single slot and owner lock until exit.

Lifecycle acceptance becomes WI `Uncertain`, never successful completion.
Authenticated inspect supplies typed durable outcome evidence through existing
AQ reconciliation, preserving the original receipt and `Phase::Reconciled`.
Callbacks supply no authority and are unnecessary. Guest shutdown, lost replies,
ambiguous backend dispatch, WI restart and supervisor restart retain the same key
without duplicate activation or final reconciliation.

WP-035–039 are registered in `worker/supervisor-coverage.json` and the 60-case
campaign registry and the committed evaluation packet. The six worker operations
and existing tasks remain unchanged; supervisor operations use separate operator
grants. WP-A08 is resolved there by the external result journal and
authenticated inspect. Imported planning matrices/assumptions remain unchanged.
The historical worker milestone and packaged handoff retain exactly their 55
cases and ten gates. WP-040, G5 closure and supervisor handoff remain WSP-04.
All eight exact crate versions stay alpha.1 until WSP-05; nothing is published.
Public API consumer tests are included in worker and runtime archives.

Verification evidence for this run is retained outside the worktree at
`/mnt/workhorse/craftingtable/runs/9cc604a0-fc83-4398-b802-adec555ce7a0/scratch/verification/`.
`checks.json` records complete adopted command durations and results on the
committed head; `runtime-test.log` includes the full campaign timings, including
build and discovery. Campaign concurrency is 12 bounded harnesses (formerly 8);
coverage and the adopted 30-minute ceiling are unchanged. The acceptance target
is at most 1,500 seconds for the full runtime check. The final implementation
report supplies the measured value and exact verification head.

F-001 remediation: the guest now echoes a fixed ASCII readiness message through
standard streams using the already bound `/bin/cat` fixture, before the host reads
its procfs environment and descriptors. Socket reads and writes have one-second
timeouts. This avoids both the inherited-parent snapshot and the transient empty
snapshot during exec; the empty-environment and stdio-only assertions remain.
The parent owns and reaps the guest, including on panic, and the existing bounded
WI invocation precedes guest shutdown. Remediation stress results, negative
controls and adopted-check timings are retained at
`/mnt/workhorse/craftingtable/runs/0581b1d4-30b9-40fd-840d-4c0d2df9608d/scratch/verification/`.
Parallel repeats also exposed an activation-publication race: both the lost-ack
helper and the harness now wait for the completed activation record, rather than
the earlier creation of the backend command file, before supervisor teardown.

F-002 remediation separates the one-second idle poll from the one-second frame
budget, which starts after the first byte is received. Already buffered bytes
are read before checking the remaining budget; partial progress cannot renew it.
Transport errors close both endpoints. Consumers must provision a fresh
authenticated channel and inspect the same durable key, preserving uncertainty
about admission after a lost response. Public consumer regressions cover a valid
30 ms Submit beginning 990 ms into an idle poll, slow partial-header progress,
and closure of the unusable channel. A unit regression covers buffered data when
the reader resumes after its deadline.

F-003 remediation restores the controlled fixture clock from durable WI history
on restart. It advances directly to a Scheduled or RetryWait timer, once per
actual due time, rather than adding a second on every idle tick. Each distinct
AQ polling wait is paced for one real second before advancing to its recorded
deadline, allowing the supervisor's asynchronous inspect to finish without
burning the virtual reconciliation budget. The engines use an explicit
one-second recovery backoff. Submission and reconciliation deadlines, the
ten-second settling bounds and the twelve-second WI child bound remain intact.
The lifecycle submission explicitly uses the existing 15,000 ms supervisor
attempt budget. The generic egress example's inherited 1,000 ms AQ watchdog
could otherwise cancel during WI preparation before writing Submit, explaining
the lost-ack helper's EOF. Frame I/O remains bounded to one second and the child
cutoff still applies; no EOF or convergence assertion is ignored.
 WP-038 deliberately delays backend inspect by 250 ms, retains all
three crash cuts, and still requires confirmed commit, unchanged original
receipts, idempotent reconciliation and exactly one activation. No EOF is ignored.
Remediation stress, negative-control and adopted-check evidence is retained at
`/mnt/workhorse/craftingtable/runs/6633cf7b-ebd1-48e7-930c-c2df381fb74a/scratch/verification/`.

A separate security pass and its retained review evidence examine authentication
provenance, descriptor inheritance, exact replay/generation binding, cross-tenant
inspect, corruption/crash windows, server limits and duplicate activation. See
`WSP-03-security.md`; qualifying independent review remains CraftingTable-owned.

F-004 remediation retains all 66 sequential provision/release cycles, duplicate
admission and release checks, the final real process launch, durable inspection
evidence and the one-host/one-slot assertions. The isolated predecessor took
512.83 seconds. Fixture operations now construct their demand once and pass its
submission, authorization snapshot and configuration to execution together,
avoiding a second packet/build check and repeated durable artifact publication.
Admission and dispatch still independently validate the complete committed
packet. No validation result is cached. The test logs preparation and execution
time for every operation. Pending executor polls are paced at 20 ms instead of
2 ms, avoiding repeated reads of growing durable state while blocking handlers
validate and journal. The thirty-second operation deadline is unchanged.

Scheduling alone did not remove the growing cost. The effect reducer now reuses
its freshly computed digest within validation of the same immutable state;
replay still computes and verifies its own digest. The storage writer persists
the complete tenant state, accounting and journal as before, but updates dispatch
outboxes and run bindings only for the effect ID returned by the reducer. Recovery
uses the same incremental projection path. A regression rejects even identical
writes to an unrelated retained outbox and requires identical replayed state and
outboxes. Existing stale-command, rollback and receipt-crash checks remain.

The campaign first runs the existing non-live reviewed-build test exactly once,
then starts capacity at the head of the shared twelve-worker pool. Timing reports
these two execution phases. Serializing the whole capacity test used too much of
the full-check budget; all behavioral tests therefore share the bounded pool.
Compiler preparation remains inside the complete adopted command, and every test
retains its 600-second timeout. A marker/barrier regression requires build-before-
capacity order, no peers during preparation, all twelve subsequent slots, and
exactly one result per test with binary/log digests retained.

The test host's usual live build accessor also avoids a second non-live packet
reconstruction immediately after its fresh committed reconstruction of the exact
same source root. It still rechecks every committed source byte before accessing
the immutable compiled-artifact cache and compares the complete packet digest;
the external-root diagnostic accessor retains its independent root checks.

Complete coverage and the adopted 30-minute ceiling remain unchanged.
The previous run stopped before measuring the final optimized head; its earlier
experiments do not establish F-004 closure. Follow-up verification is retained in
`/mnt/workhorse/craftingtable/runs/23025397-d028-4de5-b5e7-4180ec153db1/scratch/verification/`.
The final report records two adopted runtime-test durations and the individual
capacity durations separately.

The follow-up also removes the second effect preflight snapshot/reduction.
Non-receipt commands now validate artifacts against the reduced state under the
same writer lock that protects journal, accounting and projection publication.
The transaction still checks the command's exact base digest, replays the domain
command, rejects outcome commands that need atomic receipts, and verifies every
required artifact before writes. Artifact rejection and injected failures leave
all projections unchanged; stale commands retain the existing contention error.

Intentional supervisor crashes disable dumpability with `PR_SET_DUMPABLE` and set
both `RLIMIT_CORE` limits to zero immediately at the abort cut. The helper asserts
both settings and writes evidence before aborting. WP-038 requires that evidence,
a real SIGABRT death, and no core-dump flag on every crash-cut iteration. Earlier
guest procfs isolation checks run with normal dumpability. Supplemental repeats
are limited to four simultaneous harnesses on the shared workstation.
