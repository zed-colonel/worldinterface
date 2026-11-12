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
to 15,000 ms. Pending requests return promptly. Deadline expiry escalates; late
backend results cannot establish completion. The consumer must execute bounded
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

A separate security pass and its retained review evidence examine authentication
provenance, descriptor inheritance, exact replay/generation binding, cross-tenant
inspect, corruption/crash windows, server limits and duplicate activation. See
`WSP-03-security.md`; qualifying independent review remains CraftingTable-owned.
