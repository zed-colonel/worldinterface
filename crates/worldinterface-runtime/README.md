# worldinterface-runtime

Runtime layer for WorldInterface durable embedded workflows. Version 0.2.0-alpha.1; Rust 1.89.0, Apache-2.0.

This pre-release carries the WI embedded worker proof. See the repository
`docs/release/0.2.0-alpha.1.md` for scope, limitations and consumer instructions.

Consumer evidence is shipped under `release/handoff.json` and `release/gates/`.
The repository lock snapshot is `release/repository-Cargo.lock`; the archive
`Cargo.lock` is independently normalized by Cargo.

The public Unix `supervisor` module provides `Server`, `PrivateChannel`, `Client`
and `SupervisorConnector`. A consumer implements
`worldinterface_worker::supervisor::SupervisorHandler`; WI contains no engine
client. Provision each anonymous Unix channel from trusted host composition with
an exact `ChannelPolicy`. Never transfer it to the guest or worker. Requests do
not carry authentication credentials or choose their own principal/tenant.
Revoke the server-owned policy to fence both submit and inspect. A fresh host
connection is required after WI restarts.
`PrivateChannel::serve_one` may return successfully after an idle one-second poll
without consuming bytes. Its separate frame deadline starts with the first byte;
buffered bytes are drained before requiring more time. Any transport error closes
the endpoint. Provision a fresh authenticated channel and inspect the same durable
key to recover; a lost reply alone does not establish whether Submit was admitted.

Open the supervisor journal in a private operator-owned directory outside both
`StoreBundle` and the guest's filesystem. It atomically syncs complete command
bindings before acceptance/dispatch and immutable terminal observations before
exposing them. An exclusive owner lock fences supervisor instances. Existing
records only inspect after restart; they never dispatch again. The consumer
must guarantee authoritative inspect across an ambiguous dispatch, including
that rejection excludes later activation. Unsupported backends fail admission.
Restore/rollback of the operator journal while its backend may remain active is
not supported: preserve it and reconcile with the backend; do not erase it to
retry an activation.

Bounds: 32 exact grants/channel, 256 retained journal records, 65,536-byte frames,
4,096-byte backend evidence, one outstanding backend call, one-second total
frame reads/writes, and at most 15,000 ms to the command deadline. The server
escalates unfinished observations at the command deadline and refuses late
dispatch completion. Authoritative inspect can still recover retained backend
completion after that deadline, without dispatching again.
Consumers implement bounded backend commands outside the guest, returning
`Running` promptly while transitions continue. The Rust handler is trusted host
code: it is not a sandbox; a stuck handler retains its owner lock and slot until
it returns or its supervisor process is stopped. Operator process isolation and
engine-specific cancellation remain consumer responsibilities.

`SupervisorConnector` pins the exact host session, target document, request,
provider key and descriptor. Invocation returns `Uncertain`; authenticated
inspect drives AQ reconciliation. A completed transition confirms commit, a
never-dispatched rejection confirms noncommit, and partial/irrecoverable work
escalates. Reconciliation preserves the original receipt and does not establish
application acceptance. Callbacks are unnecessary.

WSP-03 packages the interface without publication or G5 closure. Its separate
process fixture models guest loss; it does not qualify a container/VM isolation
boundary. WSP-04 owns G5/WP-040 and WSP-05 prepares alpha.2 distribution.
