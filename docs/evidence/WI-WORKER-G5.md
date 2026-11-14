# WI-WORKER-G5: bounded external supervisor evidence

WSP-04 records authenticated interface behavior and durable reconciliation over
WP-035–WP-040. The gate record is effective on reviewed merge with adopted checks.
Passing a campaign does not grant closure, integration, publication, ordinary use,
consumer task acceptance, or provider qualification.

The public `worldinterface_worker::supervisor` protocol and `SupervisorHandler`
are implemented by an operator-owned supervisor. The runtime's `Server`,
`ChannelPolicy`, `PrivateChannel`, `Client` and `SupervisorConnector` exercise that
boundary without an engine client in WI core or lifecycle authority in WorkerPort.
The producer fixture uses another process and separately retained operator records,
with the same OS identity. It is not a separately implemented consumer supervisor.

The fixture synchronizes stable command identity, activation and terminal backend
results outside the guest and WI StoreBundle. It kills the guest, reopens WI and
the supervisor, and uses authenticated inspect to reconcile the original uncertain
intent without rewriting its receipt or activating again. WP-038 retains separate
records for receipt-history, receipt-commit and reconciliation-commit crash cuts.
Intentional aborts disable dumpability and core dumps at the abort point only.

Campaign observations use `wi-supervisor-observation-v1`, bounded to 65536 bytes.
Each is bound to its case/scenario, command key/digest, authorization scope,
backend result, authenticated inspect record, original WI intent and unchanged
receipt, reconciliation result and activation count. The campaign binds each file
to an exact passing test, executable/log hashes, source generation and evaluation
packet. Missing, substituted, contradictory and unowned evidence fails closed.
Failed scenarios retain their operator records for diagnosis in external evidence.

Exoskeleton owns actual Kata stop/recreate, storage durability, image compatibility,
deployment controls and isolation qualification. No Kata engine, real VM lifecycle,
strong isolation, production credential or hostile-code qualification is claimed.
Bounds remain 15000 ms per lifecycle command, 65536 bytes per protocol frame and
4096 bytes of backend evidence (or the lower exact request bound).

G5 derives its criterion and six cases from the immutable worker execution profile.
Prerequisites are WI-WORKER-G1/G2, WI-AQ-G1–G4, and WSP-03's supervisor boundary.
The retired ten-gate export is unchanged. The historical worker milestone remains
55 cases and ten gates; the complete campaign contains 61 cases and separate G5.

The generated consumer descriptor has a separate `supervisor` section. Reproduce
with the ten adopted checks on the committed candidate and `handoff verify` against
that same commit's fresh external campaign directory. See
[WSP-04](../work-items/WSP-04.md) for changes, measurements and verification.
