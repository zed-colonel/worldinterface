# WSP-01 — Reviewed, merged evaluation packet acceptance

Plan: WI-SUPERVISOR-PROOF-1, operator decision 2026-11-09. Baseline:
`928b5ef07797762bc002920af05a01aa4a4b0e0c`.

The complete evaluation manifest is reconstructed by the same implementation for
refresh, contract-check, campaign validation and trusted test-host setup. Live
candidate checks require exact committed manifest/source bytes at HEAD. A reviewed
merge with adopted checks establishes durable acceptance without a separate
operator receipt, trusted anchor or calendar expiry. Tests grant no integration,
publication, review authentication or gate closure. Historical receipts and anchors
are retained unchanged as provenance. This supersedes only the frozen imported
profile's explicit evaluation approval procedure.

Scope remains reviewed nonsensitive fixtures, the existing typed tasks and cases,
six operations, 15000 ms wall time and 65536 retained output bytes; no ordinary use
or production credentials. Trusted host catalog registration of reviewed fixtures
in a reviewed commit follows the same packet rule, retaining exact bindings,
revocation, quarantine and recovery. Request JSON supplies no registration authority.

Campaign schema 3 records the packet digest and acceptance policy and binds build
observations to that digest. Schema 2 evidence is historical. Non-live regressions
cover source/scope/manifest drift, unsafe paths, uncommitted refresh, pre-setup
rejection, canonical digest agreement, calendar independence and evidence identity
substitution. The adopted runtime campaign covers catalog denial and recovery.
Verification results and measured runtime-test duration are recorded in the run's
external `scratch/verification` and implementation handoff.

The explicit inventory disposition removes the active `OperatorApprovalReceipt`
type and anchor import, and registers the four new packet support module entries
in the existing fabric-support family as lifecycle test evidence. Historical
receipt and anchor files remain unchanged; no connector ownership changes.
