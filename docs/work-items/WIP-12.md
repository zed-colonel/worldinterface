# WIP-12: process catalog and provider quarantine

Candidate implementation under the accepted WIP-12 design and WI-ADR-017.
Ordinary process use remains disabled. G4 remains open for WIP-16. Source review,
renewed exact-packet evaluation acceptance, positive live verification and adopted checks
on the committed candidate remain due; this note grants none of them.

The foundation capability module declares bounded version-1 catalog bindings,
evaluation/ordinary record variants and inspection results. Runtime verification
requires independent host registration provenance as well as exact current
measurements and the WIP-10 operator receipt. Qualification verifies structure,
validity, revocation, producer/results/reviewer/operator provenance and exact
applicability, then refuses ordinary process use. No record or test promotes a
provider. Inspection reports exactly six operations and the two mandatory finite
controls. Unsupported operations cannot produce a connector.

Every complete governing policy is independently registered for each exact
demand. Its authority sets contain operation-specific demand hashes. Therefore
the shared run identity uses a projection of the stable governing rules, while
the complete policy is still checked at each admission and dispatch. Profile,
descriptor, adapter, fixture/helper/dependencies, configuration, controls, AQ/WI
identities, environment, input and export bindings remain exact. This is the
concrete accommodation required by the existing exact-demand policy model.
Trusted host registration must authenticate current measurements and policy;
parsing a declaration or copying current values into a record is insufficient.

Provider quarantine keys on tenant/provider, records affected catalog bindings,
revision 1, reason, incident evidence and principal attribution, and has no clear
or promotion API. It shares the worker command journal with reservation and
recovery material. A SQLite immediate transaction serializes the final dispatch
fence with quarantine. Quarantine committed first rejects a stale capability;
dispatch ordered first may finish before quarantine commits. New run names,
replacement records, cleanup and projection rebuild cannot clear quarantine.
The reservation atomically persists the original operation realizations and
connector bindings before any provider admission or process launch.

The fresh-store feature profile advances from `local-worker-12` to
`local-worker-13`, schema 1. Old stores are rejected and require a fresh lineage;
there is no silent migration. Replay and projection validation include provider
quarantine and original launch material.

Recovery parses historical profile values structurally, then checks the exact persisted operation rather than re-admitting its old profile under current definitions. New work retains strict current-profile validation.

Recovery uses current host policy, authenticated caller, tenant, exact durable
WorkerBinding, operation and original/current implementation identities. It
permits inspect, terminate and reconciliation only. The domain and evidence
boundary retain ordinary agency, grants, policy and resource checks, while the
recovery branch does not require expired fixture credentials or admission
approval. Recovery-only construction needs no executable or helper file, installs no executable registration and has no admission verifier. Reopen reads the original private cleanup journal without
adopting or signalling a numeric PID. A stop acknowledgement never proves cleanup
or releases capacity; missing private evidence remains uncertain.

The generic connector identity rule is unchanged for invocation. AQ
reconciliation alone can use a trusted recovery hook that verifies the original
persisted connector binding and the current recovery implementation under current
host policy. Committed collection results and original receipts remain immutable.
Invalidation cannot publish a prepared or quarantined collection. A Fabric writer fence also orders ContextStore export publication against provider quarantine; final collection transitions recheck quarantine. The supervisor
candidate disposition changes only the existing mod.rs pin: host-authorized
observations remain possible after fixture credential expiry. The five-file
exception and generic continuation ban retain their scope.

WP-007 retains its definition tests and ownership, with runtime evidence appended.
WP-032 maps to non-ignored tests in `tests/worker_catalog.rs`. The authoritative
catalog contract and coverage file enforce exact operation, binding, qualification,
quarantine, recovery and live WI/AQ obligations. Runtime-pass claims and imported
source hashes are preserved. Structural inventory changes are explicitly proposed
in the inventory dispositions for this candidate; qualifying review remains due.

Non-live tests exercise all binding fields, fabricated/expired/revoked records,
ordinary-use exclusion, persistent quarantine, both dispatch orderings and exact
recovery scope. Live tests exercise the approved packet through WI/AQ, deny changed
facts before allocation, and retain recovery after revocation/quarantine and a
recovery-only reopen. These live tests require renewed exact-packet acceptance.
The packet builder covers WP-007, WP-032 and the new sources/contracts. Separate
approval provisioning stays outside its own digest. Verification logs and the
source-bound build record belong in the supplied run scratch/verification.

Pre-approval verification in this implement run: the complete foundation suite,
foundation isolation and Clippy passed; runtime Clippy passed; seven catalog tests
and ten evaluation/artifact validation/crash tests passed without launching a
worker fixture or helper. The former local-worker-12 profile also rejects in an
isolated test. Earlier failures during development (optional journal null encoding,
private test-directory permissions and stale inventory hashes) were corrected.
The source-bound build record and subsequent adopted-check results are retained
outside the packet under the supplied scratch/verification directory. Positive
live results for the revised packet are not asserted here.

## Accepted evaluation and test-composition remediation

The operator accepted packet
`d06bd8139cc46bcd792873e7ad829d5226222e5ffb3966d65b5c98357851d7a9`, including
its supervisor disposition, in run `29e02a4e-673a-400c-80f1-3e1f27d9a219`.
Commit `e5fc3b6` records that actual decision in the separate receipt and trusted
test-host anchor. Its finite expiry is 2026-11-11T00:00:00Z. Scope remains the
reviewed fixtures and six operations, nonsensitive inputs, 15000 ms wall time
and 65536 retained output bytes. Ordinary use remains disabled.

The accepted direct catalog evaluation passed eight tests and failed the two
WP-007/WP-032 collection success assertions; WP-007 also failed alone. Both
compositions used Probe without declaring its `probe.json` export, so the strict
collector correctly rejected that unlisted file. The remediation declares the
exact 14-byte JSON export in those two compositions and checks the published
content. It changes no collector or admission policy. Shared operation assertions
now identify the failed operation.

The accepted six-target worker regression run passed 33 tests, including live
collection crash/reconciliation, lifecycle crash, process cleanup and seam tests.
WP-024's pending-signal assertion failed in that run and passed alone. Start's AQ
ticks can already have delivered every observation before finish returns. The
remediation commits a distinct late exit through the existing observation boundary
after process completion, verifies its pending obligation before ticking AQ, then
retains authoritative event/state, delivery and duplicate-drain assertions. This
removes the scheduling assumption without weakening the ordering assertion.

These changed test and status bytes require a new packet acceptance. The receipt
and host anchor retain the actual accepted d06bd813 identity; no test or agent
retargets that decision. The supervisor disposition is unchanged. Source-bound
build, non-live evidence and committed-head check results are retained in the
current run's scratch/verification. Renewed acceptance, revised live verification,
qualifying review and CraftingTable integration remain due.
