# WIP-12: process catalog and provider quarantine

Implementation under the accepted WIP-12 design and WI-ADR-017. Ordinary process
use remains disabled. G4 remains open for WIP-16. Packet `34f2d904…` was accepted
through 2026-11-11T00:00:00Z and passed live verification and all ten adopted checks
on `d73d56a`. The review remediation below changes the packet and requires renewed
exact-packet acceptance and verification; earlier acceptance remains historical.

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

The runtime synthesizes the catalog binding from facts already checked by `derive`
and authenticates it through `TrustedApprovals::registered`. Dispatch rechecks that
host provenance and the approval, then fences the persisted launch/catalog identity.
The verifier API also compares a supplied record and current binding, but these
are the same verified binding in this composition; that equality is not an
independent runtime measurement. The per-field verifier test covers the API only;
WP-032's WI/AQ drift test covers the production derive checks. The process host
seals executable bytes at installation, and authenticates executable/helper build
provenance separately.

Provider quarantine keys on tenant/provider, records affected catalog bindings,
revision 1, reason, incident evidence and principal attribution, and has no clear
or promotion API. It shares the worker command journal with reservation and
recovery material. A SQLite immediate transaction serializes the final dispatch
fence with quarantine. Quarantine committed first rejects a stale capability;
dispatch ordered first may finish before quarantine commits. New run names,
replacement records, cleanup and projection rebuild cannot clear quarantine.
Approval expiry, revocation or absence is an admission denial, not an incident
decision. Poll and reconcile apply run-level quarantine and retain stop/recovery
authority without creating a permanent provider record or attributing an incident
to the run principal. A renewed valid approval may admit other runs on that
provider; it does not clear a run's quarantine or an explicit provider incident.
Ordinary Terminate retains current approval/registration checks and the exact
persisted launch/catalog fence while remaining available under provider
quarantine. It submits and durably acknowledges termination, so reconciliation
can confirm it. Release still requires admission and verified cleanup/collection;
quarantine does not authorize export publication or capacity release.

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

## Accepted evaluation and review remediation

The operator accepted packet
`34f2d904342b8c49d915f11f631878084e07978000cc2ec3abe528b0b781b957`, including
its unchanged supervisor disposition, in run
`c0deea63-825e-4724-bf55-c15ceafb2c40`. Commit `d73d56a` records that actual
receipt and trusted host anchor through **2026-11-11T00:00:00Z**. All ten adopted
checks passed on that commit (285 runtime, 189 foundation and 92 contract tests).
The independent review also passed catalog 10/10 and lifecycle 12/12, none ignored,
and recomputed all 287 packet file hashes. Review verdict was mergeable.

The earlier d06bd813 evaluation exposed missing Probe export declarations and a
WP-024 event scheduling assumption. Both were corrected before the accepted
34f2d904 evaluation; they are historical failures, not outstanding results.

This revision addresses F-001 with accurate historical status, F-002 with
run-only approval invalidation, F-003 with termination acknowledgement under
provider quarantine, and F-004 with explicit binding/provenance documentation.
New WP-032 regressions cover expired/revoked/absent approval without an existing
provider incident, renewed admission, and ordinary termination/acknowledgement/
reconciliation against the recording seam provider. The latter is seam simulation,
not external backend qualification. The existing drift test exercises derive.
The termination fence also rejects changed catalog or launch bindings.

The current candidate is identified by `evaluation-manifest.json`, not by the
historical accepted digest above. Source-bound build and non-live evidence go in
the supplied run's scratch/verification. Renewed acceptance, live verification,
committed-head adopted checks, qualifying review and CraftingTable integration
remain due for this revision. No record here grants acceptance or promotion.
