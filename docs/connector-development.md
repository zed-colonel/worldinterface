# Connector admission and disposition

WI-10/domain provides an offline checker and a reviewed disposition plan. Run
`wi-contract-check catalog check` or the scoped `scripts/check-wi-contract.sh`.
A passing plan still reports removal blocked. No catalog entry is admitted by
this slice and no existing runtime connector is removed. The checker is an
independent tooling workspace with no production WI or AQ dependency.

`inventory/connector-disposition.json` groups every current inventory ID, including
private helpers, all manifest copies and unresolved dynamic registrations. It
binds source hashes and registration sites. Each family lists removal/rebuild
surfaces: modules, exports, embedded schemas/tests, integration examples,
documentation and shared dependency manifests. Shared files require selective
edits. Cargo entries are a dependency audit list, not evidence they are unused.
Re-scan after replacement changes and remove a dependency only when no remaining
user needs it. Never delete archived fixtures or frozen imports.

Code search/read/edit/write/patch, fuzzy/common helpers, tests, Git diff, ignore
and repository context belong in a standalone specialist package/image outside
WorldInterface dependencies. Semantic Rust analysis belongs in that specialist
or an independently admitted verifier/analysis service. Filesystem and HTTP
connectors require canonical resource and authority contracts. Shell execution
requires qualified bounded `process.execute` or stays specialist-local; arbitrary
deployment shell is inadmissible. Sandbox execution moves to sandbox/worker
lifecycle. Signal/delay deletion remains owned by WI-08. Streaming/webhook sources
move to ingress. WASM transforms require deterministic, I/O-free behavior; other
WASM capabilities require independent admission even when the old name survives.

Before removal, bind exact replacement source revision and artifact digests,
applicable WI-06/WI-09 acceptance, independent qualification and successful
replacement fixtures. The domain command has no trusted runtime evidence intake;
it rejects all readiness claims. Its pure removal assessor compares supplied
assertions with an independently reviewed expectation for definition testing.
Integration must authenticate the evidence before any removal. Controller-owned
milestones and final forbidden-symbol activation remain unchanged.

The admission checklist in `contracts/wi10-catalog.json` preserves all six
justifications from implementation-plan §10.2 and all Appendix A questions.
At least one criterion needs a concrete justification; frequency and mediation
cost need explicit assessment. “An agent wants to call it” is insufficient.
Appendix answers may say not applicable only with a reason. Every descriptor
must specify effect class, canonical resource grammar and resolver, operation
schema, input/output artifact profiles, idempotency/provider key mapping, commit
evidence, reconciliation/retry boundaries, isolation, secrets, resource limits,
receipt evidence and privacy/retention. Offline resource limits are positive integer
milliseconds (time) and bytes (memory/output). The process definition requires
`bounded-process`, `process-managed-proof-1` and `no-production-secrets`; inconsistent
process declarations fail review. The review digest is part of the qualification
and exact evaluation-approval binding. Bind implementation version and descriptor
hash into authorization and receipts; changes require re-resolution/authorization.
This checklist validates declarations, not the truth or consistency of a proposed
runtime implementation. Independent descriptor/conformance review remains due.

Foundational lifecycle entries remain separately governed and pending. An
artifact, candidate, test report or evaluation success cannot authorize its own
activation. Runtime checkpoint/activation/rollback require separate lifecycle
authority and external acceptance. Optional filesystem enumeration stays deferred.

WP-007 uses synthetic definition tests only (`runtime_pass_asserted: false`).
Ordinary use requires applicable producer qualification binding profile, adapter,
executable, dependencies, configuration, environment, fixtures, results and
independent review. Drift or revocation invalidates the binding. Evaluation-only
eligibility requires explicit approval of that exact binding and reviewed fixtures
in a non-sensitive environment. It cannot waive authorization, enforcement,
resource bounds, exclusions or qualify itself. Quarantine blocks new starts;
inspection, termination and reconciliation remain separately authorized. Discovery
reports unavailable operations and grants no authority. Worker command/checkpoint/
suspend/resume remain unavailable for the initial process profile.

WI-ADR-016's approved early mapping uses the worker resource selector and
EffectInputRef for requested inputs, ResolvedEffect and typed immutable manifests
for selection, and bounded versioned EvidenceRef artifacts for controls. Keep
requested, advertised, applied and observed controls distinct. Process image
applicability is NotApplicable, never a fabricated image digest or Unknown.
Known/Unknown remain distinct representable values for future reviewed backends.
Portable provider/run/resource references are opaque; paths/PIDs stay private.
Enclosing-instance evidence is separate and establishes no child-worker isolation.
The tooling records are not a public worker wire protocol or metadata escape hatch.

`contracts/wi10-sandbox-disposition.json` specifies the future standalone package
and a replacement fixture proving search, edit, test and artifact export without
Fabric coding connectors. It defines five verifier contracts: changed-path scope,
artifact schema, exported test reports, secret scanning and patch metadata.
Verifiers consume explicit bounded artifacts and emit bounded findings tied to
input digests and verifier versions. Report validation never executes tests or
fetches external resources. Real verifier implementations and specialist image
qualification remain integration work; no package/image is built here.

Legacy sandbox.exec constrains shell arguments, directory, environment, timeout
and output; this does not establish OS isolation. Initial process fixtures allow
reviewed executables/dependencies and non-sensitive inputs only. Hostile code,
production credentials/data and newly generated unreviewed executables are
excluded. No independent VM, Kata, filesystem/network confinement, or unimplemented
CPU/memory/process-count control follows from a profile name, mediation mode or
outer instance. Stronger assurances require separately qualified profiles.

`contracts/wi10-obligations.json` traces each obligation to tests and retained
follow-on work, including WP-A01–WP-A08. Policy revision 1 uses controller-local
exact-source/target review and controller integration. Independent review,
controller merge and post-rebase checks on the actual merge target remain due.
Remote protection remains operator-owned before first publication and final
release approval. Local checks do not complete WI-10 or upstream conformance.
