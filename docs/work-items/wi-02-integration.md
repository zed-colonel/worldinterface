# WI-02 G1 integration

The production runtime is `crates/worldinterface-runtime`, an independent Cargo
workspace using AQ 0.2.0 at commit 29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860,
tree 1c0c85dd700bd9d0c75de223dada911ffeadbb5b, conformance revision 16.
`contracts/aq-integration-target.json` retains the archive/source-tree identities;
`aq-consumer-reconciliation.json` records WI-AQ-012/015 mappings. The historical
archive lock and imported plans remain historical evidence, not current receipts.

`QueueReferences` obtains both store identity and projection from one locked AQ
read session. It validates local task/run/attempt ancestry and encodes foreign
provenance only as `external_ref`. AQ constructs child inheritance. The version-1
`TaskInputLocator` is canonical JSON in task bytes, capped at 4096 bytes. WI record
digests remain separate from AQ CanonicalAdmissionV2. `ArtifactResolver` requires
an injected trusted authorization implementation and host-authenticated context;
artifact possession and attribution are not grants.

`StoreBundle` stages WI and AQ initialization, validates both, then publishes a
completion manifest and directory with atomic no-replace rename (no visible empty
reservation). Unsupported filesystem operations fail closed. SIGKILL tests cover
both sides of the rename for initialization, backup and restore, plus concurrent
destination creation and foundation backup publication. Offline backup holds an
exclusive bundle lock,
exclusive WI maintenance handle and retained shared AQ offline session across all
copies. WI handles cannot write while in maintenance mode; AQ writers are excluded
by AQ's own lock. AQ backup/restore retains its complete required WAL and eligible
snapshot. All advertised WI blobs and G1 AQ task-input/output references validate
before publication. G3/G4 must extend artifact closure for their signal/checkpoint
adapters before adding those APIs. Unknown external schemes remain owned by their
resolvers; this boundary does not fetch them.

SQLite DELETE/FULL recovery requires exclusive ownership. It validates root and
store manifests, recovers a private copy using SQLite, and validates every database
identity and artifact before opening the original databases for recovery. A failed
preflight leaves the source unchanged. Empty and interrupted zero-magic rollback
headers are valid non-hot states; SQLite classifies them in the private copy before
original cleanup. Linux syscall fault tests kill actual journal creation and
partial header writes (1, 4, 7, 8, 28 and 511 bytes), then prove reopen and idempotent
retry. Malformed magic, truncated hot headers and invalid recovered identities,
schema, history or artifacts still reject without changing the original.
Filesystem roots are trusted host-owned
storage; this is not protection against a hostile same-OS-identity process rewriting
files or bypassing advisory locks. Test-only subprocess barriers are compiled only
for unit tests, not exposed by the production library.

The source PR tests include canonical vectors, state properties, admission conflicts
and concurrency, atomic record/outbox/replay, WP-002 definitions, real AQ child
execution/replay, cross-store ancestry, authorization/integrity, fresh lineage,
and killed-process storage/publication tests. `scripts/check-wi-integration.sh`
checks the exact supplied AQ files, resolved versions/features and production graph.
Domain/checker checks run through `ct-check`; all three scoped jobs run through
`ct-act`. Following the recorded operator decision of 2026-09-20, use the updated
controller Cargo launcher for native foundation/checker manifests as well as
integration. It records AQ-free graphs as supplementary checks in
`current-upstream-build` mode, preserving foundation isolation without an
explicit-config bypass or artificial AQ dependency. Supplementary receipts do not
replace the required successful integration build/test against the current exact
AQ pins. Select Rust 1.89.0; CI continues to use the supplied explicit Cargo config.

The conformance package registers AB-WI-001/015/016 independently of unrelated
unimplemented W1–W59 skeleton cases. Source-record SHA-256 uses the controller's
recursively sorted compact JSON convention over the imported baseline row; test
source, fixture, environment, build and result hashes remain separate. Result
artifacts and reviews are external. No checked-in manifest claims a passing review,
merged revision, parent acceptance, worker-provider qualification or remote release.
