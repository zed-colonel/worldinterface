# WSP-02 — new-runtime 0.2.0-alpha.1 preparation

Maturity: release-preparation. Destination: `wi-supervisor-proof-1`.
Publication, remote protection, tagging/pushing and integration remain external.

The seven foundation crates and runtime have exact pre-release requirements,
publish metadata and explicit package contents. The legacy root manifests/lock
and crates are unchanged. `docs/release/package-tests.json` enumerates every
integration target and the adopted owner of repository conformance. Library
tests and doctests are always run explicitly on extracted archives; required
unit support/fixtures are package-local and checked against authoritative bytes.

`release.py` prepares on a clean full commit with Rust/Cargo 1.89.0. It executes
the ten unchanged adopted argv definitions, exact archive/local-registry
verification, denied-warning rustdoc, registry consumer, and version-pinned worker
installation. It preserves external build/cache settings and writes evidence
outside source. Each finalized archive precedes its dependent in the registry;
there are no seed WI packages, path/git/patch dependency substitutions or AQ
changes. Compiler-artifact records separately establish production crash-feature
exclusion. The release verifier rehashes the complete inventory and validates
source, packages, gates, locks, handoff, bundle and WI's existing preparation
receipt; it runs offline crate-release plan when installed.

The packaged descriptor and ten gates are authoritative byte copies.
`dependencies_digest()` embeds a checked package-local snapshot of the repository
runtime lock; external manifest/handoff record the archive's normalized lock
separately. Worker digests and sizes are measured by the documented install
recipe and scoped to the recorded build environment. No archive includes its
own checksum. No preparation receipt claims publication or reviewed closure.

The exact process-supervisor source pin/disposition is updated solely for the
lock include route; it preserves repository digest bytes and grants no new wait
exception or supervisor authority. Structural disposition additions cover the package-local shared fixture and
standalone consumer; existing source hashes and the complete evaluation packet
are refreshed together. New release scripts, provenance data, C helper, README
and consumer inputs are bound by the packet without a self-reference.

Verification results and release bundles belong in the run's external
`scratch/verification/`, with final committed-head check results recorded by
CraftingTable. Repeat preparation at the actual reviewed merge revision before
operator publication, as described in `docs/release/publication-staging.md`.

The first preparation retries exposed intermittent rejection of already-reaped
grandchildren during ownership discovery. A kernel-level diagnostic reproduced
`ESRCH` reading an open proc-stat descriptor after reaping; the existing path
handled only `ENOENT`. WSP-02 also handles that equivalent departure result and
adds a deterministic unit regression using the real proc descriptor. All other
read failures and live ownership mismatches still fail closed. This narrow
correctness fix is a deviation from packaging-only scope needed for dependable
clean-commit verification; it adds no operation, scope, waiting exception or
authority.

The release runner also pins and records `RUSTUP_TOOLCHAIN=1.89.0` for external
staging. Checking versions in the repository alone would allow rustup's different
machine default to build archives outside the checkout. A regression invokes both
compiler and Cargo from an external directory with the effective environment.

Refresh inventory hashes, generate the authoritative handoff, copy it into the
runtime package, refresh hashes again (the catalog binds that copy), and refresh
the evaluation packet last. Contract checks precede the live worker campaign in
release preparation, with all adopted commands preserved.

Release logs always carry command and working-directory headers, including for
silent successes: WI's existing receipt validator rejects empty artifacts.
Machine-readable metadata stdout remains separate from its diagnostic log.

Committed-head verification also exposed an incidental timing dependency in the
receipt-inspection fixture: its one-second effect-attempt budget can expire while
the concurrent campaign journals data, leaving AQ in RetryWait. A 1.2-second
fixture invocation deterministically reproduced that result. Receipt inspection
now uses a bounded 15000 ms attempt budget; the same delayed invocation must
complete and expose its canonical receipt. This is test support only; production
budgets, timeout conformance and adopted definitions retain their behavior.
The delay-family catalog disposition explicitly retains this independent
receipt regression, its shared test support and documentation; it grants no
legacy connector replacement or removal claim.

Review remediation preserves finding IDs F-001 through F-005. Worker recipes now
replace inherited Rust flags with remappings for each complete registry source
root, staging and target paths. A real Cargo regression installs one archive
from two registry locations and Cargo homes, checks an embedded dependency
`file!()` path, and compares executable digests. The worker environment record
captures the effective disposable Cargo home and encoded flags; verification
reconstructs the remappings from the recorded resolution (F-001, F-002).
Local-registry package, documentation, consumer and regression invocations use
staged Cargo homes; seeding reads the shared archive cache without unpacking
sources there (F-002). Rustdoc removes old pages before building its archive,
with a stale-page regression (F-003). The consumer resolves and records its own
fresh lock rather than claiming to seed runtime versions (F-004). Publication
staging directs the operator to relay exact handoff bytes and the trusted
manifest digest as downstream work-item inputs for crates.io-only agents
(F-005). These are implementation claims for review, not finding closure.
