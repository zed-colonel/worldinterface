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
