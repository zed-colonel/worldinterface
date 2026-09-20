# WI-FABRIC-2 conformance skeleton, revision 1

This package defines coverage slots, not passing runtime tests. All runtime cases and fresh-store scenarios remain unimplemented. WI-02 owns store behavior; later slices replace skeleton slots with executable cases and independently reviewed evidence. No AQ baseline case is assigned to WI-01.

The standalone checker validates package structure, source fidelity and evidence rejection. Its unit tests are checker tests, never current AQ/WI runtime conformance. Fixture versions are immutable; see the archive README. Additive coverage increments package revision; semantic change requires operator amendment.

Runtime evidence must identify the exact consumer commit, upstream archive/tree/build/conformance, fixture and source-record digests, toolchain/environment, command, test-run and results, reviewer, and phase. Candidate, merged, and post-rebase evidence are separate. Historical dependency identities and planned cases cannot establish a target pass. Use `wi-contract-check evidence check RECEIPT.json` to validate execution receipts. No fixture runner is supplied until target implementations exist.

WI-02/domain and WI-03/domain now supply independent local fixtures in `domain/`
and `ingress/`. They preserve the runtime skeleton and do not satisfy AQ cases.
