# Frozen experimental evidence

All files listed in manifest.json are inert version-1 evidence. Source copies retain their original bytes; Rust source fixtures are scenarios, not newly executed cases. Observations retain their collection paths and identities. No target reader or dependency is permitted.

The baseline build passed on Rust 1.86.0. Original tests: 755 passed, four code_git_diff failures with inherited diff.mnemonicPrefix=true. Rerun with process-local diff.mnemonicPrefix=false: 759 passed. Recovery and existing property tests passed; zero feature-gated WASM cases executed. The actual AQ dependencies were registry crates 0.1.0/0.1.1. See observations/recovery-summary.json, frozen Cargo.lock, and the external artifact index for exact commands and log digests. That historical report's missing owners/CI/coding questions were subsequently resolved by the accepted design; its runtime and publication limitations remain.

EXO selected easy and Aya results preserve successes and failures. Frozen source commit does not identify historical benchmark execution. The full collection manifest is an index of 90 externally retained files, not a claim that all 90 are checked in. No task-quality, improvement, or target conformance claim follows.

Immutable fixture versions: never overwrite a published fixture version. Add a new path/version and increment conformance package revision for additive coverage. Normative semantic changes require an explicit contract amendment. No migrations from this archive are supported.
