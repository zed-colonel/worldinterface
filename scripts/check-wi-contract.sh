#!/usr/bin/env bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.89.0
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/wi-target}"
case "$CARGO_TARGET_DIR" in "$PWD"/*) echo 'build output must be outside the worktree' >&2; exit 1;; esac
# Includes WI-10 inventory, admission, removal-readiness and synthetic WP-007 checks.
# No production AQ/WI runtime or sandbox qualification is asserted.
manifest=tools/wi-contract-check/Cargo.toml
run_cargo() { cargo "$@"; }
rustc --version --verbose
rustfmt --version
rustfmt --check --edition 2021 tools/wi-contract-check/src/*.rs tools/wi-contract-check/tests/*.rs
run_cargo clippy --locked --manifest-path "$manifest" --all-targets -- -D warnings
run_cargo build --locked --manifest-path "$manifest"
run_cargo test --locked --manifest-path "$manifest"
run_cargo run --locked --manifest-path "$manifest" -- all check
