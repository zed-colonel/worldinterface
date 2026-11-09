#!/usr/bin/env bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.89.0
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/wi-target}"
export TMPDIR="${TMPDIR:-/tmp}"
case "$CARGO_TARGET_DIR" in "$PWD"/*) echo 'build output must be outside worktree' >&2; exit 1;; esac
case "$TMPDIR" in "$PWD"/*) echo 'scratch must be outside worktree' >&2; exit 1;; esac
manifest=crates/worldinterface-runtime/Cargo.toml
run_cargo() { cargo "$@"; }
rustc --version --verbose
run_cargo fmt --manifest-path "$manifest" -- --check
run_cargo clippy --locked --manifest-path "$manifest" --all-targets -- -D warnings
run_cargo build --locked --manifest-path "$manifest"
run_cargo test --locked --manifest-path "$manifest"
metadata="$(mktemp "$TMPDIR/wi-integration-metadata.XXXXXX")"
trap 'rm -f "$metadata"' EXIT
run_cargo metadata --locked --manifest-path "$manifest" --format-version 1 > "$metadata"
python3 scripts/check-wi-integration.py "$metadata" crates/worldinterface-runtime/Cargo.lock
