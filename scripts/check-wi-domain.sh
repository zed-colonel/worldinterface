#!/usr/bin/env bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.89.0
cd "$(dirname "$0")/.."
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/wi-target}"
export TMPDIR="${TMPDIR:-/tmp}"
case "$CARGO_TARGET_DIR" in "$PWD"/*) echo 'build output must be outside worktree' >&2; exit 1;; esac
case "$TMPDIR" in "$PWD"/*) echo 'scratch must be outside worktree' >&2; exit 1;; esac
manifest=crates/worldinterface-foundation/Cargo.toml
run_cargo() { cargo "$@"; }
export PROPTEST_DISABLE_FAILURE_PERSISTENCE=1
rustc --version --verbose
rustfmt --version
mapfile -t rust_sources < <(find crates/worldinterface-foundation -type f -name "*.rs" | sort)
test "${#rust_sources[@]}" -gt 0
rustfmt --check --edition 2021 "${rust_sources[@]}"
run_cargo clippy --locked --manifest-path "$manifest" --workspace --all-targets --all-features -- -D warnings
run_cargo build --locked --manifest-path "$manifest" --workspace --all-features
run_cargo test --locked --manifest-path "$manifest" --workspace --all-features
metadata="$(mktemp "$TMPDIR/wi-domain-metadata.XXXXXX")"
trap 'rm -f "$metadata"' EXIT
run_cargo metadata --locked --manifest-path "$manifest" --all-features --format-version 1 > "$metadata"
python3 scripts/check-wi-domain-isolation.py "$metadata"
