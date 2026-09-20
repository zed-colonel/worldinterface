#!/usr/bin/env bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.89.0
cd "$(dirname "$0")/.."
: "${CRAFTINGTABLE_CARGO_CONFIG:?controller Cargo config is required}"
: "${CRAFTINGTABLE_DEPENDENCY_MANIFEST:?controller dependency manifest is required}"
: "${CARGO_TARGET_DIR:?external target directory is required}"
: "${TMPDIR:?external scratch directory is required}"
case "$CARGO_TARGET_DIR" in "$PWD"/*) echo 'build output must be outside worktree' >&2; exit 1;; esac
case "$TMPDIR" in "$PWD"/*) echo 'scratch must be outside worktree' >&2; exit 1;; esac
manifest=crates/worldinterface-foundation/Cargo.toml
controller_cargo="$(dirname "$CRAFTINGTABLE_DEPENDENCY_MANIFEST")/bin/cargo"
run_cargo() {
  if [[ "$(command -v cargo)" == "$controller_cargo" ]]; then
    cargo "$@"
  else
    if [[ "$1" == clippy ]]; then
      shift
      cargo --config "$CRAFTINGTABLE_CARGO_CONFIG" clippy --config "$CRAFTINGTABLE_CARGO_CONFIG" "$@"
    else
      cargo --config "$CRAFTINGTABLE_CARGO_CONFIG" "$@"
    fi
  fi
}
export PROPTEST_DISABLE_FAILURE_PERSISTENCE=1
rustc --version --verbose
rustfmt --version
rustfmt --check --edition 2021 crates/worldinterface-foundation/*/src/*.rs crates/worldinterface-foundation/*/tests/*.rs
run_cargo clippy --locked --manifest-path "$manifest" --all-targets --all-features -- -D warnings
run_cargo build --locked --manifest-path "$manifest" --all-features
run_cargo test --locked --manifest-path "$manifest" --all-features
metadata="$(mktemp "$TMPDIR/wi-domain-metadata.XXXXXX")"
trap 'rm -f "$metadata"' EXIT
run_cargo metadata --locked --manifest-path "$manifest" --all-features --format-version 1 > "$metadata"
python3 scripts/check-wi-domain-isolation.py "$metadata"
