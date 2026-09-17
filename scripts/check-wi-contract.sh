#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${CRAFTINGTABLE_CARGO_CONFIG:?controller Cargo config is required}"
: "${CRAFTINGTABLE_DEPENDENCY_MANIFEST:?controller dependency manifest is required}"
: "${CARGO_TARGET_DIR:?external target directory is required}"
case "$CARGO_TARGET_DIR" in "$PWD"/*) echo 'build output must be outside the worktree' >&2; exit 1;; esac
manifest=tools/wi-contract-check/Cargo.toml
controller_cargo="$(dirname "$CRAFTINGTABLE_DEPENDENCY_MANIFEST")/bin/cargo"
run_cargo() {
  if [[ "$(command -v cargo)" == "$controller_cargo" ]]; then
    # The session launcher injects pins and rejects all explicit --config arguments.
    cargo "$@"
  else
    # act's Rust installation must explicitly consume the mounted controller pins.
    cargo --config "$CRAFTINGTABLE_CARGO_CONFIG" "$@"
  fi
}
rustc --version --verbose
rustfmt --version
rustfmt --check --edition 2021 tools/wi-contract-check/src/*.rs tools/wi-contract-check/tests/*.rs
run_cargo clippy --locked --manifest-path "$manifest" --all-targets -- -D warnings
run_cargo build --locked --manifest-path "$manifest"
run_cargo test --locked --manifest-path "$manifest"
"$CARGO_TARGET_DIR/debug/wi-contract-check" all check
