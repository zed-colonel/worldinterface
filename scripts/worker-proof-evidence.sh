#!/usr/bin/env bash
# Supplemental evidence helper. The adopted runtime test invokes this same campaign.
set -euo pipefail
: "${CARGO_TARGET_DIR:?use the supplied external build cache}"
: "${TMPDIR:?use external scratch}"
mkdir -p "$TMPDIR/verification"
if [[ $# -eq 0 ]]; then
  campaign_output=$(mktemp -d "$TMPDIR/verification/worker-campaign-XXXXXX")
else
  campaign_output=$1
fi
timeout --kill-after=15s 7200s cargo run --locked --offline \
  --manifest-path tools/wi-contract-check/Cargo.toml -- campaign run "$campaign_output"
