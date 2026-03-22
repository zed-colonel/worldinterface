#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="${1:-$SCRIPT_DIR/compiled}"

mkdir -p "$OUTPUT_DIR"

for module_dir in "$SCRIPT_DIR"/*/; do
    module_name=$(basename "$module_dir")
    [ "$module_name" = "compiled" ] && continue

    cargo_toml="$module_dir/Cargo.toml"
    [ ! -f "$cargo_toml" ] && continue

    echo "Building $module_name..."
    cargo build --manifest-path "$cargo_toml" --target wasm32-wasip2 --release

    # Find the compiled .wasm file
    wasm_file=$(find "$module_dir/target/wasm32-wasip2/release" -name "*.wasm" -maxdepth 1 2>/dev/null | head -1)
    if [ -n "$wasm_file" ]; then
        cp "$wasm_file" "$OUTPUT_DIR/$module_name.wasm"
        echo "  → $OUTPUT_DIR/$module_name.wasm"
    fi

    # Copy the manifest
    manifest="$module_dir/$module_name.connector.toml"
    if [ -f "$manifest" ]; then
        cp "$manifest" "$OUTPUT_DIR/"
        echo "  → $OUTPUT_DIR/$module_name.connector.toml"
    fi
done

echo "Done. Compiled modules in $OUTPUT_DIR"
