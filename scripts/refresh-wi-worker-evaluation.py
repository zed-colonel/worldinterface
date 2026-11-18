#!/usr/bin/env python3
"""Prepare candidate bytes; review and merge establish durable acceptance."""
import json
import sys
sys.dont_write_bytecode = True
from pathlib import Path
from wi_worker_evaluation import PACKET, canonical, digest, expected_manifest

root = Path(__file__).resolve().parents[1]
manifest = expected_manifest(root)
(root / PACKET).write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps({"manifest_digest": digest(canonical(manifest)), "source_files": len(manifest["files"])}))
