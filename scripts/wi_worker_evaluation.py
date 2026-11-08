"""Shared encoding and source validation for the evaluation packet."""
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def check_drift(repo):
    manifest = json.loads((repo / "conformance/wi-fabric-2/worker/evaluation-manifest.json").read_text())
    for name, expected in manifest["files"].items():
        if digest((repo / name).read_bytes()) != expected:
            raise SystemExit("packet source drift: " + name)
    return manifest


if __name__ == "__main__":
    check_drift(Path.cwd())
