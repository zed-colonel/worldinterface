#!/usr/bin/env python3
"""Prepare the exact candidate packet. This script never writes an acceptance receipt."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def digest(data):
    return hashlib.sha256(data).hexdigest()

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

files = set()
for directory in ["crates/worldinterface-foundation", "crates/worldinterface-runtime", "tools/wi-contract-check"]:
    for path in (ROOT / directory).rglob("*"):
        if path.is_file() and (path.suffix in [".rs", ".txt", ".toml"] or path.name == "Cargo.lock"):
            if any(part in ["target", "dev-output"] for part in path.parts):
                continue
            files.add(path.relative_to(ROOT).as_posix())
for path in (ROOT / "fixtures/pre-wi-fabric-2/exo/repos/sample-rust").rglob("*"):
    if path.is_file():
        files.add(path.relative_to(ROOT).as_posix())
for name in [
    "AGENTS.md", ".gitignore", ".cargo/config.toml", ".craftingtable/checks.json",
    "rust-toolchain.toml", "Cargo.toml", "Cargo.lock",
    "fixtures/pre-wi-fabric-2/source/rust-toolchain.toml",
    "contracts/aq-integration-target.json", "contracts/aq-baseline.lock.json",
    "contracts/aq-consumer-reconciliation.json",
    "contracts/sources/wi-fabric-2-worker-execution-profile.yaml",
    "contracts/sources/wi-fabric-2-worker-execution-profile.md",
    "contracts/sources/wi-fabric-2-worker-execution-acceptance-matrix.yaml",
    "contracts/sources/aq-baseline-integration-acceptance-matrix.yaml",
    "conformance/wi-fabric-2/worker/evaluation-environment.json",
    "conformance/wi-fabric-2/worker/lifecycle-coverage.json",
    "conformance/wi-fabric-2/worker/supervisor-disposition.json",
    "scripts/build-wi-worker-evaluation.sh", "scripts/refresh-wi-worker-evaluation.py",
    "scripts/check-wi-contract.sh", "scripts/check-wi-domain.sh",
    "scripts/check-wi-domain-isolation.py", "scripts/check-wi-integration.sh",
    "scripts/check-wi-integration.py", "scripts/wi_crash_isolation.py",
    "docs/work-items/WIP-11.md", "docs/work-items/WIP-12.md", "docs/work-items/wi-worker-proof-1.md",
]:
    files.add(name)
# Bind the actual fixture and normative data, not only source references to their paths.
for directory in ["contracts", "conformance/wi-fabric-2", "inventory"]:
    for path in (ROOT/directory).rglob("*"):
        name=path.relative_to(ROOT).as_posix()
        if not path.is_file() or name in ["conformance/wi-fabric-2/worker/evaluation-manifest.json", "conformance/wi-fabric-2/worker/evaluation-approval.json", "inventory/current.json", "inventory/connector-disposition.json"] or name.startswith("conformance/wi-fabric-2/proof/gates/"):
            continue
        files.add(name)
# Trust provisioning is separate from the packet, so acceptance does not recursively hash itself.
# The two derived inventories above include the anchor's source hash; the contract
# checker still regenerates/validates them, but they cannot enter this trust digest.
files.remove("crates/worldinterface-runtime/tests/support/worker_approval_anchor.rs")
tasks = [{"task": "probe"}, {"task": "coding"}, {"task": "churn", "children": 512}]
tasks += [{"task": "sleep", "millis": n} for n in [1, 5000, 30000]]
tasks += [{"task": "transform", "xor": n} for n in [0, 7]]
tasks += [{"task": "flood", "bytes": 1048576}]
tasks += [{"task": "tree", "early_exit": a, "change_session": b, "resist_term": c}
          for a, b, c in [(False, False, False), (False, True, True), (True, True, True)]]
manifest = {
    "version": 1, "profile": "process-managed-proof-1",
    "environment": digest((ROOT / "conformance/wi-fabric-2/worker/evaluation-environment.json").read_bytes()),
    "files": {name: digest((ROOT / name).read_bytes()) for name in sorted(files)},
    "tasks": sorted({digest(canonical(task)) for task in tasks}),
    "cases": sorted(["WP-007", "WP-032", "AB-WI-021", "WIP-09-adapter-regressions"] + [f"WP-{n:03}" for n in range(17, 27)] + [f"WP-{n:03}" for n in [27, 28, 31]]),
    "operations": sorted(["provision", "start", "inspect", "terminate", "collect_artifacts", "release"]),
    "max_wall_ms": 15000, "max_output_bytes": 65536,
    "nonsensitive_only": True, "production_credentials": False, "ordinary_use": False,
}
path = ROOT / "conformance/wi-fabric-2/worker/evaluation-manifest.json"
path.write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps({"manifest_digest": digest(canonical(manifest)), "source_files": len(files), "approval": "not-created"}))
