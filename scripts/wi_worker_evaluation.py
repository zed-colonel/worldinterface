"""Complete packet reconstruction. Checking never repairs source or the manifest."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

PACKET = "conformance/wi-fabric-2/worker/evaluation-manifest.json"
POLICY = "reviewed-merged-packet-2026-11-09"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def source_bytes(root, name):
    parts = name.split("/")
    if not name or any(p in ("", ".", "..") for p in parts) or "\\" in name:
        raise ValueError("unsafe packet path: " + name)
    current = os.fspath(root)
    for part in parts:
        current = os.path.join(current, part)
        mode = os.lstat(current).st_mode
        if stat.S_ISLNK(mode):
            raise ValueError("packet symlink: " + name)
    if not stat.S_ISREG(mode):
        raise ValueError("missing packet file: " + name)
    with open(current, "rb") as source:
        return source.read()


def walk_error(error):
    raise error


def walk(directory, exclude_build_outputs=False):
    if directory.is_symlink():
        raise ValueError("packet directory symlink: " + str(directory))
    for parent, directories, files in os.walk(directory, followlinks=False, onerror=walk_error):
        if exclude_build_outputs:
            directories[:] = [d for d in directories if d not in ("target", "dev-output")]
        for name in directories + files:
            path = Path(parent) / name
            if path.is_symlink():
                raise ValueError("packet symlink: " + str(path))
            yield path


def expected_manifest(root, read_source=None):
    if read_source is None:
        read_source = lambda name: source_bytes(root, name)
    files = set()
    for directory in ["crates/worldinterface-foundation", "crates/worldinterface-runtime", "tools/wi-contract-check"]:
        for path in walk(root / directory, exclude_build_outputs=True):
            if path.is_file() and (path.suffix in [".rs", ".txt", ".toml"] or path.name == "Cargo.lock"):
                if any(part in ["target", "dev-output"] for part in path.parts):
                    continue
                files.add(path.relative_to(root).as_posix())
    for directory in ["docs/release", "examples/downstream-consumer"]:
        for path in walk(root / directory, exclude_build_outputs=True):
            if path.is_file():
                files.add(path.relative_to(root).as_posix())
    for name in ["scripts/release.py", "scripts/wi_release.py", "scripts/wi_release_packages.py",
                 "scripts/check-packages.py", "scripts/check-consumer.py", "scripts/test_wi_release.py"]:
        files.add(name)
    # Package copies include JSON, C helpers and the immutable repository lock
    # snapshot. Gate copies are compared to authority by handoff check; bind all.
    for directory in ["crates/worldinterface-foundation", "crates/worldinterface-runtime"]:
        for path in walk(root / directory, exclude_build_outputs=True):
            if path.is_file() and (path.suffix in [".json", ".c", ".md", ".lock"] or path.name == "LICENSE"):
                files.add(path.relative_to(root).as_posix())
    for path in walk(root / "fixtures/pre-wi-fabric-2/exo/repos/sample-rust"):
        if path.is_file():
            files.add(path.relative_to(root).as_posix())
    for name in [
        "AGENTS.md", "README.md", ".gitignore", ".cargo/config.toml", ".craftingtable/checks.json",
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
        "scripts/build-wi-worker-evaluation.sh", "scripts/refresh-wi-worker-evaluation.py", "scripts/wi_worker_evaluation.py",
        "scripts/test_wi_worker_evaluation.py",
        "scripts/check-wi-contract.sh", "scripts/check-wi-domain.sh",
        "scripts/check-wi-domain-isolation.py", "scripts/check-wi-integration.sh",
        "scripts/check-wi-integration.py", "scripts/wi_crash_isolation.py", "scripts/worker-proof-evidence.sh",
        "docs/work-items/WSP-01.md", "docs/work-items/WSP-02.md", "LICENSE", "docs/work-items/WIP-11.md", "docs/work-items/WIP-12.md", "docs/work-items/WIP-13.md", "docs/work-items/WIP-14.md", "docs/work-items/WIP-15.md", "docs/work-items/wi-worker-proof-1.md",
        "docs/work-items/WIP-16.md", "docs/work-items/WI-WORKER-PROOF-1-simplification.md", "docs/evidence/WI-EMBEDDED-WORKER-PROOF-1.md",
    ]:
        files.add(name)
    # Bind the actual fixture and normative data, not only source references to their paths.
    for directory in ["contracts", "conformance/wi-fabric-2", "inventory"]:
        for path in walk(root / directory):
            name=path.relative_to(root).as_posix()
            if not path.is_file() or name in ["conformance/wi-fabric-2/worker/evaluation-manifest.json", "conformance/wi-fabric-2/worker/evaluation-approval.json", "inventory/current.json", "inventory/connector-disposition.json"] or name.startswith("conformance/wi-fabric-2/proof/gates/"):
                continue
            files.add(name)
    # Historical trust provisioning is excluded; receipts and anchors are provenance only.
    # The two derived inventories above include the anchor's source hash; the contract
    # checker still regenerates/validates them, but they remain outside this packet.
    files.discard("crates/worldinterface-runtime/tests/support/worker_approval_anchor.rs")
    tasks = [{"task": "probe"}, {"task": "coding"}, {"task": "churn", "children": 512}]
    tasks += [{"task": "sleep", "millis": n} for n in [1, 5000, 30000]]
    tasks += [{"task": "transform", "xor": n} for n in [0, 7]]
    tasks += [{"task": "flood", "bytes": 1048576}]
    tasks += [{"task": "tree", "early_exit": a, "change_session": b, "resist_term": c}
              for a, b, c in [(False, False, False), (False, True, True), (True, True, True)]]
    return {
        "version": 1, "profile": "process-managed-proof-1",
        "environment": digest(read_source("conformance/wi-fabric-2/worker/evaluation-environment.json")),
        "files": {name: digest(read_source(name)) for name in sorted(files)},
        "tasks": sorted({digest(canonical(task)) for task in tasks}),
        "cases": sorted(["WP-007", "WP-032", "AB-WI-021", "WIP-09-adapter-regressions"] + [f"WP-{n:03}" for n in range(17, 27)] + [f"WP-{n:03}" for n in [27, 28, 29, 30, 31, 33, 34]]),
        "operations": sorted(["provision", "start", "inspect", "terminate", "collect_artifacts", "release"]),
        "max_wall_ms": 15000, "max_output_bytes": 65536,
        "nonsensitive_only": True, "production_credentials": False, "ordinary_use": False,
    }


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate manifest key: " + key)
        result[key] = value
    return result


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30).stdout


def check_packet(root, require_committed=False):
    root = Path(root).resolve()
    raw = source_bytes(root, PACKET)
    manifest = json.loads(raw, object_pairs_hook=unique_object)
    # One fresh byte snapshot per invocation feeds both manifest and Git hashes.
    # Never retain this cache between checks: admission/dispatch must detect drift.
    sources = {}
    def read_source(name):
        if name not in sources:
            sources[name] = source_bytes(root, name)
        return sources[name]
    expected = expected_manifest(root, read_source)
    # Canonical byte comparison also distinguishes JSON booleans from integers.
    if canonical(manifest) != canonical(expected):
        raise ValueError("evaluation packet differs from complete current source/scope; refresh required")
    if require_committed:
        # Check the raw manifest and every bound byte, irrespective of index flags.
        # The committed manifest fixes the file set as well as the individual hashes.
        if git(root, "show", "HEAD:" + PACKET) != raw:
            raise ValueError("evaluation packet is not committed at HEAD")
        entries = {}
        for entry in git(root, "ls-tree", "-rz", "HEAD").split(b"\0"):
            if entry:
                meta, name = entry.split(b"\t", 1)
                mode, kind, oid = meta.split()
                entries[name.decode()] = (mode, kind, oid.decode())
        algorithm = git(root, "rev-parse", "--show-object-format").decode().strip()
        for name in manifest["files"]:
            data = read_source(name)
            oid = hashlib.new(algorithm, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            if name not in entries or entries[name][0] not in (b"100644", b"100755") or entries[name][1:] != (b"blob", oid):
                raise ValueError("evaluation source is not committed at HEAD: " + name)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--require-committed", action="store_true")
    parser.add_argument("--serve", action="store_true")
    args = parser.parse_args()

    def result():
        manifest = check_packet(args.root, args.require_committed)
        return {"manifest_digest": digest(canonical(manifest)), "acceptance_policy": POLICY}

    if args.serve:
        # The host owns this process; every request reconstructs a fresh packet.
        for request in sys.stdin:
            try:
                if request != "check\n":
                    raise ValueError("invalid validator request")
                response = result()
            except (ValueError, OSError, subprocess.SubprocessError) as error:
                response = {"error": str(error)}
            print(json.dumps(response), flush=True)
    else:
        try:
            print(json.dumps(result()))
        except (ValueError, OSError, subprocess.SubprocessError) as error:
            parser.exit(1, "evaluation packet rejected: " + str(error) + "\n")
