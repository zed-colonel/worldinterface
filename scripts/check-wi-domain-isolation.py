#!/usr/bin/env python3
"""Supplement Rust source-route checks with the resolved all-features target graph."""
import hashlib
import json
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parent.parent
meta = json.loads(pathlib.Path(sys.argv[1]).read_text())
allowed = {
    "worldinterface-protocol": set(),
    "worldinterface-capability": {"worldinterface-protocol"},
    "worldinterface-domain": {"worldinterface-protocol"},
    "worldinterface-ingress": {"worldinterface-protocol", "worldinterface-domain", "worldinterface-storage"},
    "worldinterface-egress": {"worldinterface-protocol", "worldinterface-domain", "worldinterface-storage"},
    "worldinterface-storage": {"worldinterface-protocol", "worldinterface-domain"},
    "worldinterface-worker": {"worldinterface-protocol", "worldinterface-capability"},
}
packages = {p["id"]: p for p in meta["packages"]}
# Integration verification resolves the shared consumer graph. Inspect every
# reachable dependency from all foundation roots, including optional/dev edges.
nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
pending = [p["id"] for p in packages.values() if p["name"] in allowed]
assert len(pending) == len(allowed)
reachable = set()
while pending:
    key = pending.pop()
    if key in reachable: continue
    reachable.add(key)
    pending.extend(d["pkg"] for d in nodes[key]["deps"])
packages = {k: v for k, v in packages.items() if k in reachable}
for p in packages.values():
    name = p["name"]
    assert not name.startswith("actionqueue"), name
    if name.startswith("worldinterface"):
        assert name in allowed, name
        for d in p["dependencies"]:
            if d["name"].startswith("worldinterface"):
                assert d["name"] in allowed[name], (name, d["name"])
        assert p["features"] == {}, "new features require isolation review"
        assert not any(t["kind"] == ["bin"] for t in p["targets"]), "inactive library scope only"
for p in (root / "crates/worldinterface-foundation").rglob("*.rs"):
    if "src" not in p.parts:
        continue
    # No process/network calls or production port implementations. Test mocks live in tests/.
    s = p.read_text()
    assert not re.search(r"\b(?:std\s*::\s*(?:process|net)|tokio|reqwest|axum|actionqueue)\s*::", s), str(p)
    assert not re.search(r"impl\s+TestEffectAdmissionPort\s+for", s), str(p)
    assert not re.search(r"impl\s+Queue(?:Admission|Inspection|Wake)Port\s+for", s), str(p)
    if "worker/src" in str(p):
        assert not re.search(r"impl\s+WorkerPort\s+for|std\s*::\s*(?:fs|thread|process|net)", s), str(p)
    assert "worldinterface_core" not in s and "worldinterface_contextstore" not in s, str(p)
    if p.relative_to(root / "crates/worldinterface-foundation").parts[0] in ("protocol", "capability", "domain"):
        assert not re.search(r"std\s*::\s*(?:fs|thread)|SystemTime\s*::\s*now|Utc\s*::\s*now", s), str(p)
coverage = json.loads((root / "conformance/wi-fabric-2/ingress/coverage.json").read_text())
assert coverage["scope"] == "wi/WI-03/domain" and coverage["upstream_conformance"] is False
seen = set()
for obligation in coverage["obligations"]:
    name = obligation["test"]
    assert name not in seen, "duplicate ingress coverage"
    seen.add(name)
    package = obligation["package"]
    directory = root / ("tools/wi-contract-check" if package == "wi-contract-check"
                        else "crates/worldinterface-foundation/" + package.removeprefix("worldinterface-"))
    assert any(re.search(r"#\[test\]\s*fn\s+" + re.escape(name) + r"\s*\(", p.read_text())
               for p in directory.rglob("*.rs")), name
egress_coverage = json.loads((root / "conformance/wi-fabric-2/egress/coverage.json").read_text())
assert egress_coverage["scope"] == "wi/WI-04/domain" and egress_coverage["upstream_conformance"] is False
assert set(egress_coverage["assigned_cases"]) == {f"WP-{n:03}" for n in range(9, 15)}
for case, tests in egress_coverage["assigned_cases"].items():
    assert tests, case
for obligation in egress_coverage["obligations"]:
    directory = root / "crates/worldinterface-foundation" / obligation["package"].removeprefix("worldinterface-")
    assert any(re.search(r"#\[test\]\s*fn\s+" + re.escape(obligation["test"]) + r"\s*\(", p.read_text())
               for p in directory.rglob("*.rs")), obligation
fixture_dir = root / "conformance/wi-fabric-2/domain"
worker_coverage = json.loads((root / "conformance/wi-fabric-2/worker/coverage.json").read_text())
assert worker_coverage["scope"] == "wi/WI-09/domain" and worker_coverage["upstream_conformance"] is False
assert {c["id"] for c in worker_coverage["cases"]} == {"WP-001", "WP-003", "WP-004", "WP-005", "WP-006", "WP-008"}
for case in worker_coverage["cases"]:
    assert case["package"] == "worldinterface-worker"
    assert any(re.search(r"#\[test\]\s*fn\s+" + re.escape(case["test"]) + r"\s*\(", p.read_text())
               for p in (root / "crates/worldinterface-foundation/worker/tests").rglob("*.rs")), case
receipt_coverage = json.loads((root / "conformance/wi-fabric-2/receipts/coverage.json").read_text())
assert receipt_coverage["scope"] == "wi/WI-05/domain" and receipt_coverage["upstream_conformance"] is False
assert set(receipt_coverage["assigned_cases"]) == {"WP-015", "WP-016"}
for obligation in receipt_coverage["obligations"]:
    directory = root / "crates/worldinterface-foundation" / obligation["package"]
    assert any(re.search(r"#\[test\]\s*fn\s+" + re.escape(obligation["test"]) + r"\s*\(", p.read_text())
               for p in directory.rglob("*.rs")), obligation
print(json.dumps({"scopes": ["wi/WI-02/domain", "wi/WI-03/domain", "wi/WI-04/domain", "wi/WI-05/domain", "wi/WI-09/domain"], "isolation": "passed", "aq_packages": [],
                  "production_adapter": False, "runtime_conformance": False,
                  "fixtures": {str(p.relative_to(fixture_dir.parent)): hashlib.sha256(p.read_bytes()).hexdigest()
                               for p in sorted(fixture_dir.parent.glob("*/**/*.json"))}}, sort_keys=True))
