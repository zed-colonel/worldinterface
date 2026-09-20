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
    "worldinterface-storage": {"worldinterface-protocol", "worldinterface-domain"},
}
packages = {p["id"]: p for p in meta["packages"]}
assert {packages[p]["name"] for p in meta["workspace_members"]} == set(allowed)
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
    assert not re.search(r"impl\s+Queue(?:Admission|Inspection|Wake)Port\s+for", s), str(p)
    assert "worldinterface_core" not in s and "worldinterface_contextstore" not in s, str(p)
    if p.relative_to(root / "crates/worldinterface-foundation").parts[0] in ("protocol", "capability", "domain"):
        assert not re.search(r"std\s*::\s*(?:fs|thread)|SystemTime\s*::\s*now|Utc\s*::\s*now", s), str(p)
fixture_dir = root / "conformance/wi-fabric-2/domain"
print(json.dumps({"scope": "wi/WI-02/domain", "isolation": "passed", "aq_packages": [],
                  "production_adapter": False, "runtime_conformance": False,
                  "fixtures": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                               for p in sorted(fixture_dir.glob("*.json"))}}, sort_keys=True))
