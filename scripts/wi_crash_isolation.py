"""The operator-approved crash hook is empty, dev-only, and absent from builds."""
import json
import pathlib
import subprocess
import tomllib


def check_manifests(manifests):
    for path, manifest in manifests:
        name = manifest.get("package", {}).get("name", "")
        features = manifest.get("features", {})
        if name == "worldinterface-storage":
            assert features == {"crash-points": []}, "storage crash feature must be exactly empty"
        elif name in {"worldinterface-protocol", "worldinterface-capability", "worldinterface-domain",
                      "worldinterface-ingress", "worldinterface-egress", "worldinterface-worker", "worldinterface-runtime"}:
            assert features == {}, "new features require isolation review"
        else:
            assert "crash-points" not in features, "foreign crash feature declaration"
        tables = [manifest]
        tables.extend(manifest.get("target", {}).values())
        if "workspace" in manifest:
            tables.append(manifest["workspace"])
        for table in tables:
            for kind in ("dependencies", "build-dependencies", "dev-dependencies"):
                for alias, dependency in table.get(kind, {}).items():
                    if not isinstance(dependency, dict):
                        continue
                    package = dependency.get("package", alias)
                    if "crash-points" in dependency.get("features", []):
                        assert (name, kind, package) == (
                            "worldinterface-runtime", "dev-dependencies", "worldinterface-storage"
                        ), f"forbidden crash feature activation in {path}: {kind}/{alias}"
        # Patch/replace may redirect storage to an unreviewed hook implementation,
        # even when that edge does not explicitly enable the feature.
        overrides = list(manifest.get("patch", {}).values()) + [manifest.get("replace", {})]
        for table in overrides:
            for alias, dependency in table.items():
                package = dependency.get("package", alias) if isinstance(dependency, dict) else alias
                assert package.split(":", 1)[0] != "worldinterface-storage", "storage patch/replace forbidden"
                if isinstance(dependency, dict):
                    assert "crash-points" not in dependency.get("features", []), "override crash feature activation"
        for forwarded in features.values():
            assert not any("crash-points" in feature for feature in forwarded), "crash feature forwarding"


def check_repository(root):
    manifests = sorted(path for path in root.rglob("Cargo.toml")
                       if not {"target", "dev-output", ".git"}.intersection(path.relative_to(root).parts))
    check_manifests([(path, tomllib.loads(path.read_text())) for path in manifests])
    subprocess.run(["python3", "-B", "scripts/test_wi_crash_isolation.py"], cwd=root, check=True)


def check_artifacts(records):
    storage = []
    for record in records:
        if record.get("reason") != "compiler-artifact":
            continue
        if record["target"]["name"] == "worldinterface_storage":
            storage.append(record)
            assert "crash-points" not in record["features"], "crash hook compiled in production storage"
            assert not record["profile"]["test"], "test build cannot prove production exclusion"
    assert storage, "missing production storage compiler-artifact evidence"


def check_production_build(root, workspace):
    # Resolver v2 selects the real build units. Metadata includes unified dev
    # features and cannot prove which features reached normal compilation.
    command = ["cargo", "build", "--offline", "--locked", "--manifest-path",
               f"crates/worldinterface-{workspace}/Cargo.toml", "--message-format=json"]
    if workspace == "foundation":
        command.append("--workspace")
    result = subprocess.run(command, cwd=root, check=True, stdout=subprocess.PIPE, text=True)
    records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    check_artifacts(records)
    return {"workspace": workspace, "production_crash_points": False,
            "storage_artifacts": [r for r in records if r.get("reason") == "compiler-artifact"
                                  and r["target"]["name"] == "worldinterface_storage"]}
