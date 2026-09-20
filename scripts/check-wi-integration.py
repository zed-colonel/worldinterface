#!/usr/bin/env python3
"""Verify the resolved consumer graph and controller-supplied current AQ bytes."""
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parent.parent
meta = json.loads(pathlib.Path(sys.argv[1]).read_text())
supplied = json.loads(pathlib.Path(sys.argv[2]).read_text())
target = json.loads((root / 'contracts/aq-integration-target.json').read_text())['target']
identity = next(v for v in supplied['dependencyIdentities'] if v['alias'] == 'aq')
assert identity['commitSha'] == target['implementation_commit'], 'stale AQ commit'
assert identity['treeSha'] == target['git_tree'], 'stale AQ tree'
pins = {p['name']: pathlib.Path(p['path']).resolve() for p in supplied['packages']}
packages = {p['id']: p for p in meta['packages']}
nodes = {p['id']: p for p in meta['resolve']['nodes']}
aq = []
for node in nodes.values():
    p = packages[node['id']]
    if p['name'].startswith('actionqueue-'):
        assert p['name'] in pins and p['version'] == '0.2.0'
        assert p['source'] is None and pathlib.Path(p['manifest_path']).parent.resolve() == pins[p['name']]
        assert not set(node['features']) & {'actor', 'budget', 'platform', 'workflow'}
        aq.append({'name': p['name'], 'version': p['version'], 'features': node['features']})
assert {p['name'] for p in aq} >= {'actionqueue-core', 'actionqueue-storage', 'actionqueue-runtime'}
# Inspect the complete production dependency graph, including transitive aliases.
wi = next(p for p in meta['packages'] if p['name'] == 'worldinterface-runtime')
pending = [wi['id']]
seen = set()
allowed = {'worldinterface-runtime', 'worldinterface-storage', 'worldinterface-domain', 'worldinterface-protocol'}
while pending:
    key = pending.pop()
    if key in seen: continue
    seen.add(key)
    p = packages[key]
    assert not p['name'].startswith('exoskeleton')
    if p['name'].startswith('worldinterface-'): assert p['name'] in allowed, p['name']
    for d in nodes[key]['deps']:
        if any(k['kind'] != 'dev' for k in d['dep_kinds']): pending.append(d['pkg'])
source_root = pins['actionqueue-core'].parent.parent
files = []
for f in supplied['files']:
    p = pathlib.Path(f['path'])
    assert hashlib.sha256(p.read_bytes()).hexdigest() == f['digest'], str(p)
    if p.is_relative_to(source_root): files.append((p.relative_to(source_root).as_posix(), f['digest']))
# Source lock records SHA-256 over sorted '<sha256>  <relative path>\n' rows.
tree = hashlib.sha256(''.join(f'{h}  {p}\n' for p, h in sorted(files)).encode()).hexdigest()
assert tree == target['source_tree_sha256'], (tree, target['source_tree_sha256'])
manifest_text = (source_root / 'conformance/aq-cont-1/manifest.yaml').read_text()
# AQ ships this manifest as JSON-compatible YAML.
conformance = json.loads(manifest_text)
assert conformance['package_revision'] == target['conformance_revision']
print(json.dumps({'scope': 'wi/WI-02/integration', 'upstream': target, 'source_files': len(files),
                  'resolved_aq': sorted(aq, key=lambda p: p['name']), 'production_graph': 'passed',
                  'independent_review': 'pending', 'parent_acceptance': False}, sort_keys=True))
