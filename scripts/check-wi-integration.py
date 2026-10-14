#!/usr/bin/env python3
"""Verify the runtime's resolved graph uses the published AQ 0.2.0 crates from crates.io.

Usage: check-wi-integration.py <cargo-metadata.json> [<Cargo.lock>]

Outside the cross-project map, AQ comes from crates.io. Each AQ package must be version
0.2.0 from the crates.io index, and its Cargo.lock checksum must equal the published
.crate SHA-256 recorded in contracts/aq-integration-target.json (published_release).
"""
import hashlib
import json
import pathlib
import sys
import tomllib

CRATES_IO = 'registry+https://github.com/rust-lang/crates.io-index'
sys.dont_write_bytecode = True
from wi_crash_isolation import check_repository, check_production_build

root = pathlib.Path(__file__).resolve().parent.parent
meta = json.loads(pathlib.Path(sys.argv[1]).read_text())
lock_path = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else root / 'crates/worldinterface-runtime/Cargo.lock'
contract_path = pathlib.Path(sys.argv[3]) if len(sys.argv) > 3 else root / 'contracts/aq-integration-target.json'
target_file = json.loads(contract_path.read_text())
target, published = target_file['target'], target_file['published_release']
assert published['source_commit'] == target['implementation_commit'], 'published release is not the target commit'
assert target['implementation_commit'] == '29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860', 'historical or substituted source commit'
assert target['git_tree'] == '1c0c85dd700bd9d0c75de223dada911ffeadbb5b', 'wrong source tree'
assert target['source_tree_sha256'] == 'a6a8f87272d76e94e69e10d8bcf0992296d1bf8e28ff3d9222374c3b8cdcc34e', 'wrong complete source tree digest'
assert target['conformance_revision'] == 16, 'wrong conformance revision'
assert published['registry'] == 'crates.io' and published['tag'] == 'v0.2.0', 'wrong published release'
assert hashlib.sha256(json.dumps(published['crates'], sort_keys=True, separators=(',', ':')).encode()).hexdigest() == '5dc10e3c7dcf4698df1b13abd74e72420209a80c91ea9dd23c2750ca8892ed68', 'published checksums changed'
feature_profile = {
    'actionqueue-core': {'default', 'serde'}, 'actionqueue-engine': {'default'},
    'actionqueue-executor-local': set(), 'actionqueue-runtime': {'serde'},
    'actionqueue-storage': {'default', 'serde'}, 'actionqueue-workflow': set(),
}
expected = published['crates']
locked = {p['name']: p for p in tomllib.loads(lock_path.read_text())['package'] if p['name'].startswith('actionqueue-')}
packages = {p['id']: p for p in meta['packages']}
nodes = {n['id']: n for n in meta['resolve']['nodes']}
aq = []
for node in nodes.values():
    p = packages[node['id']]
    if p['name'].startswith('actionqueue-'):
        name = p['name']
        assert name in expected, f'unexpected AQ crate {name}'
        assert p['version'] == expected[name]['version'] == '0.2.0', f'{name} is not 0.2.0'
        assert p['source'] == CRATES_IO, f'{name} does not come from crates.io: {p["source"]}'
        assert locked.get(name, {}).get('checksum') == expected[name]['checksum'], f'{name}: Cargo.lock checksum is not the published one'
        assert name in feature_profile and set(node['features']) == feature_profile[name], f'{name}: unsupported feature profile'
        aq.append({'name': name, 'version': p['version'], 'features': node['features']})
assert {p['name'] for p in aq} >= {'actionqueue-core', 'actionqueue-storage', 'actionqueue-runtime'}
# Inspect the complete production dependency graph, including transitive aliases.
wi = next(p for p in meta['packages'] if p['name'] == 'worldinterface-runtime')
pending = [wi['id']]
seen = set()
allowed = {'worldinterface-runtime', 'worldinterface-storage', 'worldinterface-domain', 'worldinterface-protocol', 'worldinterface-egress', 'worldinterface-ingress', 'worldinterface-worker', 'worldinterface-capability'}
while pending:
    key = pending.pop()
    if key in seen:
        continue
    seen.add(key)
    p = packages[key]
    assert not p['name'].startswith('exoskeleton')
    if p['name'].startswith('worldinterface-'):
        assert p['name'] in allowed, p['name']
    for d in nodes[key]['deps']:
        if any(k['kind'] != 'dev' for k in d['dep_kinds']):
            pending.append(d['pkg'])
check_repository(root)
production = check_production_build(root, "runtime")
print(json.dumps(production, sort_keys=True))
print(json.dumps({'upstream': target, 'registry': published['registry'], 'tag': published['tag'],
                  'resolved_aq': sorted(aq, key=lambda p: p['name']), 'production_graph': 'passed'},
                 sort_keys=True))
