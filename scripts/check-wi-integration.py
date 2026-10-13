#!/usr/bin/env python3
"""Verify the runtime's resolved graph uses the published AQ 0.2.0 crates from crates.io.

Usage: check-wi-integration.py <cargo-metadata.json> [<Cargo.lock>]

Outside the cross-project map, AQ comes from crates.io. Each AQ package must be version
0.2.0 from the crates.io index, and its Cargo.lock checksum must equal the published
.crate SHA-256 recorded in contracts/aq-integration-target.json (published_release).
"""
import json
import pathlib
import sys
import tomllib

CRATES_IO = 'registry+https://github.com/rust-lang/crates.io-index'
root = pathlib.Path(__file__).resolve().parent.parent
meta = json.loads(pathlib.Path(sys.argv[1]).read_text())
lock_path = pathlib.Path(sys.argv[2]) if len(sys.argv) > 2 else root / 'crates/worldinterface-runtime/Cargo.lock'
target_file = json.loads((root / 'contracts/aq-integration-target.json').read_text())
target, published = target_file['target'], target_file['published_release']
assert published['source_commit'] == target['implementation_commit'], 'published release is not the target commit'
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
        assert not set(node['features']) & {'actor', 'budget', 'platform', 'workflow'}, f'{name}: unexpected feature'
        aq.append({'name': name, 'version': p['version'], 'features': node['features']})
assert {p['name'] for p in aq} >= {'actionqueue-core', 'actionqueue-storage', 'actionqueue-runtime'}
# Inspect the complete production dependency graph, including transitive aliases.
wi = next(p for p in meta['packages'] if p['name'] == 'worldinterface-runtime')
pending = [wi['id']]
seen = set()
allowed = {'worldinterface-runtime', 'worldinterface-storage', 'worldinterface-domain', 'worldinterface-protocol', 'worldinterface-egress', 'worldinterface-ingress'}
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
print(json.dumps({'upstream': target, 'registry': published['registry'], 'tag': published['tag'],
                  'resolved_aq': sorted(aq, key=lambda p: p['name']), 'production_graph': 'passed'},
                 sort_keys=True))
