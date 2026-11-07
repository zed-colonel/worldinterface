#!/usr/bin/env bash
# Compile and measure the packet without executing process fixtures or granting approval.
set -euo pipefail
: "${CARGO_TARGET_DIR:?use the supplied external build cache}"
: "${TMPDIR:?use the supplied run scratch}"
mkdir -p "$TMPDIR/verification/revised-packet"
python3 - <<'PY'
import hashlib,json,pathlib
repo=pathlib.Path.cwd()
manifest=json.loads((repo/'conformance/wi-fabric-2/worker/evaluation-manifest.json').read_text())
for name,digest in manifest['files'].items():
    if hashlib.sha256((repo/name).read_bytes()).hexdigest()!=digest:
        raise SystemExit('packet source drift: '+name)
PY
# Do not prebuild binaries. Cargo must compile all tests from a cold cache without
# requiring unfinished executable bytes during macro expansion.
timeout 1800 cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --lib --test '*' --no-run --message-format=json > "$TMPDIR/verification/revised-packet/cargo-build.jsonl"
# These two tests build and publish reviewed artifacts and reject path replacement.
# They launch no fixture/helper and do not require or create an evaluation approval.
timeout 1200 cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --test worker_build
python3 - <<'PY'
import hashlib,json,os,pathlib
repo=pathlib.Path.cwd()
manifest=json.loads((repo/'conformance/wi-fabric-2/worker/evaluation-manifest.json').read_text())
for name,digest in manifest['files'].items():
    if hashlib.sha256((repo/name).read_bytes()).hexdigest()!=digest:
        raise SystemExit('packet source drift during validation: '+name)
digest=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()
verification=pathlib.Path(os.environ['TMPDIR'])/'verification/revised-packet'
records=[]
for path in verification.glob('trusted-build-*.json'):
    record=json.loads(path.read_text())
    if record['manifest']==digest:
        records.append((path.stat().st_mtime,record))
if not records:
    raise SystemExit('missing trusted worker build result')
result=max(records,key=lambda r:r[0])[1]
result['recipe']='scripts/build-wi-worker-evaluation.sh'
result['builder']='crates/worldinterface-runtime/tests/support/worker_build.rs'
result['artifact_limit_bytes']=16*1024*1024
for artifact in result['executables'].values():
    if artifact['bytes']>result['artifact_limit_bytes']:
        raise SystemExit('oversized worker build artifact')
(verification/'worker-build.json').write_text(json.dumps(result,indent=2)+'\n')
PY
