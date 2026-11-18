#!/usr/bin/env bash
# Compile and measure the packet without executing process fixtures.
set -euo pipefail
repo="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$repo"
: "${CARGO_TARGET_DIR:?use the supplied external build cache}"
: "${TMPDIR:?use the supplied run scratch}"
mkdir -p "$TMPDIR/verification/revised-packet"
python3 -I -B "$repo/scripts/wi_worker_evaluation.py"
# Do not prebuild binaries. Cargo must compile all tests from a cold cache without
# requiring unfinished executable bytes during macro expansion.
timeout 1800 cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --lib --test '*' --no-run --message-format=json > "$TMPDIR/verification/revised-packet/cargo-build.jsonl"
# These two tests build and publish reviewed artifacts and reject path replacement.
# They launch no fixture/helper and exercise non-live artifact publication.
timeout 1200 cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --test worker_build
python3 -I -B - "$repo" <<'PY'
import json,os,pathlib,sys
sys.path.append(str(pathlib.Path(sys.argv[1]) / 'scripts'))
from wi_worker_evaluation import canonical, check_packet, digest
manifest=check_packet(pathlib.Path(sys.argv[1]))
packet_digest=digest(canonical(manifest))
verification=pathlib.Path(os.environ['TMPDIR'])/'verification/revised-packet'
records=[]
for path in verification.glob('trusted-build-*.json'):
    record=json.loads(path.read_text())
    if record['manifest']==packet_digest:
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
