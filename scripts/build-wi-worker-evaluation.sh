#!/usr/bin/env bash
# Compile the packet without executing any process fixture. Never supplies approval.
set -euo pipefail
: "${CARGO_TARGET_DIR:?use the supplied external build cache}"
: "${TMPDIR:?use the supplied run scratch}"
mkdir -p "$TMPDIR/verification"
cargo build --locked --manifest-path crates/worldinterface-runtime/Cargo.toml --bins
timeout 900 cargo test --locked --manifest-path crates/worldinterface-runtime/Cargo.toml --no-run
python3 - <<'PY'
import hashlib,json,os,pathlib,subprocess
repo=pathlib.Path.cwd()
manifest=json.loads((repo/'conformance/wi-fabric-2/worker/evaluation-manifest.json').read_text())
for name,digest in manifest['files'].items():
    if hashlib.sha256((repo/name).read_bytes()).hexdigest()!=digest:
        raise SystemExit('packet source drift: '+name)
result={'version':1,'manifest':hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),'recipe':'scripts/build-wi-worker-evaluation.sh','executables':{}}
for name in ['wi-process-fixture','wi-process-reaper']:
    result['executables'][name]=hashlib.sha256((pathlib.Path(os.environ['CARGO_TARGET_DIR'])/'debug'/name).read_bytes()).hexdigest()
(pathlib.Path(os.environ['TMPDIR'])/'verification/worker-build.json').write_text(json.dumps(result,indent=2)+'\n')
PY
