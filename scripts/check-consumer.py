#!/usr/bin/env python3
"""Verify a standalone registry consumer and the version-pinned worker install route."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
sys.dont_write_bytecode = True
from wi_release import (ROOT, RUNTIME, PACKAGES, check_graph, read, ref, require, run,
                        sha, toml, toolchain, worker_recipe, worker_environment, recipe_digest, write)
from wi_release_packages import cargo, environment
from wi_crash_isolation import check_artifacts


def consumer(output, work, version):
    folder = work / 'consumer'
    shutil.copytree(ROOT / 'examples/downstream-consumer', folder, ignore=shutil.ignore_patterns('Cargo.lock', 'target'))
    base = cargo(work / 'registry.toml'); env = environment(work)
    # Resolve a fresh consumer lock from the versions available in the candidate
    # registry; preserve it separately from the packaged runtime lock.
    run(base + ['generate-lockfile', '--offline'], folder, output / 'logs/consumer-lock.log', env, 180)
    run(base + ['metadata', '--offline', '--locked', '--format-version', '1'], folder, output / 'logs/consumer-metadata.log', env, 180, stdout_path=output / 'consumer-resolution.json')
    meta = read(output / 'consumer-resolution.json')
    checksums = {n: sha(output / f'crates/{n}-{version}.crate') for n in PACKAGES}
    check_graph(meta, toml(folder / 'Cargo.lock'), 'wi-downstream-consumer', version, checksums)
    for node in meta['resolve']['nodes']:
        require('crash-points' not in node['features'], 'consumer crash feature leakage')
    write(output / 'consumer-resolution.json', meta)
    shutil.copyfile(folder / 'Cargo.lock', output / 'consumer-Cargo.lock')
    runtime = next(p for p in meta['packages'] if p['name'] == 'worldinterface-runtime')
    source = Path(runtime['manifest_path']).parent
    for rel in ['release/handoff.json', 'release/repository-Cargo.lock', *['release/gates/' + x.name for x in (ROOT / RUNTIME / 'release/gates').glob('*.json')]]:
        require((source / rel).read_bytes() == (ROOT / RUNTIME / rel).read_bytes(), 'consumer packaged evidence mismatch')
    run(base + ['build', '--offline', '--locked', '--message-format=json'], folder, output / 'logs/consumer-production.log', env, 900)
    records = [__import__('json').loads(line) for line in (output / 'logs/consumer-production.log').read_text().splitlines() if line.startswith('{')]
    check_artifacts(records)
    write(output / 'production/consumer.json', records)
    run(base + ['run', '--offline', '--locked'], folder, output / 'logs/consumer-run.log', env, 300)
    install = work / 'install'
    roots = {str(Path(p['manifest_path']).parent.parent) for p in meta['packages'] if p['source']}
    env = worker_environment(env, work, roots, toolchain(env))
    build = toolchain(env)
    write(output / 'build-environment.json', build)
    recipe = worker_recipe(version, build['target'])
    command = recipe['argv'].copy(); command[command.index('{install-root}')] = str(install)
    command[2:2] = ['--config', str(work / 'registry.toml')]
    # Offline is a cargo install option, and the environment also forbids network.
    command.append('--offline')
    run(command, work, output / 'logs/worker-install.log', env, 1200)
    binaries = {}
    for name in ['wi-process-fixture', 'wi-process-reaper']:
        dest = output / 'workers' / name; dest.parent.mkdir(exist_ok=True)
        shutil.copy2(install / 'bin' / name, dest)
        binaries[name] = dict(ref(output, dest), size=dest.stat().st_size)
    write(output / 'consumer-handoff.json', {
        'schema_version': 1, 'kind': 'wi-release-consumer-handoff-v1',
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True, timeout=30).strip(),
        'descriptor_sha256': sha(source / 'release/handoff.json'), 'version': version,
        'archives': checksums, 'repository_lock_sha256': sha(source / 'release/repository-Cargo.lock'),
        'packaged_lock_sha256': sha(work / 'extracted/worldinterface-runtime/Cargo.lock'),
        'consumer_lock_sha256': sha(output / 'consumer-Cargo.lock'), 'build': build,
        'recipe': recipe, 'recipe_sha256': recipe_digest(recipe), 'install_command': command, 'executables': binaries,
        'digest_scope': 'Exact recorded compiler, target, profile, environment and linker/system; no universal reproducibility claim.',
        'publication_status': 'not-performed'})


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--work', type=Path, required=True)
    p.add_argument('--version', required=True)
    a = p.parse_args(); consumer(a.output, a.work, a.version)
