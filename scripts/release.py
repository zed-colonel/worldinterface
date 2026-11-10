#!/usr/bin/env python3
"""Prepare or verify WI release evidence on one clean commit. Publishes nothing.

Adapted from ActionQueue v0.3.0's prepare/verify structure. Python 3.11+.
Failed/interrupted preparations retain logs and never leave a successful manifest.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
sys.dont_write_bytecode = True
from wi_release import (ROOT, FOUNDATION, RUNTIME, PACKAGES, ADOPTED, BASELINE, TOOLCHAIN, archive_files,
                        clean, check_graph, external_target, git, metadata, read, ref, require, run, worker_recipe, recipe_digest,
                        safe, sha, toolchain, toml, validate_archive, validate_schema, write)
from wi_crash_isolation import check_artifacts


def gates():
    adopted = read(ROOT / '.craftingtable/checks.json')['checks']
    require([c['id'] for c in adopted] == ADOPTED, 'adopted gate inventory changed; review release coverage')
    # Reject static source/binding failures before the expensive live campaign.
    # Preserve every adopted argv; this changes only preparation's run order.
    result = sorted(((c['id'], c['argv']) for c in adopted), key=lambda gate: gate[0] == 'runtime-test')
    common = ['--version', '{version}', '--output', '{output}', '--work', '{work}']
    return result + [
        ('packages', ['python3', '-B', 'scripts/check-packages.py', *common]),
        ('rustdoc', ['python3', '-B', 'scripts/check-packages.py', *common, '--rustdoc']),
        ('consumer', ['python3', '-B', 'scripts/check-consumer.py', *common])]


def preflight(version, commit, output):
    clean(commit)
    require(not output.is_symlink(), 'symlink output directory')
    output = output.resolve()
    require(not output.is_relative_to(ROOT.resolve()), 'output is inside repository')
    require(not output.exists() or (output.is_dir() and not any(output.iterdir())), 'output directory is not empty')
    external_target()
    build = toolchain()
    meta = metadata(version)
    return output, build, meta


def publication_command(path):
    return ['cargo', 'run', '--locked', '--offline', '--manifest-path', 'tools/wi-contract-check/Cargo.toml',
            '--', 'publication', 'check', str(path)]


def sums(output, paths):
    return ''.join(f'{sha(safe(output, p))}  {p}\n' for p in sorted(paths))


def validate_evidence(manifest, output):
    expected = gates()
    evidence = manifest['evidence']
    require(len(evidence) == len(expected), 'missing release gates')
    for e, (name, command) in zip(evidence, expected):
        require(e['gate'] == name and e['command'] == command and e['result'] == 'passed' and e['source_commit'] == manifest['source_commit'], 'wrong/failed gate identity')
        require(e['log'] == 'logs/' + name + '.log' and e['record'] == 'logs/' + name + '.result.json', 'wrong gate paths')
        require(sha(safe(output, e['log'])) == e['sha256'], 'gate log drift')
        result = read(safe(output, e['record']))
        resolved = [x.replace('{output}', manifest['preparation_output']).replace('{work}', manifest['build_work']).replace('{version}', manifest['version']) for x in command]
        require(result['exit_code'] == 0 and result['result'] == 'passed' and result['argv'] == resolved and result['elapsed_seconds'] >= 0, 'wrong gate result/command')


def validate_receipt(manifest, output):
    receipt = read(safe(output, 'publication-preparation.json'))
    require(receipt['source_revision'] == manifest['source_commit'] and receipt['baseline_revision'] == manifest['baseline_revision'], 'receipt/source disagreement')
    require(receipt['owner'] == 'operator' and receipt['publication_status'] == 'not-performed', 'wrong publication authority/status')
    require(receipt['environment_sha256'] == sha(safe(output, 'build-environment.json')) and receipt['toolchain'] == 'Rust and Cargo 1.89.0', 'receipt/environment disagreement')
    artifact_map = {a['path']: a for a in manifest['artifacts']}
    # All evidence existing before receipt validation is bound by the receipt.
    expected_paths = set(artifact_map) - {'publication-preparation.json', 'logs/publication-validation.log', 'logs/publication-validation.result.json'}
    require(receipt['artifacts'] == [artifact_map[p] for p in sorted(expected_paths)], 'receipt/artifact disagreement')
    require(receipt['verification_receipts'] == [artifact_map[e['record']] for e in manifest['evidence']], 'receipt/gate disagreement')
    require(receipt['commands'] == [{'argv': read(output / e['record'])['argv'], 'exit_code': 0, 'log': artifact_map[e['log']]} for e in manifest['evidence']], 'receipt/command disagreement')
    # Reuse the existing schema and artifact authority, with no replacement schema.
    subprocess.run(publication_command(output / 'publication-preparation.json'), cwd=ROOT, check=True, stdout=subprocess.DEVNULL, timeout=120)
    result = read(safe(output, 'logs/publication-validation.result.json'))
    require(result['exit_code'] == 0 and result['result'] == 'passed' and result['argv'] == publication_command(Path(manifest['preparation_output']) / 'publication-preparation.json'), 'missing receipt validation evidence')


def validate_packages(manifest, output):
    version = manifest['version']
    checksums = {n: sha(safe(output, f'crates/{n}-{version}.crate')) for n in PACKAGES}
    report = read(safe(output, 'package-verification.json'))
    require(set(report) == set(PACKAGES), 'incomplete package report')
    inventory = read(ROOT / 'docs/release/package-tests.json')
    locks = {}
    for name in PACKAGES:
        files = validate_archive(output / f'crates/{name}-{version}.crate', name, version, checksums)
        digest = __import__('hashlib').sha256(files['Cargo.lock']).hexdigest()
        locks[name] = digest
        expected_tests = [['test', '--lib'], *[['test', '--test', r['target']] for r in inventory['packages'][name]['tests'] if r['disposition'] == 'packaged'], ['test', '--doc']]
        repository_digest = sha(ROOT / (RUNTIME if name == 'worldinterface-runtime' else FOUNDATION) / 'Cargo.lock')
        require(report[name] == {'archive_sha256': checksums[name], 'packaged_lock_sha256': digest, 'repository_lock_sha256': repository_digest, 'test_commands': expected_tests, 'result': 'passed'}, 'package lock provenance/report disagreement')
        for index in range(len(expected_tests)):
            record = read(safe(output, f'logs/test-{name}-{index}.result.json'))
            require(record['exit_code'] == 0 and record['result'] == 'passed' and record['argv'] == ['cargo', '--config', manifest['build_work'] + '/registry.toml', *expected_tests[index], '--locked', '--offline'], 'missing/failed package test')
        if name in ['worldinterface-storage', 'worldinterface-egress', 'worldinterface-ingress', 'worldinterface-runtime']:
            check_artifacts(read(safe(output, f'production/{name}.json')))
        meta = read(safe(output, f'resolutions/{name}.json'))
        validate_resolution(meta, name, version)
    require(manifest['packaged_locks'] == locks, 'manifest packaged locks disagreement')
    handoff = read(safe(output, 'consumer-handoff.json'))
    require(handoff['kind'] == 'wi-release-consumer-handoff-v1' and handoff['schema_version'] == 1 and handoff['source_commit'] == manifest['source_commit'] and handoff['version'] == version, 'wrong consumer handoff identity')
    require(handoff['descriptor_sha256'] == manifest['descriptor_sha256'] and handoff['archives'] == checksums, 'wrong handoff archives/descriptor')
    require(handoff['repository_lock_sha256'] == repository_digest and handoff['packaged_lock_sha256'] == locks['worldinterface-runtime'], 'repository/package lock provenance confusion')
    require(handoff['consumer_lock_sha256'] == sha(safe(output, 'consumer-Cargo.lock')) and handoff['build'] == read(safe(output, 'build-environment.json')), 'handoff build identity disagreement')
    recipe = worker_recipe(version, handoff['build']['target'])
    require(handoff['recipe'] == recipe and handoff['recipe_sha256'] == recipe_digest(recipe), 'worker recipe drift')
    command = recipe.copy(); command[command.index('{install-root}')] = manifest['build_work'] + '/install'
    command[2:2] = ['--config', manifest['build_work'] + '/registry.toml']; command.append('--offline')
    require(handoff['install_command'] == command, 'worker install command drift')
    install = read(safe(output, 'logs/worker-install.result.json'))
    require(install['argv'] == command and install['exit_code'] == 0 and install['result'] == 'passed', 'missing/failed worker installation')
    require(handoff['publication_status'] == 'not-performed', 'invalid handoff publication claim')
    require(set(handoff['executables']) == {'wi-process-fixture', 'wi-process-reaper'}, 'missing worker executable')
    for name, record in handoff['executables'].items():
        require(record['path'] == 'workers/' + name and record['sha256'] == sha(safe(output, record['path'])) and record['size'] == (output / record['path']).stat().st_size, 'worker executable drift')
    consumer_meta = read(safe(output, 'consumer-resolution.json'))
    validate_resolution(consumer_meta, 'wi-downstream-consumer', version)
    check_graph(consumer_meta, toml(safe(output, 'consumer-Cargo.lock')), 'wi-downstream-consumer', version, checksums, inspect_manifests=False)
    check_artifacts(read(safe(output, 'production/consumer.json')))


def validate_resolution(meta, root_name, version):
    from wi_release import CRATES_IO
    roots = [p for p in meta['packages'] if p['name'] == root_name]
    require(len(roots) == 1, 'wrong resolution root')
    wi = []
    aq = []
    for package in meta['packages']:
        require(package['source'] == CRATES_IO or package['id'] == roots[0]['id'], 'substituted resolved dependency')
        if package['name'] in PACKAGES:
            require(package['version'] == version, 'wrong resolved package version'); wi.append(package['name'])
        if package['name'].startswith('actionqueue-'):
            require(package['version'] == '0.3.0' and package['source'] == CRATES_IO, 'substituted resolved AQ'); aq.append(package['name'])
        for dep in package['dependencies']:
            require(dep['source'] == CRATES_IO and dep.get('path') is None, 'substituted resolution edge')
    if root_name in ['wi-downstream-consumer', 'worldinterface-runtime']:
        require(set(wi) == set(PACKAGES) and set(aq) == set(read(ROOT / 'contracts/aq-integration-target.json')['runtime_release']['crates']), 'incomplete consumer/runtime graph')
    else:
        require(not aq, 'foundation resolved AQ')


def validate_manifest(manifest, output):
    validate_schema(read(ROOT / 'docs/release/release-manifest.schema.json'), manifest)
    clean(manifest['source_commit'])
    expected = metadata(manifest['version'])
    for key, value in expected.items():
        require(manifest.get(key) == value, 'wrong manifest ' + key)
    require(set(manifest) == set(expected) | {'source_commit', 'baseline_revision', 'source_before', 'source_after', 'build_work', 'preparation_output', 'evidence', 'artifacts', 'packaged_locks'}, 'unknown/missing manifest fields')
    require(manifest['baseline_revision'] == BASELINE, 'wrong baseline revision')
    subprocess.run(['git', 'merge-base', '--is-ancestor', BASELINE, manifest['source_commit']], cwd=ROOT, check=True, timeout=60)
    identity = {'commit': manifest['source_commit'], 'tree': git('rev-parse', 'HEAD^{tree}'), 'clean': True}
    require(manifest['source_before'] == manifest['source_after'] == identity, 'source changed during preparation')
    require(isinstance(manifest['preparation_output'], str) and Path(manifest['preparation_output']).is_absolute(), 'invalid preparation output identity')
    artifacts = manifest['artifacts']
    require(len({a['path'] for a in artifacts}) == len(artifacts), 'duplicate artifact')
    for a in artifacts:
        require(set(a) == {'path', 'sha256'} and sha(safe(output, a['path'])) == a['sha256'], 'artifact checksum mismatch')
    paths = {a['path'] for a in artifacts}
    required = {'source.bundle', 'release-notes.md', 'release-manifest.schema.json', 'package-tests.json', 'build-environment.json', 'package-verification.json', 'consumer-resolution.json', 'consumer-Cargo.lock', 'consumer-handoff.json', 'publication-preparation.json', 'api-docs.tar.gz', 'logs/publication-validation.log', 'logs/publication-validation.result.json', 'workers/wi-process-fixture', 'workers/wi-process-reaper'}
    required |= {f'crates/{n}-{manifest["version"]}.crate' for n in PACKAGES}
    required |= {p for e in manifest['evidence'] for p in [e['log'], e['record']]}
    require(required <= paths, 'missing release artifacts')
    actual = {p.relative_to(output).as_posix() for p in output.rglob('*') if p.is_file() or p.is_symlink()}
    require(actual == paths | {'release-manifest.json', 'SHA256SUMS'}, 'unlisted or missing output files')
    for source, dest in [(f'docs/release/{manifest["version"]}.md', 'release-notes.md'), ('docs/release/package-tests.json', 'package-tests.json'), ('docs/release/release-manifest.schema.json', 'release-manifest.schema.json')]:
        require(sha(safe(output, dest)) == sha(ROOT / source), 'stale release document')
    build = read(safe(output, 'build-environment.json'))
    require('release: 1.89.0\n' in build['rustc'] and build['cargo'].startswith('cargo 1.89.0 ') and build['target'] == 'x86_64-unknown-linux-gnu' and build['profile'] == 'release' and build['build_environment']['RUSTUP_TOOLCHAIN'] == TOOLCHAIN, 'wrong recorded build environment')
    heads = subprocess.check_output(['git', 'bundle', 'list-heads', str(output / 'source.bundle')], text=True, timeout=60).splitlines()
    require(heads == [manifest['source_commit'] + ' HEAD'], 'wrong source bundle identity')
    subprocess.run(['git', 'bundle', 'verify', str(output / 'source.bundle')], cwd=ROOT, check=True, capture_output=True, timeout=60)
    validate_evidence(manifest, output)
    validate_packages(manifest, output)
    validate_receipt(manifest, output)
    require((output / 'SHA256SUMS').read_text() == sums(output, paths | {'release-manifest.json'}), 'checksum inventory disagreement')


def plan(path):
    if shutil.which('crate-release'):
        subprocess.run(['crate-release', 'plan', str(path)], check=True, timeout=120)
    # Local validation above enforces the supplied crate-release contract even
    # when the optional offline publisher executable is unavailable.


def verify(path):
    require(not path.is_symlink() and path.name == 'release-manifest.json', 'unsafe manifest path')
    path = path.resolve()
    validate_manifest(read(path), path.parent)
    plan(path)
    print('Verified prepared release at ' + str(path) + '; publication remains operator-owned.', flush=True)


def prepare(version, commit, output):
    output, build, manifest = preflight(version, commit, output)
    output.mkdir(parents=True, exist_ok=True)
    # The evidence directory is persistent; registry/build staging is disposable.
    with tempfile.TemporaryDirectory(prefix='wi-release-') as temporary:
        work = Path(temporary) / 'work'
        manifest.update(source_commit=commit, baseline_revision=BASELINE,
                        source_before={'commit': commit, 'tree': git('rev-parse', 'HEAD^{tree}'), 'clean': True}, build_work=str(work), preparation_output=str(output))
        write(output / 'build-environment.json', build)
        evidence = []
        env = dict(os.environ, CARGO_TARGET_DIR=str(external_target()), PYTHONDONTWRITEBYTECODE='1', RUSTUP_TOOLCHAIN=TOOLCHAIN)
        # Fetch before all offline gates; these source workspaces remain distinct.
        for scope in [FOUNDATION, RUNTIME, Path('tools/wi-contract-check')]:
            subprocess.run(['cargo', 'fetch', '--locked', '--manifest-path', str(scope / 'Cargo.toml')], cwd=ROOT, check=True, timeout=600)
        env['CARGO_NET_OFFLINE'] = 'true'
        for name, command in gates():
            print('Running gate ' + name, flush=True)
            resolved = [x.replace('{version}', version).replace('{output}', str(output)).replace('{work}', str(work)) for x in command]
            result = run(resolved, ROOT, output / f'logs/{name}.log', env, 1800 if name in ADOPTED else 3600)
            evidence.append({'gate': name, 'command': command, 'source_commit': commit, 'result': 'passed',
                             'log': f'logs/{name}.log', 'record': f'logs/{name}.result.json', 'sha256': sha(output / f'logs/{name}.log')})
            print(f'Passed {name} ({result["elapsed_seconds"]}s)', flush=True)
            clean(commit)
        manifest['evidence'] = evidence
        manifest['packaged_locks'] = {n: r['packaged_lock_sha256'] for n, r in read(output / 'package-verification.json').items()}
        manifest['source_after'] = {'commit': commit, 'tree': git('rev-parse', 'HEAD^{tree}'), 'clean': True}
        for source, dest in [(f'docs/release/{version}.md', 'release-notes.md'), ('docs/release/package-tests.json', 'package-tests.json'), ('docs/release/release-manifest.schema.json', 'release-manifest.schema.json')]:
            shutil.copyfile(ROOT / source, output / dest)
        subprocess.run(['git', 'bundle', 'create', str(output / 'source.bundle'), 'HEAD'], cwd=ROOT, check=True, timeout=120)
    artifacts = [ref(output, p) for p in sorted(output.rglob('*')) if p.is_file()]
    # Use the existing WI preparation receipt schema and validator.
    by_path = {a['path']: a for a in artifacts}
    write(output / 'publication-preparation.json', {
        'schema_version': 1, 'kind': 'publication-preparation', 'source_revision': commit,
        'baseline_revision': manifest['baseline_revision'], 'artifacts': artifacts,
        'commands': [{'argv': read(output / e['record'])['argv'], 'exit_code': 0, 'log': by_path[e['log']]} for e in evidence],
        'toolchain': 'Rust and Cargo 1.89.0', 'environment_sha256': sha(output / 'build-environment.json'),
        'verification_receipts': [by_path[e['record']] for e in evidence], 'publication_status': 'not-performed', 'owner': 'operator'})
    run(publication_command(output / 'publication-preparation.json'), ROOT, output / 'logs/publication-validation.log', timeout=120)
    clean(commit)
    manifest['artifacts'] = [ref(output, p) for p in sorted(output.rglob('*')) if p.is_file()]
    path = output / 'release-manifest.json'
    try:
        write(path, manifest)
        (output / 'SHA256SUMS').write_text(sums(output, {a['path'] for a in manifest['artifacts']} | {'release-manifest.json'}))
        verify(path)
    except BaseException:
        path.unlink(missing_ok=True)
        (output / 'SHA256SUMS').unlink(missing_ok=True)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('prepare')
    p.add_argument('--version', required=True); p.add_argument('--commit', required=True)
    p.add_argument('--output', type=Path, required=True)
    p = sub.add_parser('verify'); p.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'prepare': prepare(args.version, args.commit, args.output)
        else: verify(args.manifest)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print('Release refused: ' + str(error), file=sys.stderr)
        sys.exit(1)
