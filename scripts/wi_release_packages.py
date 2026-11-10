"""Archive-backed Cargo local registry and exact package verification."""
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib
import urllib.request

from wi_release import (ROOT, FOUNDATION, RUNTIME, PACKAGES, CRATES_IO, TOOLCHAIN, archive_files,
                        check_graph, external_target, metadata, read, require, run, sha,
                        toml, toolchain, validate_archive, write)
from wi_crash_isolation import check_artifacts


def index_path(name):
    if len(name) == 1: return '1/' + name
    if len(name) == 2: return '2/' + name
    if len(name) == 3: return '3/' + name[0] + '/' + name
    return name[:2] + '/' + name[2:4] + '/' + name


def index_record(data, checksum):
    deps = []
    for target, scope in [(None, data), *data.get('target', {}).items()]:
        for table, kind in [('dependencies', 'normal'), ('dev-dependencies', 'dev'), ('build-dependencies', 'build')]:
            for alias, dep in scope.get(table, {}).items():
                if isinstance(dep, str): dep = {'version': dep}
                require('version' in dep and not any(k in dep for k in ['path', 'git', 'registry', 'registry-index']), 'non-registry archive requirement')
                deps.append({'name': alias, 'req': dep['version'], 'features': dep.get('features', []),
                             'optional': dep.get('optional', False), 'default_features': dep.get('default-features', True),
                             'target': target, 'kind': kind, 'package': dep.get('package'), 'registry': None})
    p = data['package']
    return {'name': p['name'], 'vers': p['version'], 'deps': deps, 'cksum': checksum,
            'features': {}, 'features2': data.get('features', {}), 'v': 2, 'yanked': False,
            'links': p.get('links'), 'rust_version': p.get('rust-version')}


def add_registry(registry, archive, name, version, expected):
    require(sha(archive) == expected, 'registry archive checksum mismatch')
    files = archive_files_external(archive, name, version)
    data = tomllib.loads(files['Cargo.toml'].decode())
    require(data['package']['name'] == name and data['package']['version'] == version, 'registry archive identity mismatch')
    dest = registry / archive.name
    if archive != dest: shutil.copyfile(archive, dest)
    path = registry / 'index' / index_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
    require(not any(x['vers'] == version for x in records), 'duplicate registry version')
    records.append(index_record(data, expected))
    path.write_text(''.join(json.dumps(row, separators=(',', ':')) + '\n' for row in records))


def archive_files_external(path, name, version):
    # External crates may have no library, lock or README; enforce member safety
    # and normalized manifest identity without applying WI's contents contract.
    prefix = f'{name}-{version}/'
    result = {}
    with tarfile.open(path, 'r:gz') as archive:
        require(sum(m.size for m in archive.getmembers()) < 256 * 1024 * 1024, 'oversized dependency')
        for m in archive.getmembers():
            require(m.name.startswith(prefix), 'foreign dependency archive root')
            rel = m.name[len(prefix):]
            require(rel and '\\' not in rel and all(x not in ['', '.', '..'] for x in rel.split('/')), 'unsafe dependency member')
            require(m.isfile() and rel not in result, 'special/duplicate dependency member')
            # Only the manifest is needed to produce an index; Cargo unpacks the
            # checksum-verified archive using its own source implementation.
            if rel == 'Cargo.toml': result[rel] = archive.extractfile(m).read()
    require('Cargo.toml' in result, 'missing dependency manifest')
    return result


def seed_registry(work):
    registry = work / 'registry'; (registry / 'index').mkdir(parents=True)
    write(registry / 'index/config.json', {'dl': 'https://static.crates.io/crates', 'api': 'https://crates.io'})
    locked = {}
    for scope in [FOUNDATION, RUNTIME]:
        for p in toml(ROOT / scope / 'Cargo.lock')['package']:
            if p.get('source'):
                key = (p['name'], p['version'])
                require(p['source'] == CRATES_IO, 'foreign registry lock')
                require(key not in locked or locked[key] == p['checksum'], 'lock checksum disagreement')
                locked[key] = p['checksum']
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo'))
    for (name, version), digest in sorted(locked.items()):
        filename = f'{name}-{version}.crate'
        candidates = list((cargo_home / 'registry/cache').glob('index.crates.io-*/' + filename))
        if candidates:
            path = candidates[0]
        else:
            path = work / filename
            with urllib.request.urlopen(f'https://static.crates.io/crates/{name}/{filename}', timeout=120) as response:
                path.write_bytes(response.read(64 * 1024 * 1024 + 1))
        add_registry(registry, path, name, version, digest)
    config = work / 'registry.toml'
    config.write_text('[source.crates-io]\nreplace-with = "release-archives"\n[source.release-archives]\nlocal-registry = ' + json.dumps(str(registry)) + '\n')
    return registry, config


def cargo(config):
    return ['cargo', '--config', str(config)]


def environment(work):
    # Source replacement unpacks into CARGO_HOME, even for an offline local
    # registry. Keep those per-registry sources with the disposable staging.
    home = work / 'cargo-home'
    home.mkdir(parents=True, exist_ok=True)
    return dict(os.environ, CARGO_HOME=str(home),
                CARGO_TARGET_DIR=str(external_target()), CARGO_NET_OFFLINE='true',
                RUSTUP_TOOLCHAIN=TOOLCHAIN,
                CARGO_BUILD_JOBS=os.environ.get('CARGO_BUILD_JOBS', '8'), RUST_TEST_THREADS='4',
                PROPTEST_DISABLE_FAILURE_PERSISTENCE='1')


def prepare_archives(output, work, version):
    metadata(version)
    work.mkdir(parents=True, exist_ok=True)
    registry, config = seed_registry(work)
    base = cargo(config); env = environment(work)
    archives = output / 'crates'; archives.mkdir()
    checksums = {}
    # No seed WI package is used: each dependency is a real finalized archive
    # before Cargo may resolve its dependent's normalized package lockfile.
    for name, directory in PACKAGES.items():
        source = work / 'sources' / name
        shutil.copytree(ROOT / directory, source, ignore=shutil.ignore_patterns('target', 'dev-output'))
        shutil.copyfile(ROOT / (RUNTIME if name == 'worldinterface-runtime' else FOUNDATION) / 'Cargo.lock', source / 'Cargo.lock')
        manifest_path = source / 'Cargo.toml'
        manifest_path.write_text(re.sub(r', path = "[^"]+"', '', manifest_path.read_text()))
        run(base + ['package', '--offline', '--allow-dirty', '--no-verify'], source,
            output / f'logs/package-{name}.log', env, 600)
        archive = archives / f'{name}-{version}.crate'
        shutil.copyfile(external_target() / 'package' / archive.name, archive)
        checksums[name] = sha(archive)
        add_registry(registry, archive, name, version, checksums[name])
    return checksums


def build_packages(output, work, version):
    checksums = prepare_archives(output, work, version)
    base = cargo(work / 'registry.toml'); env = environment(work)
    archives = output / 'crates'; report = {}
    inventory = read(ROOT / 'docs/release/package-tests.json')
    for name in PACKAGES:
        archive = archives / f'{name}-{version}.crate'
        files = validate_archive(archive, name, version, checksums)
        folder = work / 'extracted' / name; folder.mkdir(parents=True)
        for rel, content in files.items():
            dest = folder / rel; dest.parent.mkdir(parents=True, exist_ok=True); dest.write_bytes(content)
        # Preserve executable source assets if a future package needs them. All
        # current helpers are compiled from Rust or C, never executed as scripts.
        run(base + ['metadata', '--locked', '--offline', '--format-version', '1'], folder,
            output / f'logs/metadata-{name}.log', env, 180, stdout_path=output / f'resolutions/{name}.json')
        meta = read(output / f'resolutions/{name}.json')
        check_graph(meta, toml(folder / 'Cargo.lock'), name, version, checksums)
        write(output / f'resolutions/{name}.json', meta)
        run(base + ['build', '--locked', '--offline', '--message-format=json'], folder,
            output / f'logs/production-{name}.log', env, 900)
        # Cargo emits diagnostics on stderr too: extract only JSON records.
        records = []
        for line in (output / f'logs/production-{name}.log').read_text().splitlines():
            if line.startswith('{'):
                records.append(json.loads(line))
        if name in ['worldinterface-storage', 'worldinterface-ingress', 'worldinterface-egress', 'worldinterface-runtime']:
            check_artifacts(records)
        write(output / f'production/{name}.json', records)
        commands = [['test', '--lib'], *[['test', '--test', r['target']] for r in inventory['packages'][name]['tests'] if r['disposition'] == 'packaged'], ['test', '--doc']]
        for index, command in enumerate(commands):
            run(base + command + ['--locked', '--offline'], folder, output / f'logs/test-{name}-{index}.log', env, 900)
        report[name] = {'archive_sha256': checksums[name], 'packaged_lock_sha256': sha(folder / 'Cargo.lock'),
                        'repository_lock_sha256': sha(ROOT / (RUNTIME if name == 'worldinterface-runtime' else FOUNDATION) / 'Cargo.lock'),
                        'test_commands': commands, 'result': 'passed'}
    write(output / 'package-verification.json', report)


def rustdoc(output, work):
    env = environment(work); env['RUSTDOCFLAGS'] = '-D warnings'
    base = cargo(work / 'registry.toml')
    # Cargo does not remove pages for deleted items from a reused target cache.
    docs = external_target() / 'doc'
    if docs.exists(): shutil.rmtree(docs)
    for name in PACKAGES:
        run(base + ['doc', '--no-deps', '--locked', '--offline'], work / 'extracted' / name,
            output / f'logs/rustdoc-{name}.log', env, 600)
    # Capture the completed documentation before any later use of the cache.
    with tarfile.open(output / 'api-docs.tar.gz', 'w:gz') as archive:
        archive.add(docs, arcname='doc')
