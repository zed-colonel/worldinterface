"""Shared release invariants. No publication or credential-file access."""
import hashlib
from contextlib import ExitStack
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[1]
FOUNDATION = Path('crates/worldinterface-foundation')
RUNTIME = Path('crates/worldinterface-runtime')
ORDER = ['protocol', 'capability', 'domain', 'storage', 'worker', 'ingress', 'egress', 'runtime']
PACKAGES = {'worldinterface-' + p: (RUNTIME if p == 'runtime' else FOUNDATION / p) for p in ORDER}
CRATES_IO = 'registry+https://github.com/rust-lang/crates.io-index'
BASELINE = '928b5ef07797762bc002920af05a01aa4a4b0e0c'
ADOPTED = ['foundation-fmt', 'foundation-clippy', 'foundation-test', 'foundation-isolation',
           'runtime-fmt', 'runtime-clippy', 'runtime-test', 'runtime-aq-graph', 'contract-check', 'contract-test']


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result


def read(path):
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def write(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def toml(path):
    return tomllib.loads(Path(path).read_text())


def safe(base, name):
    require(isinstance(name, str) and name and '\\' not in name, 'unsafe artifact path')
    p = PurePosixPath(name)
    require(not p.is_absolute() and str(p) == name and all(x not in ('', '.', '..') for x in name.split('/')), 'unsafe artifact path: ' + name)
    current = Path(base)
    for part in p.parts:
        current = current / part
        require(not current.is_symlink(), 'symlink artifact: ' + name)
    require(current.is_file(), 'missing artifact: ' + name)
    return current


def ref(base, path):
    return {'path': str(Path(path).relative_to(base)), 'sha256': sha(path)}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, timeout=60).decode().strip()


def clean(commit):
    require(re.fullmatch('[a-f0-9]{40}', commit or ''), 'commit must be a full SHA')
    require(git('rev-parse', 'HEAD') == commit, 'different source checkout')
    require(not git('status', '--porcelain', '--untracked-files=all'), 'dirty source checkout')
    # Ignore-index bits must not hide modified tracked bytes.
    require(not git('ls-files', '-v').splitlines() or all(x.startswith('H ') for x in git('ls-files', '-v').splitlines()), 'index flags hide source state')


def toolchain():
    rust = subprocess.check_output([os.environ.get('RUSTC', 'rustc'), '-vV'], text=True, timeout=30)
    cargo = subprocess.check_output(['cargo', '--version'], text=True, timeout=30).strip()
    require('release: 1.89.0\n' in rust and cargo.startswith('cargo 1.89.0 '), 'release requires rustc and Cargo 1.89.0')
    host = re.search(r'^host: (.+)$', rust, re.M)[1]
    require(host == 'x86_64-unknown-linux-gnu', 'worker proof release requires Linux x86_64 GNU')
    return {'rustc': rust, 'cargo': cargo, 'target': host, 'profile': 'release',
            'cargo_home': os.environ.get('CARGO_HOME', str(Path.home() / '.cargo')),
            'target_directory': str(external_target()),
            'cc': subprocess.check_output(['cc', '--version'], text=True, timeout=30),
            'ld': subprocess.check_output(['ld', '--version'], text=True, timeout=30),
            'platform': os.uname()._asdict() if hasattr(os.uname(), '_asdict') else list(os.uname()),
            'build_environment': {k: os.environ.get(k, '8' if k == 'CARGO_BUILD_JOBS' else '') for k in ['RUSTFLAGS', 'RUSTDOCFLAGS', 'CC', 'CFLAGS', 'CARGO_BUILD_JOBS', 'SOURCE_DATE_EPOCH', 'RUSTC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER', 'CARGO_PROFILE_RELEASE_OPT_LEVEL', 'CARGO_PROFILE_RELEASE_LTO', 'CARGO_PROFILE_RELEASE_CODEGEN_UNITS']}}


def external_target():
    target = Path(os.environ.get('CARGO_TARGET_DIR', '/tmp/wi-release-target')).resolve()
    require(not target.is_relative_to(ROOT.resolve()), 'CARGO_TARGET_DIR must be outside checkout')
    return target


def dependencies(data, version, normalized=False):
    require(not any(k in data for k in ['patch', 'replace']), 'dependency substitution')
    for scope in [data, *data.get('target', {}).values()]:
        for kind in ['dependencies', 'dev-dependencies', 'build-dependencies']:
            for alias, dep in scope.get(kind, {}).items():
                if isinstance(dep, str):
                    dep = {'version': dep}
                name = dep.get('package', alias)
                require(not any(k in dep for k in ['git', 'registry', 'registry-index', 'workspace']), 'dependency substitution: ' + name)
                if normalized or name not in PACKAGES:
                    require('path' not in dep, 'path dependency: ' + name)
                if name in PACKAGES:
                    require(dep.get('version') == '=' + version, 'inexact internal requirement: ' + name)
                    if not normalized:
                        require('path' in dep, 'missing checkout dependency: ' + name)
                if name.startswith('actionqueue-') and (data.get('package', {}).get('name') in PACKAGES or 'package' not in data):
                    require(dep.get('version') == '=0.3.0', 'wrong AQ requirement')


def byte_copies(root=None):
    if root is None: root = ROOT
    inventory = read(root / 'docs/release/package-tests.json')
    for dest, source in inventory['byte_copies'].items():
        require(safe(root, dest).read_bytes() == safe(root, source).read_bytes(), 'stale packaged data: ' + dest)
    return inventory


def metadata(version):
    require(re.fullmatch(r'0\.2\.0-alpha\.[1-9][0-9]*', version), 'unsupported release version')
    require(read(ROOT / 'docs/release/blockers.json') == {'schema_version': 1, 'blockers': []}, 'unresolved release blockers')
    require(f'Release identity: **{version} / v{version}**.' in (ROOT / f'docs/release/{version}.md').read_text(), 'wrong release notes identity')
    inventory = byte_copies()
    require(set(toml(ROOT / FOUNDATION / 'Cargo.toml')['workspace']['members']) == set(ORDER) - {'runtime'}, 'wrong foundation workspace inventory')
    require(set(inventory['packages']) == set(PACKAGES), 'incomplete package test inventory')
    discovered = {}
    for workspace in [FOUNDATION, RUNTIME]:
        resolved = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--offline', '--no-deps', '--format-version', '1', '--manifest-path', str(ROOT / workspace / 'Cargo.toml')], cwd=ROOT, timeout=180))
        for package in resolved['packages']:
            if package['name'] in PACKAGES:
                discovered[package['name']] = package
    require(set(discovered) == set(PACKAGES), 'incomplete discovered package inventory')
    for name, directory in PACKAGES.items():
        require(Path(discovered[name]['manifest_path']) == ROOT / directory / 'Cargo.toml', 'substituted checkout package')
        data = toml(ROOT / directory / 'Cargo.toml')
        p = data['package']
        require((p['name'], p['version']) == (name, version), 'wrong package identity')
        require(p.get('publish', True) is not False, 'package cannot publish')
        require(all(p.get(k) for k in ['description', 'license', 'repository', 'readme', 'keywords', 'include']), 'missing publish metadata')
        require(p['license'] == 'Apache-2.0' and safe(ROOT / directory, p['readme']).stat().st_size, 'invalid publish metadata')
        dependencies(data, version)
        tests = inventory['packages'][name]
        require(tests['manifest'] == str(directory / 'Cargo.toml') and tests['library'] is True and tests['doctests'] is True, 'wrong package test contract')
        rows = tests['tests']
        require(len({r['target'] for r in rows}) == len(rows), 'duplicate test target')
        require({r['target'] for r in rows} == {t['name'] for t in discovered[name]['targets'] if 'test' in t['kind']}, 'unclassified test target')
        expected_owner = 'runtime-test' if name == 'worldinterface-runtime' else 'foundation-test'
        for row in rows:
            require(row['disposition'] in ['packaged', 'repository-only'] and row['owner'] == expected_owner and row['reason'], 'invalid test disposition')
        includes = p['include']
        require({x for x in includes if x.startswith('tests/') and x.endswith('.rs')} == {'tests/' + r['target'] + '.rs' for r in rows if r['disposition'] == 'packaged'}, 'package/test inventory disagreement')
    locks = {}
    for workspace in [FOUNDATION, RUNTIME]:
        path = ROOT / workspace / 'Cargo.lock'
        packages = toml(path)['package']
        expected = set(PACKAGES) - ({'worldinterface-runtime'} if workspace == FOUNDATION else set())
        wi = [p for p in packages if p['name'].startswith('worldinterface-')]
        require(len(wi) == len(expected) and {p['name'] for p in wi} == expected and all(p['version'] == version and 'source' not in p for p in wi), 'wrong repository lock inventory')
        require(all(p.get('source') == CRATES_IO and re.fullmatch('[0-9a-f]{64}', p.get('checksum', '')) for p in packages if p not in wi), 'non-registry repository dependency')
        locks[str(workspace / 'Cargo.lock')] = sha(path)
    consumer = toml(ROOT / 'examples/downstream-consumer/Cargo.toml')
    dependencies(consumer, version, normalized=True)
    require(set(consumer['dependencies']) == {'worldinterface-runtime', 'worldinterface-protocol', 'worldinterface-storage', 'serde_json', 'tempfile'}, 'wrong consumer inventory')
    return {'schema_version': 1, 'version': version, 'tag': 'v' + version,
            'crates': [{'name': n, 'version': version} for n in PACKAGES], 'blockers': [],
            'repository_locks': locks, 'aq': read(ROOT / 'contracts/aq-integration-target.json')['runtime_release'],
            'feature_profiles': {'production': 'default; crash-points absent', 'tests': 'package defaults; runtime storage dev crash-points'},
            'descriptor_sha256': sha(ROOT / RUNTIME / 'release/handoff.json'),
            'package_tests_sha256': sha(ROOT / 'docs/release/package-tests.json')}


def archive_files(path, name, version):
    prefix = name + '-' + version + '/'
    result = {}
    with tarfile.open(path, 'r:gz') as archive:
        members = archive.getmembers()
        require(sum(m.size for m in members) <= 64 * 1024 * 1024, 'oversized archive')
        for member in members:
            require(member.name.startswith(prefix), 'foreign archive root')
            rel = member.name[len(prefix):]
            require(rel and '\\' not in rel and all(p not in ['', '.', '..'] for p in rel.split('/')), 'unsafe archive path')
            require(member.isfile() and rel not in result, 'archive symlink, special or duplicate member')
            result[rel] = archive.extractfile(member).read()
    require({'Cargo.toml', 'Cargo.lock', 'src/lib.rs', 'README.md', 'LICENSE'} <= result.keys(), 'incomplete archive')
    return result


def validate_lock(lock, repository, name, version, checksums):
    allowed = {(p['name'], p['version']): p.get('checksum') for p in repository['package'] if p.get('source')}
    seen = set()
    for p in lock['package']:
        identity = (p['name'], p['version'])
        require(identity not in seen, 'duplicate lock identity'); seen.add(identity)
        if p['name'] in PACKAGES:
            require(p['version'] == version, 'wrong packaged WI lock version')
            if p['name'] == name:
                require('source' not in p, 'packaged root source')
            else:
                require(p.get('source') == CRATES_IO and p.get('checksum') == checksums[p['name']], 'substituted WI lock dependency')
        else:
            require(p.get('source') == CRATES_IO and p.get('checksum') == allowed.get(identity) and identity in allowed, 'external lock drift: ' + str(identity))


def validate_archive(path, name, version, checksums):
    files = archive_files(path, name, version)
    data = tomllib.loads(files['Cargo.toml'].decode())
    require(data['package']['name'] == name and data['package']['version'] == version, 'archive identity mismatch')
    dependencies(data, version, normalized=True)
    directory = PACKAGES[name]
    original = (ROOT / directory / 'Cargo.toml').read_text()
    require(files.get('Cargo.toml.orig') == re.sub(r', path = "[^"]+"', '', original).encode(), 'archive original manifest drift')
    for rel, content in files.items():
        if rel not in ['Cargo.toml', 'Cargo.toml.orig', 'Cargo.lock']:
            require(content == safe(ROOT / directory, rel).read_bytes(), 'archive/source byte mismatch: ' + rel)
    expected = tomllib.loads(original)
    for key in ['features', 'profile']:
        require(data.get(key, {}) == expected.get(key, {}), 'archive profile/feature drift')
    for scope in [None, *expected.get('target', {})]:
        actual_scope = data if scope is None else data.get('target', {}).get(scope, {})
        expected_scope = expected if scope is None else expected['target'][scope]
        for kind in ['dependencies', 'dev-dependencies', 'build-dependencies']:
            normalized = {alias: ({'version': dep} if isinstance(dep, str) else {k: v for k, v in dep.items() if k != 'path'}) for alias, dep in expected_scope.get(kind, {}).items()}
            require(actual_scope.get(kind, {}) == normalized, 'archive dependency declarations drift')
    require(set(data.get('target', {})) == set(expected.get('target', {})), 'archive target dependency drift')
    repository = toml(ROOT / (RUNTIME if name == 'worldinterface-runtime' else FOUNDATION) / 'Cargo.lock')
    validate_lock(tomllib.loads(files['Cargo.lock'].decode()), repository, name, version, checksums)
    inventory = byte_copies()
    expected_tests = {r['target'] for r in inventory['packages'][name]['tests'] if r['disposition'] == 'packaged'}
    require({Path(k).stem for k in files if re.fullmatch(r'tests/[^/]+\.rs', k)} == expected_tests, 'wrong packaged tests')
    require({r['name'] for r in data.get('test', [])} == expected_tests, 'normalized test targets disagree')
    for dest in inventory['byte_copies']:
        if dest.startswith(str(directory) + '/'):
            rel = str(Path(dest).relative_to(directory))
            require(files.get(rel) == safe(ROOT, dest).read_bytes(), 'archive packaged data drift: ' + rel)
    return files


def check_graph(meta, lock, root_name, version, checksums, inspect_manifests=True):
    roots = [p for p in meta['packages'] if p['name'] == root_name]
    require(len(roots) == 1, 'missing graph root')
    for p in meta['packages']:
        require(p['source'] == CRATES_IO or p['id'] == roots[0]['id'], 'non-registry resolved source: ' + p['name'])
        if inspect_manifests:
            dependencies(toml(p['manifest_path']), version, normalized=True)
        if p['name'] in PACKAGES:
            require(p['version'] == version, 'wrong graph WI version')
    aq = {p['name']: p for p in lock['package'] if p['name'].startswith('actionqueue-')}
    expected = read(ROOT / 'contracts/aq-integration-target.json')['runtime_release']['crates']
    if root_name in ['worldinterface-runtime', 'wi-downstream-consumer']:
        require(set(aq) == set(expected), 'incomplete AQ graph')
    else:
        require(not aq, 'AQ in foundation package')
    for name, p in aq.items():
        require(p.get('source') == CRATES_IO and p['version'] == expected[name]['version'] == '0.3.0' and p['checksum'] == expected[name]['checksum'], 'AQ checksum/source substitution')
    for p in lock['package']:
        if p['name'] in PACKAGES and p['name'] != root_name:
            require(p.get('source') == CRATES_IO and p.get('checksum') == checksums[p['name']], 'WI archive checksum substitution')


def run(argv, cwd, log, env=None, timeout=1800, stdout_path=None):
    start = time.monotonic()
    log = Path(log); log.parent.mkdir(parents=True, exist_ok=True)
    code = None
    try:
        with ExitStack() as stack:
            stream = stack.enter_context(log.open('wb'))
            if stdout_path is not None:
                stdout_path = Path(stdout_path)
                stdout_path.parent.mkdir(parents=True, exist_ok=True)
                stdout = stack.enter_context(stdout_path.open('wb'))
            else:
                stdout = stream
            result = subprocess.run(['timeout', '--kill-after=15s', f'{timeout}s', *[str(x) for x in argv]], cwd=cwd, env=env, stdout=stdout, stderr=stream, timeout=timeout + 20)
            code = result.returncode
    finally:
        write(log.with_suffix('.result.json'), {'argv': [str(x) for x in argv], 'exit_code': code,
              'elapsed_seconds': round(time.monotonic() - start, 3), 'result': 'passed' if code == 0 else 'failed'})
    require(code == 0, 'command failed; see ' + str(log))
    return read(log.with_suffix('.result.json'))


def validate_schema(schema, value, label='manifest'):
    """The small JSON Schema vocabulary used by the committed release schema."""
    if 'const' in schema:
        require(type(value) is type(schema['const']) and value == schema['const'], label + ': wrong constant')
    kind = schema.get('type')
    types = {'object': dict, 'array': list, 'string': str, 'integer': int}
    if kind:
        require(type(value) is types[kind], label + ': wrong type')
    if isinstance(value, dict):
        require(set(schema.get('required', [])) <= value.keys(), label + ': missing field')
        properties = schema.get('properties', {})
        if schema.get('additionalProperties') is False:
            require(value.keys() <= properties.keys(), label + ': unknown field')
        for key in value.keys() & properties.keys():
            validate_schema(properties[key], value[key], label + '/' + key)
    if isinstance(value, list):
        require(len(value) >= schema.get('minItems', 0) and len(value) <= schema.get('maxItems', len(value)), label + ': wrong length')
        for i, item in enumerate(value):
            validate_schema(schema.get('items', {}), item, label + '/' + str(i))
    if isinstance(value, str):
        require(len(value) >= schema.get('minLength', 0), label + ': short string')
        if 'pattern' in schema:
            require(re.search(schema['pattern'], value), label + ': pattern mismatch')


def worker_recipe(version, target):
    return ['cargo', '+1.89.0', 'install', '--locked', '--version', '=' + version,
            '--target', target, '--profile', 'release', '--bin', 'wi-process-fixture',
            '--bin', 'wi-process-reaper', '--root', '{install-root}', 'worldinterface-runtime']


def recipe_digest(recipe):
    return hashlib.sha256(json.dumps(recipe, separators=(',', ':')).encode()).hexdigest()
