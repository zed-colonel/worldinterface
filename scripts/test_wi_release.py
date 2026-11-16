"""Non-live release regressions, including real Cargo local-registry resolution."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parent))
import release
import wi_release as shared
import wi_release_packages as packages
from wi_crash_isolation import check_artifacts

VERSION = '0.2.0-alpha.2'


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='wi-release-regression-')
        self.base = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_safe_paths_reject_traversal_aliases_and_parent_symlinks(self):
        (self.base / 'good').write_text('safe')
        (self.base / 'alias').symlink_to(self.base, target_is_directory=True)
        self.assertEqual(shared.safe(self.base, 'good').read_text(), 'safe')
        for name in ['../good', '/good', './good', 'a//b', 'a/../good', 'alias/good', 'good/', 'a\\b']:
            with self.subTest(name=name), self.assertRaises(ValueError): shared.safe(self.base, name)

    def test_clean_requires_exact_commit_and_no_hidden_or_dirty_state(self):
        head = 'a' * 40
        def git(*args):
            return {'rev-parse': head, 'status': '', 'ls-files': 'H file'}[args[0]]
        with patch.object(shared, 'git', side_effect=git):
            shared.clean(head)
            for commit in ['a' * 7, 'b' * 40]:
                with self.assertRaises(ValueError): shared.clean(commit)
        for status, flags in [(' M file', 'H file'), ('?? extra', 'H file'), ('', 'h file'), ('', 'S file')]:
            def dirty(*args): return {'rev-parse': head, 'status': status, 'ls-files': flags}[args[0]]
            with patch.object(shared, 'git', side_effect=dirty), self.assertRaises(ValueError): shared.clean(head)

    def test_preflight_refuses_internal_nonempty_or_symlink_output(self):
        internal = shared.ROOT / 'release-output'
        (self.base / 'file').write_text('occupied')
        alias = self.base / 'alias'; alias.symlink_to(self.base, target_is_directory=True)
        with patch.object(release, 'clean'):
            for output in [internal, self.base, alias]:
                with self.subTest(output=output), self.assertRaises(ValueError): release.preflight(VERSION, 'a' * 40, output)

    def test_prepare_refuses_missing_or_empty_tmpdir_before_side_effects(self):
        message = 'TMPDIR must name an existing writable directory outside the checkout; create and export one before prepare.'
        for value in [None, '']:
            env = dict(os.environ)
            env.pop('TMPDIR', None)
            if value is not None: env['TMPDIR'] = value
            output = self.base / 'not-created'
            with self.subTest(value=value), patch.dict(os.environ, env, clear=True), \
                    patch.object(release, 'clean') as clean, \
                    patch.object(release, 'toolchain') as toolchain, \
                    patch.object(release, 'metadata') as metadata, \
                    patch.object(release.subprocess, 'run') as run, \
                    patch.object(release, 'gates') as gates:
                with self.assertRaisesRegex(ValueError, 'TMPDIR must name'):
                    release.prepare(VERSION, 'a' * 40, output)
                for action in [clean, toolchain, metadata, run, gates]: action.assert_not_called()
            result = subprocess.run([sys.executable, '-B', str(shared.ROOT / 'scripts/release.py'),
                                     'prepare', '--version', VERSION, '--commit', 'a' * 40,
                                     '--output', str(output)], env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stderr.strip(), 'Release refused: ' + message)
            self.assertFalse(output.exists())

    def test_preflight_refuses_unusable_or_internal_tmpdir(self):
        file = self.base / 'file'; file.write_text('not a directory')
        for directory in [self.base / 'missing', file, shared.ROOT]:
            with self.subTest(directory=directory), patch.dict(os.environ, TMPDIR=str(directory)), \
                    self.assertRaisesRegex(ValueError, 'TMPDIR must name'):
                release.preflight(VERSION, 'a' * 40, self.base / 'output')
        with patch.dict(os.environ, TMPDIR=str(self.base)), patch.object(os, 'access', return_value=False), \
                self.assertRaisesRegex(ValueError, 'TMPDIR must name'):
            release.preflight(VERSION, 'a' * 40, self.base / 'output')

    def test_preflight_rejects_wrong_toolchain(self):
        for cargo, rustc in [('cargo 1.90.0 (fixture)', 'release: 1.89.0\nhost: x86_64-unknown-linux-gnu\n'), ('cargo 1.89.0 (fixture)', 'release: 1.90.0\nhost: x86_64-unknown-linux-gnu\n')]:
            with patch.object(shared.subprocess, 'check_output', side_effect=[rustc, cargo]), self.assertRaises(ValueError): shared.toolchain()

    def test_external_staging_uses_release_toolchain_instead_of_machine_default(self):
        with patch.dict(os.environ, {'RUSTUP_TOOLCHAIN': 'stable'}):
            env = packages.environment(self.base)
        self.assertEqual(env['RUSTUP_TOOLCHAIN'], '1.89.0')
        for program in ['rustc', 'cargo']:
            result = subprocess.check_output([program, '--version'], cwd=self.base,
                                             env=env, text=True, timeout=30)
            self.assertTrue(result.startswith(program + ' 1.89.0 '), result)
        self.assertEqual(shared.toolchain()['build_environment']['RUSTUP_TOOLCHAIN'], '1.89.0')

    def test_worker_sysroot_remapping_uses_recorded_toolchain_without_local_probes(self):
        env = packages.environment(self.base)
        roots = [Path(env['CARGO_HOME']) / 'registry/src/recorded-bucket']
        commit = 'a' * 40
        # These producer paths need not exist on the verification machine. The
        # rust-src mapping is required even when the component was not installed.
        build = {'sysroot': str(self.base / 'rustup with spaces/toolchain'),
                 'rustc': f'commit-hash: {commit}\n', 'rust_src_installed': False}
        with patch.object(shared.subprocess, 'check_output', side_effect=AssertionError('local toolchain probe')), patch.object(Path, 'resolve', side_effect=AssertionError('local path resolution')):
            result = shared.worker_environment(env, self.base, roots, build)
        flag = f"--remap-path-prefix={build['sysroot']}/lib/rustlib/src/rust=/rustc/{commit}"
        self.assertEqual(result['CARGO_ENCODED_RUSTFLAGS'].split('\x1f')[-1], flag)
        self.assertEqual(shared.worker_recipe(VERSION, 'x86_64-unknown-linux-gnu')['path_remapping'][-1],
                         ['{sysroot}/lib/rustlib/src/rust', '/rustc/{rustc-commit-hash}'])
        for invalid in [dict(build, sysroot='relative'), dict(build, rustc='commit-hash: unknown\n')]:
            with self.assertRaises(ValueError):
                shared.worker_environment(env, self.base, roots, invalid)

    def test_worker_path_check_rejects_each_recorded_producer_directory(self):
        work = self.base / 'build'
        build = {'cargo_home': str(self.base / 'cargo-home'),
                 'target_directory': str(self.base / 'target'),
                 'sysroot': str(self.base / 'toolchain')}
        binary = self.base / 'worker'
        binary.write_bytes(b'ELF\x00/wi/registry/dependency/src/lib.rs\x00/rustc/commit/library/std')
        shared.check_worker_paths(binary, work, build)
        for source in [str(work), *build.values()]:
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, 'leaks producer path'):
                binary.write_bytes(b'ELF\x00' + os.fsencode(source) + b'/src/lib.rs\x00')
                shared.check_worker_paths(binary, work, build)

    def test_gate_temporary_directories_are_canonical_before_live_fixtures(self):
        physical = self.base / 'physical'; physical.mkdir()
        logical = self.base / 'logical temporary directory'
        logical.symlink_to(physical, target_is_directory=True)
        env = {key: str(logical) for key in ['TMPDIR', 'TMP', 'TEMP']}
        env['CARGO_HOME'] = str(self.base / 'cargo-home')
        with patch.dict(os.environ, env):
            with patch.object(release, 'clean'), patch.object(release, 'toolchain', return_value={}), \
                    patch.object(release, 'metadata', return_value={}):
                output, _, _ = release.preflight(VERSION, 'a' * 40, self.base / 'output')
                self.assertEqual(output, (self.base / 'output').resolve())
            env = release.gate_environment()
            self.assertEqual(os.environ['TMPDIR'], str(logical))
            self.assertEqual(env['CARGO_TARGET_DIR'], str(shared.external_target()))
        for key in ['TMPDIR', 'TMP', 'TEMP']:
            self.assertEqual(env[key], str(physical.resolve()))
        # Creation through the gate environment must have no symlink component;
        # runtime stores enforce this independently for every fixture.
        result = subprocess.check_output([sys.executable, '-c',
            'import pathlib,tempfile; p=pathlib.Path(tempfile.mkdtemp()); print(p); print(p.resolve()); p.rmdir()'],
            env=env, text=True, timeout=30).splitlines()
        self.assertEqual(result[0], result[1])

    def test_compiler_overrides_are_cleared_and_gate_environment_is_separate(self):
        overrides = {key: 'caller-override' for key in [
            'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CC', 'CFLAGS', 'RUSTC_WRAPPER',
            'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_RUSTFLAGS',
            'CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS',
            'CARGO_PROFILE_RELEASE_LTO', 'CC_x86_64_unknown_linux_gnu']}
        with patch.dict(os.environ, overrides):
            gate = release.gate_environment()
            package = packages.environment(self.base)
            for key in overrides:
                self.assertNotIn(key, gate)
                self.assertNotIn(key, package)
            recorded = shared.toolchain(gate)
            self.assertEqual(recorded['build_environment']['RUSTFLAGS'], '')
            roots = [Path(package['CARGO_HOME']) / 'registry/src/fixture']
            worker = shared.worker_environment(package, self.base.resolve(), roots, recorded)
            installed = shared.toolchain(worker)
        shared.write(self.base / 'gate-environment.json', recorded)
        shared.write(self.base / 'build-environment.json', installed)
        self.assertEqual(shared.read(self.base / 'gate-environment.json'), recorded)
        self.assertNotEqual(recorded['cargo_home'], installed['cargo_home'])
        self.assertEqual(recorded['build_environment']['CARGO_ENCODED_RUSTFLAGS'], '')
        self.assertIn('--remap-path-prefix=', installed['build_environment']['CARGO_ENCODED_RUSTFLAGS'])

    def test_prepare_refuses_cargo_home_overrides_before_gates(self):
        home = self.base / 'cargo-home'; home.mkdir()
        config = home / 'config.toml'
        output = self.base / 'output'
        find_configs = shared.cargo_config_paths
        def repository_first(directories, homes):
            return sorted(find_configs(directories, homes),
                          key=lambda p: p != shared.ROOT / '.cargo/config.toml')
        with patch.dict(os.environ, CARGO_HOME=str(home)), patch.object(release, 'clean'), \
                patch.object(shared, 'cargo_config_paths', side_effect=repository_first), \
                patch.object(release, 'toolchain', side_effect=AssertionError('toolchain before config refusal')) as toolchain, \
                patch.object(release, 'gates') as gates, patch.object(release, 'run') as run:
            for body in ['[build]\nrustflags=["--cfg", "ambient_override"]\n',
                         'paths=["local-dependency"]\n']:
                with self.subTest(body=body):
                    config.write_text(body)
                    with self.assertRaisesRegex(ValueError, '(compiler|source) overrides in ambient Cargo config: ' + str(config)):
                        release.prepare(VERSION, 'a' * 40, output)
            for action in [toolchain, gates, run]: action.assert_not_called()
        self.assertFalse(output.exists())

    def test_cargo_config_search_covers_both_names_homes_and_ancestors(self):
        source = self.base / 'clone'; source.mkdir()
        work = self.base / 'staging/work'; work.mkdir(parents=True)
        home = self.base / 'cargo-home'; home.mkdir()
        env = dict(os.environ, CARGO_HOME=str(home))
        bodies = ['[build]\n' + key + '="override"\n' for key in
                  ['rustflags', 'rustdocflags', 'rustc', 'rustdoc', 'rustc-wrapper', 'rustc-workspace-wrapper', 'target']]
        bodies += ['[target.x86_64-unknown-linux-gnu]\nlinker="override"\n',
                   '[profile.release]\nopt-level=0\n',
                   '[env]\nCARGO_ENCODED_RUSTFLAGS="override"\n',
                   '[env]\nCC={value="override",force=true}\n',
                   'include="other.toml"\n',
                   'paths=["local-dependency"]\n',
                   '[patch.crates-io]\ndependency={path="local-dependency"}\n',
                   '[source.crates-io]\nreplace-with="local"\n[source.local]\ndirectory="vendor"\n']
        for parent in [home, source / '.cargo', self.base / '.cargo', work / '.cargo', work.parent / '.cargo']:
            parent.mkdir(exist_ok=True)
            for name in ['config', 'config.toml']:
                path = parent / name
                for body in bodies:
                    with self.subTest(path=path, body=body):
                        path.write_text(body)
                        with self.assertRaisesRegex(ValueError, '(compiler|source) overrides in ambient Cargo config'):
                            shared.check_cargo_configs([source, work], env)
                path.write_text('[net]\noffline=true\n')
                checked = shared.check_cargo_configs([source, work], env)
                self.assertIn({'path': str(path), 'sha256': shared.sha(path)}, checked['files'])
                path.unlink()
        checked = shared.check_cargo_configs([source, work], env)
        self.assertIn({'path': str(home / 'config.toml'), 'sha256': None}, checked['files'])
        alias = self.base / 'alias'; alias.symlink_to(work, target_is_directory=True)
        self.assertEqual(shared.check_cargo_configs([source, alias], env), checked)

    def test_package_environment_refuses_staging_overrides(self):
        for config in [self.base / '.cargo/config.toml', self.base / 'staging/cargo-home/config']:
            config.parent.mkdir(parents=True, exist_ok=True)
            for body in ['[profile.release]\nlto=false\n', 'paths=["local-dependency"]\n']:
                config.write_text(body)
                with self.subTest(config=config, body=body), self.assertRaisesRegex(ValueError, '(compiler|source) overrides in ambient Cargo config'):
                    packages.environment(self.base / 'staging')
            config.unlink()

    def test_cargo_config_evidence_binds_committed_settings_and_complete_search(self):
        work = self.base / 'work'
        env = dict(release.gate_environment(), CARGO_HOME=str(self.base / 'cargo-home'))
        checked = shared.check_cargo_configs([shared.ROOT, work], env, [work / 'cargo-home'])
        home = checked['cargo_homes'][0]
        shared.validate_cargo_config_checks(checked, str(work), home)
        for field in ['files', 'working_directories', 'cargo_homes']:
            changed = copy.deepcopy(checked); changed.pop(field)
            with self.subTest(field=field), self.assertRaises(ValueError):
                shared.validate_cargo_config_checks(changed, str(work), home)
        for change in ['omit', 'digest', 'scope']:
            changed = copy.deepcopy(checked)
            if change == 'omit': changed['files'].pop()
            if change == 'digest':
                next(f for f in changed['files'] if f['path'] == str(shared.ROOT / '.cargo/config.toml'))['sha256'] = '0' * 64
            if change == 'scope': changed['working_directories'][1] = '/other/work'
            with self.subTest(change=change), self.assertRaises(ValueError):
                shared.validate_cargo_config_checks(changed, str(work), home)
        with patch.object(shared.subprocess, 'check_output', return_value=b'uncommitted config'):
            with self.assertRaisesRegex(ValueError, 'uncommitted Cargo config'):
                shared.check_cargo_configs([shared.ROOT, work], env)

    def test_documentation_discards_pages_from_previous_preparations(self):
        target = self.base / 'target'; docs = target / 'doc'; docs.mkdir(parents=True)
        (docs / 'removed-api.html').write_text('stale')
        def document(*args):
            self.assertFalse((docs / 'removed-api.html').exists())
            docs.mkdir(exist_ok=True)
            (docs / 'current-api.html').write_text('current')
        with patch.object(packages, 'external_target', return_value=target), patch.object(packages, 'run', side_effect=document):
            packages.rustdoc(self.base, self.base / 'work')
        with tarfile.open(self.base / 'api-docs.tar.gz') as archive:
            self.assertIn('doc/current-api.html', archive.getnames())
            self.assertNotIn('doc/removed-api.html', archive.getnames())

    def test_internal_requirements_are_exact_and_substitutions_fail(self):
        data = {'dependencies': {'worldinterface-protocol': {'version': '=' + VERSION, 'path': '../protocol'}}}
        shared.dependencies(data, VERSION)
        for change in [{'version': VERSION}, {'version': '=' + VERSION, 'git': 'https://example.invalid'}, {'version': '=' + VERSION, 'registry': 'other'}]:
            with self.subTest(change=change), self.assertRaises(ValueError): shared.dependencies({'dependencies': {'worldinterface-protocol': change}}, VERSION)
        with self.assertRaises(ValueError): shared.dependencies(data, VERSION, normalized=True)
        for key in ['patch', 'replace']:
            with self.assertRaises(ValueError): shared.dependencies({key: {}}, VERSION)
        with self.assertRaises(ValueError): shared.dependencies({'dependencies': {'actionqueue-core': '=0.2.0'}}, VERSION)

    def test_metadata_rejects_blockers_versions_and_incomplete_inventories(self):
        source = self.base / 'source'; source.mkdir()
        for directory in ['crates/worldinterface-foundation', 'crates/worldinterface-runtime', 'docs/release', 'conformance/wi-fabric-2', 'examples/downstream-consumer', 'contracts']:
            shutil.copytree(shared.ROOT / directory, source / directory, dirs_exist_ok=True, ignore=shutil.ignore_patterns('target', '__pycache__'))
        with patch.object(shared, 'ROOT', source):
            shared.metadata(VERSION)
            manifest = source / shared.RUNTIME / 'Cargo.toml'; original = manifest.read_text()
            manifest.write_text(original.replace(VERSION, '0.2.0'))
            with self.assertRaises(ValueError): shared.metadata(VERSION)
            manifest.write_text(original)
            inventory = source / 'docs/release/package-tests.json'; inv = shared.read(inventory)
            for mutate in [lambda v: v['packages'].pop('worldinterface-worker'), lambda v: v['packages']['worldinterface-runtime']['tests'].pop()]:
                value = copy.deepcopy(inv); mutate(value); shared.write(inventory, value)
                with self.assertRaises(ValueError): shared.metadata(VERSION)
            shared.write(inventory, inv)
            path = source / shared.FOUNDATION / 'Cargo.lock'; original = path.read_text()
            path.write_text(original.replace('name = "worldinterface-worker"', 'name = "wrong-worker"'))
            with self.assertRaises(ValueError): shared.metadata(VERSION)
            path.write_text(original)
            shared.write(source / 'docs/release/blockers.json', {'schema_version': 1, 'blockers': ['unresolved']})
            with self.assertRaises(ValueError): shared.metadata(VERSION)

    def test_every_package_snapshot_is_checked(self):
        inventory = shared.read(shared.ROOT / 'docs/release/package-tests.json')
        for dest in inventory['byte_copies']:
            root = self.base / hashlib.sha256(dest.encode()).hexdigest()
            for name in set(inventory['byte_copies']) | set(inventory['byte_copies'].values()) | {'docs/release/package-tests.json'}:
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(shared.ROOT / name, path)
            (root / dest).write_bytes(b'drift')
            with self.subTest(dest=dest), self.assertRaises(ValueError): shared.byte_copies(root)

    def test_packaged_lock_preserves_external_versions_and_separate_repository_identity(self):
        repository = {'package': [{'name': 'external', 'version': '1.0.0', 'source': shared.CRATES_IO, 'checksum': 'a' * 64}]}
        lock = {'package': [dict(repository['package'][0]), {'name': 'worldinterface-runtime', 'version': VERSION}, {'name': 'worldinterface-protocol', 'version': VERSION, 'source': shared.CRATES_IO, 'checksum': 'b' * 64}]}
        shared.validate_lock(lock, repository, 'worldinterface-runtime', VERSION, {'worldinterface-protocol': 'b' * 64})
        for item, field, value in [(0, 'version', '2.0.0'), (0, 'checksum', 'c' * 64), (0, 'source', 'git+fixture'), (2, 'source', None), (2, 'checksum', 'a' * 64), (1, 'version', '0.2.0')]:
            changed = copy.deepcopy(lock); changed['package'][item][field] = value
            with self.assertRaises(ValueError): shared.validate_lock(changed, repository, 'worldinterface-runtime', VERSION, {'worldinterface-protocol': 'b' * 64})

    def test_existing_evidence_rechecks_consumer_aq_and_wi_lock_checksums(self):
        checksums = {n: 'b' * 64 for n in shared.PACKAGES}
        expected = shared.read(shared.ROOT / 'contracts/aq-integration-target.json')['runtime_release']['crates']
        lock = {'package': [{'name': n, 'version': VERSION, **({'source': shared.CRATES_IO, 'checksum': checksums[n]} if n != 'worldinterface-runtime' else {})} for n in shared.PACKAGES]}
        lock['package'] += [{'name': n, 'version': p['version'], 'source': shared.CRATES_IO, 'checksum': p['checksum']} for n, p in expected.items()]
        meta = {'packages': [{'id': p['name'], 'name': p['name'], 'version': p['version'], 'source': p.get('source'), 'dependencies': []} for p in lock['package']]}
        shared.check_graph(meta, lock, 'worldinterface-runtime', VERSION, checksums, inspect_manifests=False)
        for name in ['actionqueue-core', 'worldinterface-protocol']:
            changed = copy.deepcopy(lock)
            next(p for p in changed['package'] if p['name'] == name)['checksum'] = 'f' * 64
            with self.assertRaises(ValueError): shared.check_graph(meta, changed, 'worldinterface-runtime', VERSION, checksums, inspect_manifests=False)

    def test_graph_rejects_substituted_edges_and_incomplete_runtime(self):
        checksums = {n: 'b' * 64 for n in shared.PACKAGES}
        aq = shared.read(shared.ROOT / 'contracts/aq-integration-target.json')['runtime_release']['crates']
        lock = {'package': [
            {'name': n, 'version': VERSION, **({'source': shared.CRATES_IO, 'checksum': checksums[n]} if n != 'worldinterface-runtime' else {})}
            for n in shared.PACKAGES] + [
            {'name': n, 'version': p['version'], 'source': shared.CRATES_IO, 'checksum': p['checksum']}
            for n, p in aq.items()]}
        meta = {'packages': [
            {'id': p['name'], 'name': p['name'], 'version': p['version'], 'source': p.get('source'),
             'dependencies': [{'source': shared.CRATES_IO, 'path': None}]}
            for p in lock['package']]}
        def check(value):
            shared.check_graph(value, lock, 'worldinterface-runtime', VERSION, checksums, inspect_manifests=False)
        check(meta)
        for source, path in [(shared.CRATES_IO, '/override'), ('git+https://example.invalid', None), (None, None)]:
            changed = copy.deepcopy(meta)
            changed['packages'][0]['dependencies'][0].update(source=source, path=path)
            with self.subTest(source=source, path=path), self.assertRaisesRegex(ValueError, 'resolution edge'):
                check(changed)
        for name in ['worldinterface-worker', 'actionqueue-core']:
            changed = copy.deepcopy(meta)
            changed['packages'] = [p for p in changed['packages'] if p['name'] != name]
            with self.subTest(missing=name), self.assertRaisesRegex(ValueError, 'incomplete'):
                check(changed)
        changed = copy.deepcopy(meta)
        next(p for p in changed['packages'] if p['name'] == 'actionqueue-core')['version'] = '0.2.0'
        with self.assertRaisesRegex(ValueError, 'resolved AQ'):
            check(changed)

    def test_archive_paths_duplicates_symlinks_and_tampering(self):
        path = self.base / 'bad.crate'
        prefix = 'worldinterface-protocol-' + VERSION + '/'
        for name, kind in [(prefix + '../escape', tarfile.REGTYPE), (prefix + 'symlink', tarfile.SYMTYPE), ('foreign/Cargo.toml', tarfile.REGTYPE)]:
            with tarfile.open(path, 'w:gz') as archive:
                info = tarfile.TarInfo(name); info.type = kind; info.size = 1 if kind == tarfile.REGTYPE else 0
                archive.addfile(info, io.BytesIO(b'x') if info.size else None)
            with self.assertRaises(ValueError): shared.archive_files(path, 'worldinterface-protocol', VERSION)
        path.write_bytes(b'tampered')
        with self.assertRaises(ValueError): packages.add_registry(self.base, path, 'x', '1.0.0', 'a' * 64)

    def test_archive_source_cannot_be_substituted_even_with_new_archive_checksum(self):
        import re
        original = (shared.ROOT / shared.FOUNDATION / 'protocol/Cargo.toml').read_text()
        normalized = re.sub(r', path = "[^"]+"', '', original).encode()
        files = {'Cargo.toml': normalized, 'Cargo.toml.orig': normalized, 'src/lib.rs': b'foreign source'}
        with patch.object(shared, 'archive_files', return_value=files), self.assertRaisesRegex(ValueError, 'archive/source byte mismatch'):
            shared.validate_archive(self.base / 'fixture.crate', 'worldinterface-protocol', VERSION, {})
        files['src/lib.rs'] = (shared.ROOT / shared.FOUNDATION / 'protocol/src/lib.rs').read_bytes()
        files['src/.env'] = b'SECRET=must-not-ship'
        with patch.object(shared, 'archive_files', return_value=files), self.assertRaisesRegex(ValueError, 'uncommitted archive member'):
            shared.validate_archive(self.base / 'fixture.crate', 'worldinterface-protocol', VERSION, {})

    def test_schema_refuses_partial_or_failed_manifest(self):
        schema = shared.read(shared.ROOT / 'docs/release/release-manifest.schema.json')
        with self.assertRaises(ValueError): shared.validate_schema(schema, {'schema_version': 1})
        with self.assertRaises(ValueError): shared.validate_schema({'type': 'array', 'items': {'properties': {'result': {'const': 'passed'}}}}, [{'result': 'failed'}])
        with self.assertRaises(ValueError): shared.validate_schema({'properties': {'schema_version': {'const': 1}}}, {'schema_version': True})

    def test_gate_completeness_identity_source_results_and_log_integrity(self):
        shared.write(self.base / 'logs/gate.result.json', {'argv': ['fixture'], 'exit_code': 0, 'elapsed_seconds': 1, 'result': 'passed'})
        (self.base / 'logs/gate.log').write_text('passed')
        entry = {'gate': 'gate', 'command': ['fixture'], 'source_commit': 'a' * 40, 'result': 'passed', 'log': 'logs/gate.log', 'record': 'logs/gate.result.json', 'sha256': shared.sha(self.base / 'logs/gate.log')}
        manifest = {'source_commit': 'a' * 40, 'version': VERSION, 'build_work': '/external', 'preparation_output': '/original-output', 'evidence': [entry]}
        with patch.object(release, 'gates', return_value=[('gate', ['fixture'])]):
            release.validate_evidence(manifest, self.base)
            # Artifact paths stay relative while executed argv retains producer
            # paths. Verification can read a copied bundle in another directory.
            portable = copy.deepcopy(manifest)
            portable['evidence'][0]['command'] = ['fixture', '{output}']
            shared.write(self.base / 'logs/gate.result.json', {'argv': ['fixture', '/original-output'], 'exit_code': 0, 'elapsed_seconds': 1, 'result': 'passed'})
            with patch.object(release, 'gates', return_value=[('gate', ['fixture', '{output}'])]):
                release.validate_evidence(portable, self.base)
            shared.write(self.base / 'logs/gate.result.json', {'argv': ['fixture'], 'exit_code': 0, 'elapsed_seconds': 1, 'result': 'passed'})
            for field in ['gate', 'command', 'source_commit', 'result', 'sha256']:
                value = copy.deepcopy(manifest); value['evidence'][0][field] = 'wrong'
                with self.assertRaises(ValueError): release.validate_evidence(value, self.base)
            with self.assertRaises(ValueError): release.validate_evidence(dict(manifest, evidence=[]), self.base)
            shared.write(self.base / 'logs/gate.result.json', {'argv': ['fixture'], 'exit_code': 1, 'elapsed_seconds': 1, 'result': 'failed'})
            with self.assertRaises(ValueError): release.validate_evidence(manifest, self.base)

    def test_release_preserves_adopted_commands_and_checks_contracts_before_campaign(self):
        expected = {gate['id']: gate['argv'] for gate in shared.read(shared.ROOT / '.craftingtable/checks.json')['checks']}
        gates = release.gates()
        self.assertEqual({name: argv for name, argv in gates if name in shared.ADOPTED}, expected)
        names = [name for name, _ in gates]
        self.assertEqual(len(names), len(set(names)))
        self.assertLess(names.index('contract-check'), names.index('runtime-test'))
        self.assertLess(names.index('contract-test'), names.index('runtime-test'))

    def test_receipt_manifest_disagreement_fails_before_authority_invocation(self):
        shared.write(self.base / 'publication-preparation.json', {'source_revision': 'b' * 40})
        with patch.object(release.subprocess, 'run') as authority:
            with self.assertRaises(ValueError): release.validate_receipt({'source_commit': 'a' * 40, 'baseline_revision': 'c' * 40}, self.base)
            authority.assert_not_called()

    def synthetic_bundle(self):
        """Opaque package bytes; real manifest, inventory, gates and receipt checks.

        Package internals/registry builds have separate real-Cargo coverage below.
        Only external Git/Cargo commands and package-content validation are stubbed.
        """
        commit = 'a' * 40
        meta = shared.metadata(VERSION)
        manifest = dict(meta, source_commit=commit, baseline_revision=shared.BASELINE,
                        source_before={'commit': commit, 'tree': 'b' * 40, 'clean': True},
                        source_after={'commit': commit, 'tree': 'b' * 40, 'clean': True},
                        build_work='/external/work', preparation_output=str(self.base),
                        packaged_locks={}, evidence=[])
        files = ['source.bundle', 'package-verification.json', 'consumer-resolution.json',
                 'consumer-Cargo.lock', 'consumer-handoff.json', 'api-docs.tar.gz',
                 'workers/wi-process-fixture', 'workers/wi-process-reaper']
        files += [f'crates/{name}-{VERSION}.crate' for name in shared.PACKAGES]
        for name in files:
            path = self.base / name; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'synthetic artifact')
        for source, dest in [(f'docs/release/{VERSION}.md', 'release-notes.md'),
                             ('docs/release/package-tests.json', 'package-tests.json'),
                             ('docs/release/release-manifest.schema.json', 'release-manifest.schema.json')]:
            shutil.copyfile(shared.ROOT / source, self.base / dest)
        # Config evidence describes this fixture, not the runner's Cargo home
        # (which can contain an offline source replacement under adopted checks).
        env = dict(release.gate_environment(), CARGO_HOME=str(self.base / 'cargo-home'))
        build = shared.toolchain(env)
        build['cargo_config_checks'] = shared.check_cargo_configs([shared.ROOT, manifest['build_work']],
            env, [Path(manifest['build_work']) / 'cargo-home'])
        shared.write(self.base / 'gate-environment.json', build)
        shared.write(self.base / 'build-environment.json', build)
        for name, command in release.gates():
            log = self.base / f'logs/{name}.log'; log.parent.mkdir(exist_ok=True)
            log.write_text('passed')
            argv = [x.replace('{output}', str(self.base)).replace('{work}', manifest['build_work']).replace('{version}', VERSION) for x in command]
            shared.write(log.with_suffix('.result.json'), {'argv': argv, 'exit_code': 0,
                         'elapsed_seconds': 1, 'result': 'passed'})
            manifest['evidence'].append({'gate': name, 'command': command, 'source_commit': commit,
                'result': 'passed', 'log': f'logs/{name}.log', 'record': f'logs/{name}.result.json',
                'sha256': shared.sha(log)})
        artifacts = [shared.ref(self.base, p) for p in sorted(self.base.rglob('*')) if p.is_file()]
        by_path = {a['path']: a for a in artifacts}
        shared.write(self.base / 'publication-preparation.json', {
            'source_revision': commit, 'baseline_revision': shared.BASELINE, 'owner': 'operator',
            'publication_status': 'not-performed', 'toolchain': 'Rust and Cargo 1.89.0',
            'environment_sha256': shared.sha(self.base / 'build-environment.json'),
            'artifacts': artifacts, 'verification_receipts': [by_path[e['record']] for e in manifest['evidence']],
            'commands': [{'argv': shared.read(self.base / e['record'])['argv'], 'exit_code': 0,
                          'log': by_path[e['log']]} for e in manifest['evidence']]})
        (self.base / 'logs/publication-validation.log').write_text('passed')
        shared.write(self.base / 'logs/publication-validation.result.json', {
            'argv': release.publication_command(self.base / 'publication-preparation.json'),
            'exit_code': 0, 'result': 'passed'})
        manifest['artifacts'] = [shared.ref(self.base, p) for p in sorted(self.base.rglob('*')) if p.is_file()]
        shared.write(self.base / 'release-manifest.json', manifest)
        (self.base / 'SHA256SUMS').write_text(release.sums(self.base,
            {a['path'] for a in manifest['artifacts']} | {'release-manifest.json'}))
        return manifest, meta

    def test_verify_refuses_tampered_artifacts_extra_missing_files_and_checksum_inventory(self):
        manifest, meta = self.synthetic_bundle()
        path = self.base / 'release-manifest.json'
        with patch.object(release, 'clean'), patch.object(release, 'metadata', return_value=meta), \
                patch.object(release, 'git', return_value=manifest['source_before']['tree']), \
                patch.object(release.subprocess, 'run'), \
                patch.object(release, 'check_cargo_configs'), \
                patch.object(release.subprocess, 'check_output', return_value=manifest['source_commit'] + ' HEAD\n'), \
                patch.object(release, 'validate_packages') as package_check, patch.object(release, 'plan'):
            release.verify(path)
            package_check.assert_called_once_with(manifest, self.base)
            # Every artifact class, including crates, binaries, docs, provenance,
            # gate logs/results, both environments and the preparation receipt.
            for artifact in manifest['artifacts']:
                target = self.base / artifact['path']; original = target.read_bytes()
                with self.subTest(tampered=artifact['path']):
                    target.write_bytes(original + b'drift')
                    with self.assertRaisesRegex(ValueError, 'artifact checksum mismatch'): release.verify(path)
                    target.write_bytes(original)
                with self.subTest(missing=artifact['path']):
                    target.unlink()
                    with self.assertRaisesRegex(ValueError, 'missing artifact'): release.verify(path)
                    target.write_bytes(original)
            extra = self.base / 'unlisted'; extra.write_text('extra')
            with self.assertRaisesRegex(ValueError, 'unlisted or missing'): release.verify(path)
            extra.unlink()
            (self.base / 'SHA256SUMS').write_text('wrong inventory\n')
            with self.assertRaisesRegex(ValueError, 'checksum inventory disagreement'): release.verify(path)
            (self.base / 'SHA256SUMS').unlink()
            with self.assertRaisesRegex(ValueError, 'unlisted or missing'): release.verify(path)

    def test_failed_final_verification_removes_both_success_markers(self):
        for error in [ValueError('final verify failed'), KeyboardInterrupt()]:
            output = self.base / type(error).__name__
            meta = {'packaged_locks': {}}
            def run(argv, cwd, log, *args, **kwargs):
                shared.write(output / 'package-verification.json', {})
                shared.write(output / 'build-environment.json', {})
                Path(log).parent.mkdir(exist_ok=True)
                Path(log).write_text('passed')
                shared.write(Path(log).with_suffix('.result.json'), {'argv': argv})
                return {'elapsed_seconds': 1}
            def fail(path):
                self.assertTrue(path.is_file())
                self.assertTrue((output / 'SHA256SUMS').is_file())
                raise error
            with patch.object(release, 'preflight', return_value=(output, {}, meta)), \
                    patch.object(release, 'check_cargo_configs', return_value={}), \
                    patch.object(release, 'git', return_value='a' * 40), \
                    patch.object(release.subprocess, 'run'), patch.object(release, 'clean'), \
                    patch.object(release, 'gates', return_value=[('fixture', ['fixture'])]), \
                    patch.object(release, 'run', side_effect=run), patch.object(release, 'verify', side_effect=fail):
                with self.assertRaises(type(error)): release.prepare(VERSION, 'a' * 40, output)
            self.assertFalse((output / 'release-manifest.json').exists())
            self.assertFalse((output / 'SHA256SUMS').exists())

    def test_consumer_handoff_requires_g5_release_and_both_store_directions(self):
        descriptor = shared.read(shared.ROOT / shared.RUNTIME / 'release/handoff.json')
        handoff = {'supervisor': {'gate': 'WI-WORKER-G5', 'available_since': VERSION},
                   'store_compatibility': copy.deepcopy(descriptor['release']['store_compatibility'])}
        release.validate_consumer_contract(handoff, descriptor)
        for section, field in [('supervisor', 'gate'), ('supervisor', 'available_since'),
                               *[('store_compatibility', key) for key in handoff['store_compatibility']]]:
            for remove in [True, False]:
                changed = copy.deepcopy(handoff)
                if remove: changed[section].pop(field)
                else: changed[section][field] = 'substituted'
                with self.subTest(section=section, field=field, remove=remove), self.assertRaises(ValueError):
                    release.validate_consumer_contract(changed, descriptor)
        for section in handoff:
            changed = copy.deepcopy(handoff); changed.pop(section)
            with self.assertRaises(ValueError): release.validate_consumer_contract(changed, descriptor)
        for field in ['previous_store_requires_recreation', 'current_store_readable_by_previous']:
            changed = copy.deepcopy(handoff)
            changed['store_compatibility'][field] = int(changed['store_compatibility'][field])
            with self.assertRaises(ValueError): release.validate_consumer_contract(changed, descriptor)

    def test_silent_commands_have_nonempty_receipt_logs_without_polluting_metadata(self):
        log = self.base / 'silent.log'
        command = [sys.executable, '-c', 'pass']
        result = shared.run(command, self.base, log, timeout=30)
        self.assertEqual(result['exit_code'], 0)
        self.assertIn('Command: ' + json.dumps(command), log.read_text())
        self.assertIn('Working directory: ' + str(self.base), log.read_text())
        output = self.base / 'metadata.json'
        shared.run([sys.executable, '-c', 'print(\'{"value": 42}\')'], self.base,
                   self.base / 'metadata.log', timeout=30, stdout_path=output)
        self.assertEqual(shared.read(output), {'value': 42})
        self.assertTrue((self.base / 'metadata.log').stat().st_size)

    def test_production_artifacts_reject_test_or_crash_feature(self):
        good = {'reason': 'compiler-artifact', 'target': {'name': 'worldinterface_storage'}, 'features': [], 'profile': {'test': False}}
        check_artifacts([good])
        for bad in [dict(good, features=['crash-points']), dict(good, profile={'test': True})]:
            with self.assertRaises(AssertionError): check_artifacts([bad])
        with self.assertRaises(AssertionError): check_artifacts([])

    def test_failed_or_interrupted_gate_never_writes_success_manifest(self):
        for error in [ValueError('failed gate'), KeyboardInterrupt()]:
            output = self.base / type(error).__name__
            with patch.object(release, 'preflight', return_value=(output, {}, {})), patch.object(release, 'check_cargo_configs', return_value={}), patch.object(release, 'git', return_value='a' * 40), patch.object(release.subprocess, 'run'), patch.object(release, 'gates', return_value=[('failure', ['fixture'])]), patch.object(release, 'run', side_effect=error):
                with self.assertRaises(type(error)): release.prepare(VERSION, 'a' * 40, output)
            self.assertFalse((output / 'release-manifest.json').exists())
            self.assertFalse((output / 'SHA256SUMS').exists())

    def test_real_cargo_archive_registry_and_consumer_without_paths(self):
        # Exercise a symlinked TMPDIR/base, including spaces, on every run.
        physical = self.base / 'physical'; physical.mkdir()
        logical = self.base / 'logical base'; logical.symlink_to(physical, target_is_directory=True)
        self.base = logical
        # Keep builds in the assigned cache, but reach it through a target alias.
        target_alias = self.base / 'target alias'
        target_alias.symlink_to(shared.external_target(), target_is_directory=True)
        target_env = patch.dict(os.environ, {'CARGO_TARGET_DIR': str(target_alias)})
        target_env.start(); self.addCleanup(target_env.stop)
        registry = self.base / 'registry'; (registry / 'index').mkdir(parents=True)
        source = self.base / 'source'; (source / 'src').mkdir(parents=True)
        (source / 'Cargo.toml').write_text('[workspace]\n[package]\nname="wi-release-test-dependency"\nversion="1.0.0"\nedition="2021"\n')
        (source / 'src/lib.rs').write_text('pub fn value() -> u32 { 42 }\npub fn location() -> &\'static str { file!() }\n#[test] fn unit() { assert_eq!(value(),42); }\n')
        # Package from a real commit while ignored sensitive/editor files are
        # present under Cargo's include globs in the caller's clean checkout.
        (source / '.gitignore').write_text('.env\n*.swp\n')
        with (source / 'Cargo.toml').open('a') as manifest:
            manifest.write('include=["src/**"]\n')
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=source, text=True, timeout=30)
        git('init', '--quiet')
        git('add', '.')
        git('-c', 'user.name=Release Test', '-c', 'user.email=release@example.invalid', 'commit', '--quiet', '-m', 'fixture')
        (source / 'src/.env').write_text('SECRET=must-not-ship')
        (source / 'src/lib.rs.swp').write_text('editor contents')
        self.assertEqual(git('status', '--porcelain'), '')
        staged = self.base / 'committed'
        with patch.object(packages, 'ROOT', source):
            packages.committed_sources(Path('.'), staged)
        self.assertFalse((staged / 'src/.env').exists())
        self.assertFalse((staged / 'src/lib.rs.swp').exists())
        target = shared.external_target()
        def cargo(args, cwd, env=None):
            # The outer Rust harness bounds this entire suite at 900 seconds.
            # A nested command may wait on the shared target lock under load.
            result = subprocess.run(['cargo', *args], cwd=cwd, capture_output=True, text=True, timeout=600,
                                    env=packages.environment(self.base) if env is None else env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result
        cargo(['package', '--offline', '--no-verify', '--allow-dirty'], staged)
        archive = target / 'package/wi-release-test-dependency-1.0.0.crate'
        with tarfile.open(archive) as packed:
            self.assertFalse(any(name.endswith(('.env', '.swp')) for name in packed.getnames()))
        packages.add_registry(registry, archive, 'wi-release-test-dependency', '1.0.0', shared.sha(archive))
        extracted = self.base / 'extracted'
        with tarfile.open(archive) as packed:
            packed.extractall(extracted, filter='data')
        cargo(['test', '--lib', '--locked', '--offline'], extracted / 'wi-release-test-dependency-1.0.0')
        config = self.base / 'registry.toml'
        config.write_text('[source.crates-io]\nreplace-with="fixture"\n[source.fixture]\nlocal-registry=' + json.dumps(str(registry)) + '\n')
        consumer = self.base / 'consumer'; (consumer / 'src').mkdir(parents=True)
        (consumer / 'Cargo.toml').write_text('[workspace]\n[package]\nname="wi-release-test-consumer"\nversion="1.0.0"\nedition="2021"\n[dependencies]\nwi-release-test-dependency="=1.0.0"\n')
        (consumer / 'src/main.rs').write_text('fn main() { assert_eq!(wi_release_test_dependency::value(),42); println!("{}", wi_release_test_dependency::location()); std::thread::scope(|s| { s.spawn(|| 42).join().unwrap(); }); }')
        cargo(['--config', str(config), 'run', '--offline'], consumer)
        result = cargo(['--config', str(config), 'metadata', '--locked', '--offline', '--format-version', '1'], consumer)
        dep = next(p for p in json.loads(result.stdout)['packages'] if p['name'] == 'wi-release-test-dependency')
        self.assertEqual(dep['source'], shared.CRATES_IO)
        lock = shared.toml(consumer / 'Cargo.lock')
        self.assertEqual(next(p for p in lock['package'] if p['name'] == dep['name'])['checksum'], shared.sha(archive))
        # Retain exact archive identity; replacing bytes refuses registry admission.
        bad = self.base / archive.name; bad.write_bytes(archive.read_bytes() + b'drift')
        with self.assertRaises(ValueError): packages.add_registry(registry, bad, dep['name'], '1.0.0', shared.sha(archive))

        # Install the same archive from two different registries AND Cargo homes.
        # The dependency's file!() embeds its absolute registry path without the
        # remapping. Hash equality checks the executable, beyond mocked argv.
        cargo(['--config', str(config), 'package', '--offline', '--no-verify', '--allow-dirty'], consumer)
        worker = target / 'package/wi-release-test-consumer-1.0.0.crate'
        packages.add_registry(registry, worker, 'wi-release-test-consumer', '1.0.0', shared.sha(worker))
        digests = []; locations = []
        shared_home = self.base / 'shared-cargo-home'; shared_home.mkdir()
        for route in ['first-location', 'different-location']:
            staging = self.base / route
            staging.mkdir()
            home = self.base / (route + '-home'); home.mkdir()
            (staging / 'cargo-home').symlink_to(home.resolve(), target_is_directory=True)
            (staging / 'target').symlink_to(target_alias.resolve(), target_is_directory=True)
            with patch.dict(os.environ, {'CARGO_HOME': str(shared_home), 'RUSTFLAGS': '--cfg unwanted'}):
                env = packages.environment(staging)
            self.assertEqual(Path(env['CARGO_HOME']), home.resolve())
            self.assertEqual(Path(env['CARGO_TARGET_DIR']), target_alias.resolve())
            # Execute the documented canonical exports and remapping snippet,
            # then use its actual flags for one of the real install routes.
            notes = (shared.ROOT / 'docs/release/0.2.0-alpha.2.md').read_text()
            exports = notes.split('```sh\n', 1)[1].split('export RUSTUP_TOOLCHAIN', 1)[0]
            script = exports + "python3 -c 'import json,os; print(json.dumps({k: os.environ[k] for k in [\"WI_WORKER_BUILD_ROOT\", \"CARGO_HOME\", \"CARGO_TARGET_DIR\"]}))'"
            documented = json.loads(subprocess.check_output(['bash', '-c', script], cwd=staging, text=True, timeout=30))
            self.assertEqual(documented, {'WI_WORKER_BUILD_ROOT': str(staging.resolve()),
                                         'CARGO_HOME': env['CARGO_HOME'], 'CARGO_TARGET_DIR': env['CARGO_TARGET_DIR']})
            local = staging / 'registry'; shutil.copytree(registry, local)
            cfg = staging / 'registry.toml'
            cfg.write_text(config.read_text().replace(str(registry), str(local)))
            cargo(['--config', str(cfg), 'metadata', '--offline', '--format-version', '1'], consumer, env)
            roots = list((Path(env['CARGO_HOME']) / 'registry/src').iterdir())
            locations.extend(roots)
            build = shared.toolchain(env)
            env = shared.worker_environment(env, staging.resolve(), roots, build)
            snippet = notes.split("export CARGO_ENCODED_RUSTFLAGS=\"$(python3 - <<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
            documented_flags = subprocess.check_output([sys.executable, '-c', snippet], env=dict(env, **documented), text=True, timeout=30).rstrip('\n')
            self.assertEqual(documented_flags, env['CARGO_ENCODED_RUSTFLAGS'])
            if route == 'different-location': env['CARGO_ENCODED_RUSTFLAGS'] = documented_flags
            self.assertNotIn('RUSTFLAGS', env)
            cargo(['--config', str(cfg), 'install', '--offline', '--locked', '--version', '=1.0.0',
                   '--target', 'x86_64-unknown-linux-gnu', '--root', str(staging / 'install'),
                   'wi-release-test-consumer'], staging, env)
            binary = staging / 'install/bin/wi-release-test-consumer'
            output = subprocess.check_output([binary], text=True, timeout=30)
            self.assertIn('/wi/registry/wi-release-test-dependency-1.0.0/src/lib.rs', output)
            self.assertNotIn(str(self.base), output)
            # std panic locations must use rustc's canonical compiler source
            # prefix even when rust-src is installed at a machine-local path.
            binary_bytes = binary.read_bytes()
            shared.check_worker_paths(binary, staging.resolve(), shared.toolchain(env))
            for source in [self.base, self.base.resolve(), staging / 'cargo-home', target_alias]:
                self.assertNotIn(os.fsencode(source), binary_bytes)
            self.assertFalse(build['sysroot'].encode() in binary_bytes, 'worker binary leaks the local sysroot path')
            commit = next(line.split(': ', 1)[1] for line in build['rustc'].splitlines() if line.startswith('commit-hash: '))
            self.assertTrue(f'/rustc/{commit}/library/'.encode() in binary_bytes, 'worker binary lacks the canonical compiler source prefix')
            digests.append(shared.sha(binary))
            recorded = shared.toolchain(env)
            self.assertEqual(recorded['sysroot'], build['sysroot'])
            self.assertEqual(recorded['rust_src_installed'], (Path(recorded['sysroot']) / 'lib/rustlib/src/rust/library').is_dir())
            self.assertEqual(recorded['cargo_home'], env['CARGO_HOME'])
            self.assertEqual(recorded['build_environment']['CARGO_ENCODED_RUSTFLAGS'], env['CARGO_ENCODED_RUSTFLAGS'])
        self.assertNotEqual(locations[0].name, locations[1].name)
        self.assertEqual(digests[0], digests[1])
        self.assertEqual(list(shared_home.iterdir()), [])


if __name__ == '__main__': unittest.main()
