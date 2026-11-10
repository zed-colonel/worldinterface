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

VERSION = '0.2.0-alpha.1'


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
        meta = {'packages': [{'id': p['name'], 'name': p['name'], 'version': p['version'], 'source': p.get('source')} for p in lock['package']]}
        shared.check_graph(meta, lock, 'worldinterface-runtime', VERSION, checksums, inspect_manifests=False)
        for name in ['actionqueue-core', 'worldinterface-protocol']:
            changed = copy.deepcopy(lock)
            next(p for p in changed['package'] if p['name'] == name)['checksum'] = 'f' * 64
            with self.assertRaises(ValueError): shared.check_graph(meta, changed, 'worldinterface-runtime', VERSION, checksums, inspect_manifests=False)

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
        with self.assertRaises(ValueError): release.validate_receipt({'source_commit': 'a' * 40, 'baseline_revision': 'c' * 40}, self.base)

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
            with patch.object(release, 'preflight', return_value=(output, {}, {})), patch.object(release, 'git', return_value='a' * 40), patch.object(release.subprocess, 'run'), patch.object(release, 'gates', return_value=[('failure', ['fixture'])]), patch.object(release, 'run', side_effect=error):
                with self.assertRaises(type(error)): release.prepare(VERSION, 'a' * 40, output)
            self.assertFalse((output / 'release-manifest.json').exists())
            self.assertFalse((output / 'SHA256SUMS').exists())

    def test_real_cargo_archive_registry_and_consumer_without_paths(self):
        registry = self.base / 'registry'; (registry / 'index').mkdir(parents=True)
        source = self.base / 'source'; (source / 'src').mkdir(parents=True)
        (source / 'Cargo.toml').write_text('[workspace]\n[package]\nname="wi-release-test-dependency"\nversion="1.0.0"\nedition="2021"\n')
        (source / 'src/lib.rs').write_text('pub fn value() -> u32 { 42 }\npub fn location() -> &\'static str { file!() }\n#[test] fn unit() { assert_eq!(value(),42); }\n')
        target = shared.external_target()
        def cargo(args, cwd, env=None):
            result = subprocess.run(['cargo', *args], cwd=cwd, capture_output=True, text=True, timeout=120,
                                    env=packages.environment(self.base) if env is None else env)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            return result
        cargo(['package', '--offline', '--no-verify', '--allow-dirty'], source)
        archive = target / 'package/wi-release-test-dependency-1.0.0.crate'
        packages.add_registry(registry, archive, 'wi-release-test-dependency', '1.0.0', shared.sha(archive))
        extracted = self.base / 'extracted'
        with tarfile.open(archive) as packed:
            packed.extractall(extracted, filter='data')
        cargo(['test', '--lib', '--locked', '--offline'], extracted / 'wi-release-test-dependency-1.0.0')
        config = self.base / 'registry.toml'
        config.write_text('[source.crates-io]\nreplace-with="fixture"\n[source.fixture]\nlocal-registry=' + json.dumps(str(registry)) + '\n')
        consumer = self.base / 'consumer'; (consumer / 'src').mkdir(parents=True)
        (consumer / 'Cargo.toml').write_text('[workspace]\n[package]\nname="wi-release-test-consumer"\nversion="1.0.0"\nedition="2021"\n[dependencies]\nwi-release-test-dependency="=1.0.0"\n')
        (consumer / 'src/main.rs').write_text('fn main() { assert_eq!(wi_release_test_dependency::value(),42); println!("{}", wi_release_test_dependency::location()); }')
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
            with patch.dict(os.environ, {'CARGO_HOME': str(shared_home), 'RUSTFLAGS': '--cfg unwanted'}):
                env = packages.environment(staging)
            self.assertEqual(Path(env['CARGO_HOME']), staging / 'cargo-home')
            local = staging / 'registry'; shutil.copytree(registry, local)
            cfg = staging / 'registry.toml'
            cfg.write_text(config.read_text().replace(str(registry), str(local)))
            cargo(['--config', str(cfg), 'metadata', '--offline', '--format-version', '1'], consumer, env)
            roots = list((Path(env['CARGO_HOME']) / 'registry/src').iterdir())
            locations.extend(roots)
            env = shared.worker_environment(env, staging, roots)
            self.assertNotIn('RUSTFLAGS', env)
            cargo(['--config', str(cfg), 'install', '--offline', '--locked', '--version', '=1.0.0',
                   '--target', 'x86_64-unknown-linux-gnu', '--root', str(staging / 'install'),
                   'wi-release-test-consumer'], staging, env)
            binary = staging / 'install/bin/wi-release-test-consumer'
            output = subprocess.check_output([binary], text=True, timeout=30)
            self.assertIn('/wi/registry/wi-release-test-dependency-1.0.0/src/lib.rs', output)
            self.assertNotIn(str(self.base), output)
            digests.append(shared.sha(binary))
            recorded = shared.toolchain(env)
            self.assertEqual(recorded['cargo_home'], env['CARGO_HOME'])
            self.assertEqual(recorded['build_environment']['CARGO_ENCODED_RUSTFLAGS'], env['CARGO_ENCODED_RUSTFLAGS'])
        self.assertNotEqual(locations[0].name, locations[1].name)
        self.assertEqual(digests[0], digests[1])
        self.assertEqual(list(shared_home.iterdir()), [])


if __name__ == '__main__': unittest.main()
