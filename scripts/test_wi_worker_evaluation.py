"""Non-live regressions for the shared validator, using private committed fixtures."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parent))
from wi_worker_evaluation import PACKET, canonical, check_packet, digest, expected_manifest

ROOT = Path(__file__).resolve().parents[1]


class PacketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = tempfile.TemporaryDirectory()
        cls.root = Path(cls.base.name)
        manifest = expected_manifest(ROOT)
        for name in manifest['files']:
            dest = cls.root / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, dest)
        cls.manifest = manifest
        (cls.root / PACKET).write_text(json.dumps(manifest, indent=2) + '\n')
        cls.git('init', '-q')
        cls.git('add', '-f', '.')
        cls.git('-c', 'user.name=Packet Test', '-c', 'user.email=packet@example.invalid',
                'commit', '-qm', 'private packet fixture')

    @classmethod
    def tearDownClass(cls):
        cls.base.cleanup()

    @classmethod
    def git(cls, *args):
        return subprocess.run(['git', '-C', str(cls.root), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)

    def tearDown(self):
        self.git('reset', '--hard', '-q', 'HEAD')
        self.git('clean', '-fdxq')

    def write(self, manifest):
        (self.root / PACKET).write_text(json.dumps(manifest, indent=2) + '\n')

    def test_complete_scope_and_canonical_digest(self):
        self.assertEqual(check_packet(self.root, True), self.manifest)
        out = subprocess.run(['python3', '-I', '-B', str(self.root / 'scripts/wi_worker_evaluation.py'),
                              '--root', str(self.root), '--require-committed'],
                             capture_output=True, check=True, timeout=60)
        self.assertEqual(json.loads(out.stdout)['manifest_digest'], digest(canonical(self.manifest)))

    def test_every_scope_field_is_exact(self):
        for key in self.manifest:
            with self.subTest(key=key):
                changed = copy.deepcopy(self.manifest)
                value = changed[key]
                if isinstance(value, bool): changed[key] = not value
                elif isinstance(value, int): changed[key] = value + 1
                elif isinstance(value, str): changed[key] = '0' * 64
                elif isinstance(value, list): changed[key] = value[:-1]
                else: changed[key].pop(next(iter(value)))
                self.write(changed)
                with self.assertRaises(ValueError): check_packet(self.root)
        self.write(dict(self.manifest, unknown=True))
        with self.assertRaises(ValueError): check_packet(self.root)

    def test_changed_deleted_added_and_omitted_sources(self):
        name = 'crates/worldinterface-runtime/src/lib.rs'
        path = self.root / name
        original = path.read_bytes()
        path.write_bytes(original + b'\n// drift\n')
        with self.assertRaises(ValueError): check_packet(self.root)
        path.unlink()
        with self.assertRaises((ValueError, OSError)): check_packet(self.root)
        path.write_bytes(original)
        extra = path.with_name('new_bound_source.rs')
        extra.write_text('// added source')
        with self.assertRaises(ValueError): check_packet(self.root)
        extra.unlink()
        changed = copy.deepcopy(self.manifest)
        changed['files'].pop(name)
        self.write(changed)
        with self.assertRaises(ValueError): check_packet(self.root)
        changed = copy.deepcopy(self.manifest)
        changed['files'][name] = '0' * 64
        self.write(changed)
        with self.assertRaises(ValueError): check_packet(self.root)

    def test_repeated_checks_detect_same_size_drift_with_preserved_mtime(self):
        path = self.root / 'crates/worldinterface-runtime/src/lib.rs'
        original = path.read_bytes()
        before = path.stat()
        check_packet(self.root, True)
        path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(path.stat().st_size, before.st_size)
        self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)
        with self.assertRaises(ValueError): check_packet(self.root, True)
        path.write_bytes(original)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        check_packet(self.root, True)

    def test_uncommitted_refresh_never_authorizes_live_setup(self):
        path = self.root / 'crates/worldinterface-runtime/src/lib.rs'
        path.write_bytes(path.read_bytes() + b'\n// candidate\n')
        self.write(expected_manifest(self.root))
        check_packet(self.root)
        with self.assertRaises(ValueError): check_packet(self.root, True)
        # Staging is not commitment either.
        self.git('add', '.')
        with self.assertRaises(ValueError): check_packet(self.root, True)
        # Even an unchanged manifest cannot hide drift via index flags.
        self.git('reset', '--hard', '-q', 'HEAD')
        self.git('update-index', '--assume-unchanged', 'crates/worldinterface-runtime/src/lib.rs')
        path.write_bytes(path.read_bytes() + b'\n// hidden drift\n')
        with self.assertRaises(ValueError): check_packet(self.root, True)
        self.git('update-index', '--no-assume-unchanged', 'crates/worldinterface-runtime/src/lib.rs')

    def test_malformed_paths_symlinks_and_manifest(self):
        for name in ['../escape', '/absolute', 'a/../b', './a', 'a//b', 'a\\b', '']:
            changed = copy.deepcopy(self.manifest)
            changed['files'][name] = '0' * 64
            self.write(changed)
            with self.assertRaises(ValueError): check_packet(self.root)
        self.write(self.manifest)
        path = self.root / 'crates/worldinterface-runtime/src/lib.rs'
        path.unlink()
        path.symlink_to('worker/evaluation.rs')
        with self.assertRaises(ValueError): check_packet(self.root)
        self.git('reset', '--hard', '-q', 'HEAD')
        directory = self.root / 'crates/worldinterface-runtime/src/extra'
        directory.symlink_to('worker', target_is_directory=True)
        with self.assertRaises(ValueError): check_packet(self.root)
        directory.unlink()
        for raw in ['{', '[]', '{"version":1,"version":1}', '{"files":null}']:
            (self.root / PACKET).write_text(raw)
            with self.assertRaises(ValueError): check_packet(self.root)

    def test_discovery_preserves_fixture_data_and_workspace_build_exclusions(self):
        generated = self.root / 'crates/worldinterface-runtime/target/generated.rs'
        generated.parent.mkdir(parents=True)
        generated.write_text('// excluded workspace build output')
        check_packet(self.root, True)
        for name in ['fixtures/pre-wi-fabric-2/exo/repos/sample-rust/target/input.txt',
                     'contracts/target/normative.json']:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('bound fixture or normative data')
            with self.assertRaises(ValueError): check_packet(self.root)
            path.unlink()

    def test_historical_receipts_and_anchor_are_not_active_inputs(self):
        for name in ['conformance/wi-fabric-2/worker/evaluation-approval.json',
                     'crates/worldinterface-runtime/tests/support/worker_approval_anchor.rs']:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('untrusted historical contents, expired or absent')
        check_packet(self.root, True)
        for name in ['conformance/wi-fabric-2/worker/evaluation-approval.json',
                     'crates/worldinterface-runtime/tests/support/worker_approval_anchor.rs']:
            (self.root / name).unlink()
        check_packet(self.root, True)


if __name__ == '__main__':
    unittest.main()
