"""Negative cases for the narrowly reviewed crash-feature exception."""
import sys
sys.dont_write_bytecode = True
import copy
import unittest
from wi_crash_isolation import check_artifacts, check_manifests


class CrashIsolationTests(unittest.TestCase):
    def setUp(self):
        self.storage = {"package": {"name": "worldinterface-storage"},
                        "features": {"crash-points": []}}
        self.runtime = {"package": {"name": "worldinterface-runtime"}, "dev-dependencies": {
            "worldinterface-storage": {"features": ["crash-points"]}}}

    def reject(self, storage, runtime):
        with self.assertRaises(AssertionError):
            check_manifests([("storage", storage), ("runtime", runtime)])

    def test_empty_dev_exception(self):
        check_manifests([("storage", self.storage), ("runtime", self.runtime)])

    def test_default_and_feature_forwarding_rejected(self):
        for name in ("default", "forward"):
            storage = copy.deepcopy(self.storage)
            storage["features"][name] = ["crash-points"]
            self.reject(storage, self.runtime)
        runtime = copy.deepcopy(self.runtime)
        runtime["features"] = {"forward": ["worldinterface-storage/crash-points"]}
        self.reject(self.storage, runtime)

    def test_normal_build_target_and_alias_activation_rejected(self):
        for kind in ("dependencies", "build-dependencies"):
            for targeted in (False, True):
                runtime = copy.deepcopy(self.runtime)
                table = runtime.setdefault("target", {}).setdefault("cfg(unix)", {}) if targeted else runtime
                table[kind] = {"alias": {"package": "worldinterface-storage", "features": ["crash-points"]}}
                self.reject(self.storage, runtime)

    def test_other_dev_consumer_and_nonempty_feature_rejected(self):
        runtime = copy.deepcopy(self.runtime)
        runtime["package"]["name"] = "worldinterface-egress"
        self.reject(self.storage, runtime)
        self.storage["features"]["crash-points"] = ["dep:anything"]
        self.reject(self.storage, self.runtime)

    def test_compilation_evidence_rejects_activation_tests_and_missing_records(self):
        record = {"reason": "compiler-artifact", "target": {"name": "worldinterface_storage"},
                  "features": [], "profile": {"test": False}}
        check_artifacts([record])
        for records in ([], [{**record, "features": ["crash-points"]}],
                        [{**record, "profile": {"test": True}}]):
            with self.assertRaises(AssertionError):
                check_artifacts(records)


if __name__ == "__main__":
    unittest.main()
