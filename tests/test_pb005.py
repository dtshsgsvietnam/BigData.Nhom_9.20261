"""Contract-boundary and failure checks for the PB-005 local proof."""

import copy
from contextlib import closing
import importlib.util
from pathlib import Path
import sqlite3
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("pb005", ROOT / "scripts/verify_pb005.py")
proof = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proof)


class ArchitectureTests(unittest.TestCase):
    def setUp(self):
        self.config = proof.read_json(ROOT / "config/architecture.json")
        self.fixture = ROOT / "tests/fixtures/pb005"

    def test_complete_recovery_and_idempotent_load(self):
        with tempfile.TemporaryDirectory() as directory:
            result = proof.verify(Path(directory) / "run", ROOT / "config/architecture.json", self.fixture)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["tables"]["pageviews_history"], {"rows": 6, "views": 45})
            self.assertEqual(result["tables"]["pageviews_replay"], {"rows": 6, "views": 45})

    def test_reject_shared_table(self):
        self.config["branches"]["replay"]["table"] = "pageviews_history"
        with self.assertRaisesRegex(ValueError, "tables must be separate"):
            proof.check_config(self.config)

    def test_reject_output_checkpoint_overlap(self):
        branch = self.config["branches"]["replay"]
        branch["checkpoint_uri"] = branch["output_uri"]
        with self.assertRaisesRegex(ValueError, "overlap"):
            proof.check_config(self.config)

    def test_reject_changed_raw(self):
        manifest = copy.deepcopy(proof.read_json(self.fixture / "manifest.json"))
        manifest["files"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            proof.source_events(manifest, self.fixture, self.config, "run")

    def test_reject_other_branch_in_loader(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            with self.assertRaisesRegex(ValueError, "another branch"):
                proof.load(connection, "pageviews_history", "history", [{"branch": "replay"}])

    def test_reject_checkpoint_from_another_run(self):
        manifest = proof.read_json(self.fixture / "manifest.json")
        events = proof.source_events(manifest, self.fixture, self.config, "first")
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            proof.process_branch(workspace, "replay", events, stop_after=2)
            changed = [{**event, "run_id": "second"} for event in events]
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                proof.process_branch(workspace, "replay", changed)


if __name__ == "__main__":
    unittest.main()
