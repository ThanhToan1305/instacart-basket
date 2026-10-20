"""Unit checks for upload publication and immutability guards; no Docker required."""
import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import upload_raw


class RawUploadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.project = Path(self.temp.name)
        (self.project / "data/raw").mkdir(parents=True)
        for name, header in upload_raw.SCHEMAS.items():
            with (self.project / "data/raw" / name).open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(header)
                writer.writerow(["1"] * len(header))
        self.calls = []
        self.mismatch = False
        self.exists = False

    def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.exists and args == (upload_raw.HDFS, "dfs", "-mkdir", "/instacart/raw/test-v1"):
            raise subprocess.CalledProcessError(1, args)
        if kwargs.get("capture"):
            name = Path(args[-1]).name
            digest = "0" * 64 if self.mismatch else upload_raw.sha256(self.project / "data/raw" / name)
            return SimpleNamespace(stdout=f"{digest}  -\n".encode())
        return SimpleNamespace(stdout=b"")

    def run_upload(self, snapshot="test-v1"):
        with patch.object(upload_raw, "PROJECT", self.project), patch.object(upload_raw, "execute", self.execute):
            upload_raw.upload(snapshot)

    def test_ready_is_last_and_contains_all_source_digests(self):
        self.run_upload()
        args, kwargs = self.calls[-1]
        self.assertEqual(args[-1], "/instacart/raw/test-v1/_READY.json")
        manifest = json.loads(kwargs["payload"])
        self.assertEqual(manifest["status"], "READY")
        self.assertEqual(len(manifest["files"]), 6)
        self.assertEqual(sum("-put" in args for args, _ in self.calls), 7)

    def test_checksum_mismatch_does_not_publish(self):
        self.mismatch = True
        with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
            self.run_upload()
        self.assertFalse(any(args[-1].endswith("_READY.json") for args, _ in self.calls))

    def test_existing_snapshot_is_not_overwritten(self):
        self.exists = True
        with self.assertRaises(subprocess.CalledProcessError):
            self.run_upload()
        self.assertFalse(any("-put" in args for args, _ in self.calls))

    def test_header_mismatch_fails_before_remote_mutation(self):
        (self.project / "data/raw/aisles.csv").write_text("aisle,aisle_id\nname,1\n")
        with self.assertRaisesRegex(ValueError, "Header/order mismatch"):
            self.run_upload()
        self.assertEqual(self.calls, [])

    def test_invalid_snapshot_cannot_enter_shell_or_hdfs_paths(self):
        with self.assertRaises(ValueError):
            self.run_upload("../bad; command")
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
