"""Synthetic handoff tests: never write fixtures to project outputs."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dashboard_bundle import file_hash
from dashboard_fixture import make_fixture
from package_project import ROOT_FILES, package
from recommendation_core import digest_json


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        pointer, predictions, scores = make_fixture(self.root / "outputs/dashboard")
        folder = pointer.parent / "synthetic-model"

        def write(relative, obj):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(obj), encoding="utf-8")

        self.write = write
        bundle = json.loads((folder / "bundle.json").read_text())
        serving = json.loads((folder / "serving.json").read_text())
        protocol = {"fixture": "synthetic for unit testing"}
        serving["protocol_sha256"] = digest_json(protocol)
        checks = [{"name": "synthetic-check", "pass": True}]
        model = bundle["model_report"]
        model.update(status="PASS", run_id="synthetic-model", source_split_run="synthetic-split",
                     source_preparation_run="synthetic-preparation", checks=checks,
                     protocol_sha256=digest_json(protocol), serving_sha256=digest_json(serving))
        test = bundle["test_report"]
        test.update(status="PASS", source_model_run="synthetic-model", source_split_run="synthetic-split", checks=checks)
        # Emulate the production export format only inside this temporary fixture.
        bundle.update(data_kind="observed_project_exports", serving_sha256=digest_json(serving))
        write("outputs/dashboard/synthetic-model/bundle.json", bundle)
        write("outputs/dashboard/synthetic-model/serving.json", serving)
        manifest = {"files": {p.relative_to(folder).as_posix(): file_hash(p) for p in folder.rglob("*")
                              if p.is_file() and p.name != "manifest.json"}}
        write("outputs/dashboard/synthetic-model/manifest.json", manifest)
        write("outputs/dashboard/latest.json", {"model_run": "synthetic-model", "bundle_dir": "synthetic-model",
                                                 "manifest_sha256": file_hash(folder / "manifest.json")})
        write("outputs/models/synthetic-model/report.json", model)
        write("outputs/models/synthetic-model/serving.json", serving)
        write("outputs/models/synthetic-model/grid.json", [])
        write("outputs/models/synthetic-model/validation_predictions.json", predictions)
        write("outputs/models/synthetic-model/validation_metrics.csv", "fixture")
        write("outputs/test/synthetic-split/report.json", test)
        write("outputs/test/synthetic-split/predictions.json", predictions)
        write("outputs/test/synthetic-split/test_metrics.csv", "fixture")
        write("outputs/splits/synthetic-split/report.json", {"status": "PASS", "checks": checks,
              "run_id": "synthetic-split", "source_preparation_run": "synthetic-preparation",
              "protocol_sha256": digest_json(protocol)})
        write("outputs/splits/synthetic-split/protocol.json", protocol)
        write("outputs/preparation/synthetic-preparation/report.json", {"status": "PASS", "checks": checks,
              "run_id": "synthetic-preparation", "source_bronze_run": "synthetic-bronze", "source_snapshot": "fixture",
              "kpis": bundle["kpis"], "sample": bundle["sample"]})
        raw = {"status": "READY", "verification": "local_sha256_equals_hdfs_readback_sha256"}
        write("outputs/raw/fixture.json", raw)
        write("outputs/ingest/synthetic-bronze.json", {"status": "PASS", "checks": checks,
              "run_id": "synthetic-bronze", "source_snapshot": "fixture", "source_manifest": raw})
        for name in ROOT_FILES:
            (self.root / name).write_text("fixture", encoding="utf-8")
        for name in ("data/raw/private.csv", ".env", "outputs/unrelated/report.json", "app/.venv/secret.py"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("DO NOT INCLUDE")

    def test_archive_contains_matching_evidence_and_excludes_unrelated_files(self):
        path, audit = package(self.root)
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            self.assertIn("instacart-basket/outputs/dashboard/latest.json", names)
            self.assertIn("instacart-basket/outputs/test/synthetic-split/predictions.json", names)
            self.assertFalse(any("private.csv" in n or n.endswith(".env") or ".venv" in n or "unrelated" in n for n in names))
            for relative, expected in audit["files"].items():
                self.assertEqual(hashlib.sha256(archive.read("instacart-basket/" + relative)).hexdigest(), expected)
        self.assertTrue((self.root / "data/raw/private.csv").exists())

    def test_changed_dashboard_is_rejected(self):
        (self.root / "outputs/dashboard/synthetic-model/catalog.csv").write_text("changed")
        with self.assertRaisesRegex(ValueError, "changed or missing"):
            package(self.root)

    def test_mismatched_run_is_rejected(self):
        path = "outputs/splits/synthetic-split/report.json"
        report = json.loads((self.root / path).read_text())
        report["source_preparation_run"] = "other-run"
        self.write(path, report)
        with self.assertRaisesRegex(ValueError, "lineage mismatch"):
            package(self.root)

    def test_missing_predictions_are_rejected(self):
        (self.root / "outputs/test/synthetic-split/predictions.json").unlink()
        with self.assertRaisesRegex(ValueError, "Missing or unsafe"):
            package(self.root)


if __name__ == "__main__":
    unittest.main()
