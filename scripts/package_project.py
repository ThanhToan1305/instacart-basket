"""Collect current source and verified local results for report/slide preparation.

Uses Python standard library only. Does not run Docker, Spark, training or inference.
This archive is a source/results handoff, not a backup of HDFS or Docker images.
"""
import argparse
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from dashboard_bundle import file_hash, load_bundle
from recommendation_core import digest_json

PROJECT = Path(__file__).resolve().parents[1]
SOURCE_DIRS = {"scripts", "tests", "app", "docker", "docs", "report"}
SUFFIXES = {".py", ".md", ".txt", ".xml", ".conf", ".sh", ".toml", ".yaml", ".yml"}
ROOT_FILES = {"README.md", "compose.yaml", ".gitignore", ".gitattributes"}


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,99}", value):
        raise ValueError("Invalid run identifier")
    return value


def package(project):
    project = Path(project).resolve()
    pointer = project / "outputs/dashboard/latest.json"
    dashboard, bundle, serving, manifest = load_bundle(pointer)
    if bundle.get("data_kind") != "observed_project_exports":
        raise ValueError("Only observed project exports may be packaged")
    model_run = identifier(bundle["model_run"])
    split_run = identifier(bundle["source_split_run"])
    prep_run = identifier(bundle["source_preparation_run"])
    files = set()

    def include(path):
        path = Path(path)
        if path.is_symlink() or project not in path.resolve().parents or not path.is_file():
            raise ValueError(f"Missing or unsafe file: {path}")
        if path.stat().st_size > 100 * 1024 * 1024:
            raise ValueError(f"Unexpectedly large handoff file: {path.name}")
        files.add(path)
        return path

    def read(relative):
        return json.loads(include(project / relative).read_text(encoding="utf-8-sig"))

    def passed(report):
        if (report.get("status") != "PASS" or not report.get("checks")
                or not all(c.get("pass") is True for c in report["checks"])):
            raise ValueError("A source report has missing or failed checks")

    model = read(f"outputs/models/{model_run}/report.json")
    test = read(f"outputs/test/{split_run}/report.json")
    split = read(f"outputs/splits/{split_run}/report.json")
    prep = read(f"outputs/preparation/{prep_run}/report.json")
    for report in (model, test, split, prep):
        passed(report)
    if model != bundle["model_report"] or test != bundle["test_report"]:
        raise ValueError("Local model/test reports differ from the dashboard export")
    if (model["run_id"] != model_run or model["source_split_run"] != split_run
            or model["source_preparation_run"] != prep_run
            or test["source_model_run"] != model_run or test["source_split_run"] != split_run
            or split["run_id"] != split_run or split["source_preparation_run"] != prep_run
            or prep["run_id"] != prep_run or prep["kpis"] != bundle["kpis"]
            or prep["sample"] != bundle["sample"]):
        raise ValueError("Run lineage mismatch")
    protocol = read(f"outputs/splits/{split_run}/protocol.json")
    local_serving = read(f"outputs/models/{model_run}/serving.json")
    if (digest_json(protocol) != model["protocol_sha256"]
            or digest_json(protocol) != split["protocol_sha256"]
            or digest_json(protocol) != serving["protocol_sha256"]
            or local_serving != serving):
        raise ValueError("Protocol or serving model mismatch")
    bronze_run = identifier(prep["source_bronze_run"])
    bronze = read(f"outputs/ingest/{bronze_run}.json")
    passed(bronze)
    snapshot = identifier(bronze["source_snapshot"])
    raw = read(f"outputs/raw/{snapshot}.json")
    if (bronze["run_id"] != bronze_run or prep["source_snapshot"] != snapshot
            or raw != bronze["source_manifest"] or raw.get("status") != "READY"
            or raw.get("verification") != "local_sha256_equals_hdfs_readback_sha256"):
        raise ValueError("Raw/Bronze lineage mismatch")

    include(pointer)
    include(dashboard / "manifest.json")
    for name in manifest["files"]:
        include(dashboard / name)
    for relative in (
        f"outputs/models/{model_run}/grid.json",
        f"outputs/models/{model_run}/validation_predictions.json",
        f"outputs/models/{model_run}/validation_metrics.csv",
        f"outputs/test/{split_run}/predictions.json",
        f"outputs/test/{split_run}/test_metrics.csv",
    ):
        include(project / relative)
    for name in sorted(ROOT_FILES):
        include(project / name)
    for dirname in sorted(SOURCE_DIRS):
        for path in sorted((project / dirname).rglob("*")):
            relative = path.relative_to(project)
            if any(p in {"__pycache__", ".venv", "venv", "node_modules", ".git"} for p in relative.parts):
                continue
            if path.is_file() and (path.suffix in SUFFIXES or path.name == "Dockerfile"):
                include(path)
    if sum(p.stat().st_size for p in files) > 250 * 1024 * 1024:
        raise ValueError("Handoff exceeds 250 MiB; inspect outputs before sharing")

    hashes = {p.relative_to(project).as_posix(): file_hash(p) for p in sorted(files)}
    audit = {
        "schema_version": 1, "status": "PASS", "scope": "local_lineage_and_file_integrity",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "runs": {"model": model_run, "split": split_run, "preparation": prep_run,
                 "bronze": bronze_run, "snapshot": snapshot},
        "selected": model["selected"], "validation_metrics": model["validation_metrics"],
        "test_metrics": test["test_metrics"], "paired_test": bundle["paired_test"],
        "source_check_counts": {name: len(report["checks"]) for name, report in
                                (("bronze", bronze), ("preparation", prep), ("split", split), ("model", model), ("test", test))},
        "limitations": ["Not an HDFS/Docker-volume backup", "No fresh training, inference or data-quality audit",
                        "File hashes establish integrity of this handoff; they do not independently validate all metric semantics"],
        "files": hashes,
    }
    out = project / "outputs/delivery"
    out.mkdir(parents=True, exist_ok=True)
    name = "instacart-handoff-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    destination = out / (name + ".zip")
    temporary = out / (name + ".partial")
    try:
        with zipfile.ZipFile(temporary, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(files):
                content = path.read_bytes()
                if hashlib.sha256(content).hexdigest() != hashes[path.relative_to(project).as_posix()]:
                    raise ValueError("A source file changed while packaging; retry after jobs finish")
                archive.writestr("instacart-basket/" + path.relative_to(project).as_posix(), content)
            archive.writestr("HANDOFF_AUDIT.json", json.dumps(audit, ensure_ascii=False, indent=2))
            archive.writestr("READ_FIRST.txt", "Source/results handoff. Extract instacart-basket, read README.md and docs/08-dong-goi.md.\n"
                              "Raw CSVs, Docker images, HDFS Parquet and saved Spark models are NOT included.\n"
                              "The dashboard can use the included serving export without Spark/HDFS.\n"
                              "HANDOFF_AUDIT.json records file checksums and matching run IDs.\n")
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise ValueError("ZIP integrity check failed")
        temporary.rename(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination, audit


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        path, audit = package(PROJECT)
        print(f"PACKAGE_PASS files={len(audit['files'])} model_run={audit['runs']['model']}")
        print(f"archive={path}")
        print(f"sha256={file_hash(path)}")
    except (OSError, ValueError, KeyError, TypeError, zipfile.BadZipFile) as error:
        parser.exit(1, f"PACKAGE_FAILED: {error}\n")
