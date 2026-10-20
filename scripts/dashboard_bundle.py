"""Read a checksum-verified dashboard export; no Spark/Streamlit dependency."""
import hashlib
import json
import re
from pathlib import Path


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_bundle(pointer):
    pointer = Path(pointer)
    target = json.loads(pointer.read_text(encoding="utf-8"))
    folder_name = target["bundle_dir"]
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", folder_name):
        raise ValueError("Invalid dashboard bundle directory")
    root = (pointer.parent / folder_name).resolve()
    manifest_file = root / "manifest.json"
    if file_hash(manifest_file) != target["manifest_sha256"]:
        raise ValueError("Dashboard manifest checksum mismatch")
    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    for filename, expected in manifest["files"].items():
        path = (root / filename).resolve()
        if root not in path.parents or not path.is_file() or file_hash(path) != expected:
            raise ValueError(f"Dashboard export changed or missing: {filename}")
    required = {"bundle.json", "serving.json", "catalog.csv"}
    if not required.issubset(manifest["files"]):
        raise ValueError("Incomplete dashboard manifest")
    bundle = json.loads((root / "bundle.json").read_text(encoding="utf-8"))
    serving = json.loads((root / "serving.json").read_text(encoding="utf-8"))
    from recommendation_core import digest_json
    if (bundle["model_run"] != target["model_run"] or bundle["model_run"] != serving["model_run"]
            or digest_json(serving) != bundle["serving_sha256"]):
        raise ValueError("Dashboard model lineage mismatch")
    return root, bundle, serving, manifest
