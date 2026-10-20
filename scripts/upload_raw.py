"""Upload the six local CSVs as a new, checksum-verified HDFS snapshot."""
import argparse
import csv
import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from check_data import SCHEMAS

PROJECT = Path(__file__).resolve().parents[1]
HDFS = "/opt/hadoop/bin/hdfs"


def execute(*args, payload=None, capture=False):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "namenode", *args],
        cwd=PROJECT, input=payload, check=True,
        stdout=subprocess.PIPE if capture else None,
    )


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def upload(snapshot):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", snapshot):
        raise ValueError("Snapshot must use 1-64 letters/digits/_/- and start with a letter/digit.")
    entries = []
    # Validate and hash all local files before creating the remote destination.
    for filename, expected in SCHEMAS.items():
        path = PROJECT / "data" / "raw" / filename
        with path.open(encoding="utf-8-sig", newline="") as stream:
            if next(csv.reader(stream), []) != expected:
                raise ValueError(f"Header/order mismatch: {filename}; expected {expected}")
        entries.append({"name": filename, "size_bytes": path.stat().st_size, "sha256": sha256(path)})

    destination = f"/instacart/raw/{snapshot}"
    execute(HDFS, "dfs", "-mkdir", "-p", "/instacart/raw")
    # No -p and no overwrite here: an existing snapshot causes a hard failure.
    execute(HDFS, "dfs", "-mkdir", destination)
    for entry in entries:
        filename = entry["name"]
        print(f"UPLOADING {filename} ({entry['size_bytes']:,} bytes)", flush=True)
        execute(HDFS, "dfs", "-put", f"/input/{filename}", f"{destination}/{filename}")
        # pipefail ensures an HDFS read error cannot be hidden by sha256sum.
        result = execute(
            "/bin/bash", "-o", "pipefail", "-c",
            '/opt/hadoop/bin/hdfs dfs -cat "$1" | sha256sum',
            "verify-hdfs", f"{destination}/{filename}", capture=True,
        )
        remote_digest = result.stdout.decode("utf-8").split()[0]
        if remote_digest != entry["sha256"]:
            raise RuntimeError(f"SHA-256 mismatch for {filename}; snapshot remains incomplete.")
        print(f"VERIFIED {filename}: {remote_digest}", flush=True)

    manifest = {
        "schema_version": 1, "status": "READY", "snapshot": snapshot,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "hdfs_path": f"hdfs://namenode:9000{destination}",
        "verification": "local_sha256_equals_hdfs_readback_sha256",
        "files": entries,
    }
    payload = json.dumps(manifest, indent=2).encode("utf-8")
    out = PROJECT / "outputs" / "raw"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{snapshot}.json").write_bytes(payload)
    # Only this final marker makes the snapshot available to the next stage.
    execute(HDFS, "dfs", "-put", "-", f"{destination}/_READY.json", payload=payload)
    print(f"RAW_UPLOAD_PASS snapshot={snapshot} files={len(entries)} path={destination}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default="instacart-v1")
    args = parser.parse_args()
    try:
        upload(args.snapshot)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"RAW_UPLOAD_FAILED: {error}\nDo not delete or overwrite the snapshot automatically.\n")
