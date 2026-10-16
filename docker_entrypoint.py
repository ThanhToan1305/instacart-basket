import json
import os
import sys
from pathlib import Path

ROOT = Path("/app")
os.chdir(ROOT)

mode = sys.argv[1] if len(sys.argv) > 1 else "dashboard"

if mode == "pipeline":
    folders = [
        "data/raw",
        "data/bronze",
        "data/silver",
        "data/gold",
        "outputs/metrics",
        "outputs/figures",
        "outputs/tables",
        "outputs/evidence",
        "logs",
        "docs",
    ]

    for folder in folders:
        (ROOT / folder).mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "src/run_pipeline.py",
        *sys.argv[2:],
    ]

elif mode in {"dashboard", "dashboard-sample"}:
    app = ROOT / "app/dashboard.py"

    if mode == "dashboard-sample":
        manifest = ROOT / "outputs/metrics/sample_integration.json"

        if not manifest.exists():
            raise SystemExit(
                "Chua co ket qua sample. Hay chay pipeline sample truoc."
            )

        info = json.loads(manifest.read_text(encoding="utf-8"))

        if info.get("status") != "PASS":
            raise SystemExit("Pipeline sample chua PASS. Hay kiem tra log.")

        workspace = Path(info["workspace"])
        if not (workspace / "data/gold").is_dir() or not (workspace / "outputs/metrics").is_dir():
            raise SystemExit(f"Khong tim thay Gold/metrics sample: {workspace}")
        # Use the current UI source with the saved sample artifacts.
        os.environ["INSTACART_DASHBOARD_ROOT"] = str(workspace)

    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app),
        "--server.address=0.0.0.0",
        "--server.port=8501",
        "--server.headless=true",
        "--browser.gatherUsageStats=false",
    ]

else:
    raise SystemExit(f"Che do khong hop le: {mode}")

os.execv(sys.executable, command)
