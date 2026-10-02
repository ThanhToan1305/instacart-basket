#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
if [[ ! -x .venv/bin/python ]]; then
  echo 'Thiếu .venv/bin/python. Tạo venv và cài requirements.txt trước.' >&2
  exit 1
fi
exec .venv/bin/python src/run_pipeline.py "$@"
