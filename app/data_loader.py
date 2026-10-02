"""Cached, bounded reads of Gold and precomputed metrics; never starts Spark."""
import json
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]

class DataUnavailable(RuntimeError):
    pass


def _path(relative):
    path = (ROOT / relative).resolve()
    allowed = (ROOT / 'data/gold', ROOT / 'outputs/metrics')
    if not any(path.is_relative_to(folder) for folder in allowed):
        raise DataUnavailable('Dashboard chỉ được đọc data/gold và outputs/metrics.')
    if not path.exists():
        raise DataUnavailable(f'Thiếu dữ liệu: {relative}')
    return path


def signature(relative):
    path = _path(relative)
    files = sorted(path.rglob('*.parquet')) if path.is_dir() else [path]
    if not files:
        raise DataUnavailable(f'Không có Parquet: {relative}')
    return tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in files)


@st.cache_data(show_spinner=False, max_entries=32)
def _read(relative, stamp):
    path = _path(relative)
    try:
        if path.suffix == '.json':
            return json.loads(path.read_text(encoding='utf-8'))
        if path.suffix == '.csv':
            return pd.read_csv(path)
        table = pq.ParquetDataset(path)
        rows = sum(pq.read_metadata(f.path).num_rows for f in table.fragments)
        if rows > 100_000:
            raise DataUnavailable(f'{relative}: vượt giới hạn 100.000 dòng cho dữ liệu dashboard.')
        return table.read().to_pandas()
    except DataUnavailable:
        raise
    except Exception as exc:
        raise DataUnavailable(f'Không đọc được {relative}: {exc}') from exc


def load(relative):
    return _read(relative, signature(relative))


def metrics(name):
    return load(f'outputs/metrics/{name}')


def gold(name):
    return load(f'data/gold/{name}')
