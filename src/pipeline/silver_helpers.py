"""Shared Silver validation and stage reporting; never read raw CSVs."""
import json
import time
from contextlib import contextmanager
from pathlib import Path
from pyspark.sql import functions as F
from pipeline.raw_validation import TABLES

ROOT = Path(__file__).resolve().parents[2]


@contextmanager
def stage(name, report, logger):
    start = time.perf_counter()
    logger.info('START %s', name)
    try:
        yield
    finally:
        seconds = round(time.perf_counter() - start, 3)
        report.setdefault('stage_seconds', {})[name] = seconds
        logger.info('END %s seconds=%s', name, seconds)


def require(check, message):
    if not check:
        raise ValueError(message)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding='utf-8')


def normalized(df, name):
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in TABLES[name]['schema']])


def null_counts(df, columns):
    row = df.agg(*[F.coalesce(F.sum(F.col(c).isNull().cast('long')), F.lit(0)).alias(c) for c in columns]).first()
    return row.asDict()


def foreign_key_check(child, parent, key):
    return child.select(key).join(parent.select(key), key, 'left_anti').count()


def directory_bytes(path):
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())
