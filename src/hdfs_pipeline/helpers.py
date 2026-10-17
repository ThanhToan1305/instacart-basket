"""Reuse the ETL filesystem contract without duplicating its implementation."""
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location('instacart_hdfs_etl', ROOT / 'infra/hdfs-docker/etl.py')
_etl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_etl)
Hdfs = _etl.Hdfs
require = _etl.require
dump = _etl.dump
now = _etl.now
validate_base = _etl.validate_base
schema_signature = _etl.parquet_schema_signature


def valid_id(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value),
            'Invalid run ID')
    return value


def source_manifest(report, etl_id, root=None):
    valid_id(etl_id)
    require(report.get('status') == 'PASS' and report.get('run_id') == etl_id,
            'ETL report must be PASS for the requested run')
    base = validate_base(report['hdfs_root'])
    require(base.rsplit('/', 1)[-1] == etl_id, 'ETL root/run ID mismatch')
    require(root is None or validate_base(root) == base, 'Requested HDFS root differs from ETL report')
    for name in ('orders', 'baskets'):
        entry = report['silver'][name]
        require(entry.get('status') == 'PASS' and entry['uri'] == base + '/silver/' + name,
                'Invalid Silver lineage: ' + name)
    return base
