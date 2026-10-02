"""Validate all raw CSVs and publish actual Spark metrics."""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.logger import get_logger
from common.spark_session import create_spark_session
from pipeline.raw_validation import ROOT, TABLES, inspect_files, validate_table, write_reports


def main():
    logger = get_logger('bronze_pipeline', ROOT / 'logs/bronze_pipeline.log')
    start = time.perf_counter()
    report = {'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'RUNNING', 'tables': {}}
    spark = None
    try:
        metadata = inspect_files(ROOT / 'data/raw')
        spark = create_spark_session('InstacartRawValidation')
        for name in TABLES:
            profile = validate_table(spark, name, ROOT / 'data/raw', metadata[name])
            report['tables'][name] = profile
            logger.info('%s: %s; rows=%s; seconds=%s', name, profile['status'], profile['row_count'], profile['validation_seconds'])
        if any(p['status'] != 'PASS' for p in report['tables'].values()):
            raise ValueError('Raw validation failed; see raw_profile.json')
        report['status'] = 'PASS'
    except Exception as exc:
        report.update(status='FAIL', error=str(exc))
        logger.exception('Raw validation failed')
        raise
    finally:
        if spark is not None:
            spark.stop()
        report['total_seconds'] = round(time.perf_counter() - start, 3)
        write_reports(report)


if __name__ == '__main__':
    main()
