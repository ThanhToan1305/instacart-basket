"""Validate raw data, write Snappy Parquet, and reconcile every Bronze table."""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pyarrow.parquet as pq
from pyspark.sql import functions as F
from common.logger import get_logger
from common.spark_session import create_spark_session
from pipeline.raw_validation import ROOT, TABLES, inspect_files, read_raw, validate_table, write_reports


def main():
    logger = get_logger('bronze_pipeline', ROOT / 'logs/bronze_pipeline.log')
    start = time.perf_counter()
    run_id = 'bronze_' + uuid4().hex
    report = {'run_id': run_id, 'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'RUNNING', 'tables': {}}
    spark = None
    try:
        raw_dir = ROOT / 'data/raw'
        metadata = inspect_files(raw_dir)
        logger.info('Input files: %s', metadata)
        bronze_root = ROOT / 'data/bronze'
        for name in TABLES:
            if (bronze_root / name).exists():
                raise FileExistsError(f'Existing Bronze table: {bronze_root / name}; refusing to overwrite')
        spark = create_spark_session('InstacartBronzeIngestion')
        # Validate all six tables before writing any Bronze data.
        for name in TABLES:
            profile = validate_table(spark, name, raw_dir, metadata[name])
            report['tables'][name] = profile
            logger.info('Validation %s: %s; rows=%s; seconds=%s', name, profile['status'], profile['row_count'], profile['validation_seconds'])
            write_reports(report)
        if any(p['status'] != 'PASS' for p in report['tables'].values()):
            raise ValueError('Raw validation failed; Bronze ingestion stopped')
        stage_root = bronze_root / ('.staging_' + uuid4().hex)
        for name, spec in TABLES.items():
            table_start = time.perf_counter()
            path = stage_root / name
            source = read_raw(spark, name, raw_dir)
            enriched = (source.withColumn('ingestion_time', F.current_timestamp())
                        .withColumn('source_file', F.input_file_name()).withColumn('run_id', F.lit(run_id)))
            enriched.write.mode('errorifexists').option('compression', 'snappy').parquet(str(path))
            bronze = spark.read.parquet(str(path))
            bronze_rows = bronze.count()
            expected = [(f.name, f.dataType) for f in enriched.schema]
            schema_ok = [(f.name, f.dataType) for f in bronze.schema] == expected
            parquet_files = list(path.glob('*.parquet'))
            snappy = bool(parquet_files) and all(
                pq.ParquetFile(p).metadata.row_group(i).column(j).compression == 'SNAPPY'
                for p in parquet_files
                for i in range(pq.ParquetFile(p).metadata.num_row_groups)
                for j in range(pq.ParquetFile(p).metadata.num_columns))
            metadata_nulls = bronze.where(F.col('ingestion_time').isNull() | F.col('source_file').isNull() | (F.col('source_file') == '') | F.col('run_id').isNull()).count()
            profile = report['tables'][name]
            matched = bronze_rows == profile['row_count']
            profile.update(bronze_path=str(bronze_root / name), bronze_rows=bronze_rows,
                           bronze_size_bytes=sum(p.stat().st_size for p in path.rglob('*') if p.is_file()),
                           ingestion_seconds=round(time.perf_counter() - table_start, 3),
                           reconciliation='PASS' if matched else 'FAIL', bronze_schema_matches=schema_ok,
                           snappy=snappy, metadata_null_rows=metadata_nulls)
            profile['status'] = 'PASS' if matched and schema_ok and snappy and metadata_nulls == 0 else 'FAIL'
            logger.info('Bronze %s: %s; CSV=%s; Parquet=%s; bytes=%s; seconds=%s', name, profile['status'], profile['row_count'], bronze_rows, profile['bronze_size_bytes'], profile['ingestion_seconds'])
            write_reports(report)
            if profile['status'] != 'PASS':
                raise ValueError(f'Bronze reconciliation failed: {name}; staging retained at {stage_root}')
        for name in TABLES:
            (stage_root / name).rename(bronze_root / name)
        stage_root.rmdir()
        # Read final published paths, not only staging paths.
        for name, profile in report['tables'].items():
            final_count = spark.read.parquet(profile['bronze_path']).count()
            profile['final_read_rows'] = final_count
            if final_count != profile['row_count']:
                profile['status'] = 'FAIL'
                raise ValueError(f'Final-path reconciliation failed: {name}')
        report['status'] = 'PASS'
    except Exception as exc:
        report.update(status='FAIL', error=str(exc))
        logger.exception('Bronze pipeline failed')
        raise
    finally:
        if spark is not None:
            spark.stop()
        report['total_seconds'] = round(time.perf_counter() - start, 3)
        write_reports(report)
        logger.info('Pipeline %s; total_seconds=%s', report['status'], report['total_seconds'])


if __name__ == '__main__':
    main()
