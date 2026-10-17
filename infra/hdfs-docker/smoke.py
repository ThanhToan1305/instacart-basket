"""Synthetic infrastructure check, separate from real Instacart evaluation."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def main():
    from pyspark.sql import SparkSession, functions as F

    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
    folder = Path('/app/outputs/hdfs_checks')
    folder.mkdir(parents=True, exist_ok=True)
    report_path = folder / (run_id + '.json')
    result = {
        'run_id': run_id,
        'purpose': 'synthetic_hdfs_infrastructure_check_only',
        'status': 'FAIL',
        'hdfs_base_uri': 'hdfs://namenode:9000',
        'checks': {},
    }
    spark = None
    try:
        spark = (
            SparkSession.builder.appName('Instacart Docker HDFS smoke ' + run_id)
            .master('local[2]')
            .config('spark.driver.memory', '1g')
            .config('spark.sql.shuffle.partitions', '2')
            .config('spark.hadoop.fs.defaultFS', 'file:///')
            .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        result['spark_version'] = spark.version
        result['hadoop_client_version'] = str(spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
        rows = [(1, 'Smoke Apple', 2), (2, 'Smoke Milk', 1), (3, 'Smoke Bread', 4)]
        expected = spark.createDataFrame(rows, 'product_id INT, product_name STRING, quantity INT')
        uri = f'hdfs://namenode:9000/instacart/smoke/{run_id}/items'
        expected.write.mode('errorifexists').parquet(uri)
        actual = spark.read.parquet(uri)
        result['checks']['row_count'] = actual.count() == 3
        result['checks']['schema_types'] = actual.dtypes == expected.dtypes
        result['checks']['roundtrip_equal'] = (
            expected.exceptAll(actual).limit(1).count() == 0
            and actual.exceptAll(expected).limit(1).count() == 0
        )
        result['checks']['quantity_sum'] = actual.agg(F.sum('quantity')).first()[0] == 7
        actual.show(truncate=False)
        result['parquet_uri'] = uri
        raw_uri = 'hdfs://namenode:9000/instacart/raw/aisles.csv'
        jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(raw_uri)
        fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(jpath):
            aisles = spark.read.option('header', True).schema('aisle_id INT, aisle STRING').csv(raw_uri)
            count = aisles.count()
            result['aisles_rows'] = count
            result['checks']['real_aisles_nonempty'] = count > 0
            result['checks']['real_aisles_valid'] = aisles.filter(F.col('aisle_id').isNull() | F.col('aisle').isNull()).limit(1).count() == 0
        else:
            result['real_aisles_read'] = 'SKIP_NOT_UPLOADED'
        if not all(result['checks'].values()):
            raise RuntimeError('At least one HDFS check failed.')
        result['status'] = 'PASS'
    except Exception as exc:
        result['error'] = repr(exc)
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                result['shutdown_error'] = repr(exc)
                result['status'] = 'FAIL'
        report_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f"SMOKE_STATUS={result['status']}\nREPORT={report_path}", flush=True)
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
