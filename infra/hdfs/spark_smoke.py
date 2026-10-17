"""Kiểm tra kỹ thuật trên dữ liệu nhỏ tự tạo, không phải KPI Instacart."""
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


def main():
    try:
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    except ImportError as exc:
        raise SystemExit('Chưa có PySpark trong Python hiện tại. Kích hoạt .venv và cài theo requirements của dự án.') from exc

    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
    output_dir = Path(__file__).resolve().parents[2] / 'outputs' / 'hdfs_smoke'
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {
        'run_id': run_id,
        'purpose': 'synthetic_infrastructure_smoke_only',
        'hdfs_uri': 'hdfs://127.0.0.1:9000',
        'status': 'FAIL',
        'checks': {},
    }
    spark = None
    report_path = output_dir / (run_id + '.json')
    try:
        spark = (
            SparkSession.builder.appName('Instacart HDFS Smoke ' + run_id)
            .master('local[2]')
            .config('spark.driver.memory', '1g')
            .config('spark.sql.shuffle.partitions', '2')
            .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true')
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel('WARN')
        result['spark_version'] = spark.version
        result['hadoop_client_version'] = str(
            spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
        result['java_home'] = os.environ.get('JAVA_HOME')
        schema = StructType([
            StructField('product_id', IntegerType(), False),
            StructField('product_name', StringType(), False),
            StructField('quantity', IntegerType(), False),
        ])
        rows = [(1, 'Smoke Apple', 2), (2, 'Smoke Milk', 1), (3, 'Smoke Bread', 4)]
        expected = spark.createDataFrame(rows, schema)
        parquet_uri = f'hdfs://127.0.0.1:9000/instacart/smoke/{run_id}/items'
        expected.write.mode('errorifexists').parquet(parquet_uri)
        actual = spark.read.parquet(parquet_uri)
        result['checks']['row_count'] = actual.count() == 3
        # Parquet read may mark fields nullable; compare names/types, not nullability.
        result['checks']['schema_types'] = actual.dtypes == expected.dtypes
        result['checks']['roundtrip_equal'] = (
            expected.exceptAll(actual).limit(1).count() == 0
            and actual.exceptAll(expected).limit(1).count() == 0
        )
        result['checks']['quantity_sum'] = actual.agg(F.sum('quantity')).first()[0] == 7
        actual.show(truncate=False)
        result['parquet_uri'] = parquet_uri
        # Read a real, small dimension only if all six CSVs were uploaded beforehand.
        raw_uri = 'hdfs://127.0.0.1:9000/instacart/raw/aisles.csv'
        jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(raw_uri)
        fs = jpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(jpath):
            raw_schema = 'aisle_id INT, aisle STRING'
            aisles = spark.read.option('header', True).schema(raw_schema).csv(raw_uri)
            count = aisles.count()
            result['real_aisles_rows'] = count
            result['checks']['real_aisles_nonempty'] = count > 0
        else:
            result['real_aisles_read'] = 'SKIP_NOT_UPLOADED'
        if not all(result['checks'].values()):
            raise RuntimeError('Một kiểm tra đọc ghi HDFS chưa đạt.')
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
        report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f"\nSMOKE_STATUS={result['status']}\nREPORT={report_path}")
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    sys.exit(main())
