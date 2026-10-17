"""Real Instacart sample ETL on HDFS. Does not train or replace the old model.

Uses TABLES, profile_frame and Silver transformation helpers from the supplied
prototype. All Spark table I/O uses explicit hdfs:// URIs. Local disk holds
only sample CSV inputs, reports and figures. A PASS requires data readback,
schema/count checks, FK checks and basket/KPI reconciliation.
"""
import argparse
import csv
import hashlib
import importlib
import json
import math
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

ROOT = Path('/app')
sys.path.insert(0, str(ROOT / 'src'))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parquet_schema_signature(dtype):
    """Retain column order/names/types; ignore Parquet nullability relaxation.

    Spark can read a non-null array element as nullable from Parquet. Compare
    the full type tree without nullable/containsNull/valueContainsNull flags;
    a change from array<int> to array<bigint>, for example, must still fail.
    Accept Spark DataType.jsonValue() values to keep this independently testable.
    """
    if not isinstance(dtype, dict):
        return dtype
    result = dict(dtype)
    if dtype.get('type') == 'struct':
        result['fields'] = [
            {**{key: value for key, value in field.items() if key != 'nullable'},
             'type': parquet_schema_signature(field['type'])}
            for field in dtype['fields']
        ]
    elif dtype.get('type') == 'array':
        result.pop('containsNull', None)
        result['elementType'] = parquet_schema_signature(dtype['elementType'])
    elif dtype.get('type') == 'map':
        result.pop('valueContainsNull', None)
        result['keyType'] = parquet_schema_signature(dtype['keyType'])
        result['valueType'] = parquet_schema_signature(dtype['valueType'])
    return result


def now():
    return datetime.now(timezone.utc).isoformat()


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temp.replace(path)


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def sample_threshold(fraction):
    require(math.isfinite(fraction) and 0 < fraction <= 1, 'sample-fraction must be in (0, 1]')
    threshold = round(fraction * 10000)
    require(threshold >= 1, 'Minimum effective sample fraction is 0.0001')
    return threshold


def validate_base(uri):
    parsed = urlsplit(uri)
    require(parsed.scheme == 'hdfs' and parsed.hostname and parsed.port,
            'Use an explicit HDFS URI, e.g. hdfs://namenode:9000/instacart/runs')
    require(not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
            'HDFS base must not contain credentials, query or fragment')
    require(parsed.path not in ('', '/') and '..' not in parsed.path.split('/'),
            'HDFS base must name a project directory')
    return uri.rstrip('/')


def prepare_sample(source, target, fraction):
    """Same deterministic order_id bucket rule as evidence/sample_pipeline.py.

    PyArrow streams 1 MiB CSV batches: no full CSV DataFrame in Python memory.
    Dimension CSVs are copied whole. All order tables share the SAME mask.
    Validates every source before writing any sample, never invents data.
    """
    import pyarrow as pa
    import pyarrow.csv as pcsv
    from pipeline.raw_validation import inspect_files, TABLES
    threshold = sample_threshold(fraction)
    metadata = inspect_files(source)
    target.mkdir(parents=True, exist_ok=False)
    counts = {}
    for name, spec in TABLES.items():
        path = source / spec['file']
        dest = target / spec['file']
        reader = pcsv.open_csv(path, read_options=pcsv.ReadOptions(block_size=1024 * 1024))
        if name in ('aisles', 'departments', 'products'):
            shutil.copy2(path, dest)
            counts[name] = sum(batch.num_rows for batch in reader)
        else:
            count = 0
            with pcsv.CSVWriter(dest, reader.schema) as writer:
                for batch in reader:
                    mask = pa.array([int(order_id) % 10000 < threshold
                                     for order_id in batch.column('order_id').to_pylist()])
                    selected = batch.filter(mask)
                    writer.write_batch(selected)
                    count += selected.num_rows
            counts[name] = count
        require(counts[name] > 0, f'Empty sample table {name}; increase sample fraction')
        print(f'SAMPLE {name}: {counts[name]:,} rows', flush=True)
    inspect_files(target)
    return {'rule': f'order_id % 10000 < {threshold}',
            'requested_fraction': fraction, 'effective_bucket_fraction': threshold / 10000,
            'note': 'Order-based ETL sample; not an independent recommendation test set.',
            'source_files': metadata, 'rows': counts}


class Hdfs:
    def __init__(self, spark, base):
        self.spark = spark
        self.base = validate_base(base)
        self.jpath = spark._jvm.org.apache.hadoop.fs.Path
        self.fs = self.jpath(base).getFileSystem(spark._jsc.hadoopConfiguration())
        require(str(self.fs.getUri()).startswith('hdfs://'), 'Resolved filesystem is not HDFS')

    def uri(self, suffix=''):
        return self.base + ('/' + suffix.strip('/') if suffix else '')

    def exists(self, suffix):
        return bool(self.fs.exists(self.jpath(self.uri(suffix))))

    def mkdir(self, suffix):
        require(self.fs.mkdirs(self.jpath(self.uri(suffix))), f'Cannot mkdir {self.uri(suffix)}')

    def size(self, suffix):
        return int(self.fs.getContentSummary(self.jpath(self.uri(suffix))).getLength())

    def upload(self, local, suffix):
        require(not self.exists(suffix), f'HDFS target exists; refusing overwrite: {self.uri(suffix)}')
        self.fs.copyFromLocalFile(False, False, self.jpath(local.resolve().as_uri()),
                                  self.jpath(self.uri(suffix)))
        require(self.size(suffix) == local.stat().st_size, f'Upload size mismatch: {suffix}')

    def save(self, df, suffix, expected=None):
        require(not self.exists(suffix), f'Existing output; refusing overwrite: {self.uri(suffix)}')
        df.write.mode('errorifexists').option('compression', 'snappy').parquet(self.uri(suffix))
        saved = self.spark.read.parquet(self.uri(suffix))
        rows = saved.count()
        require(expected is None or rows == expected, f'Count mismatch {suffix}: {rows} != {expected}')
        require(parquet_schema_signature(saved.schema.jsonValue()) ==
                parquet_schema_signature(df.schema.jsonValue()),
                f'Schema mismatch: {suffix}; expected={df.schema.json()}; actual={saved.schema.json()}')
        require(self.exists(suffix + '/_SUCCESS'), f'Missing _SUCCESS: {suffix}')
        print(f'HDFS WRITE {suffix}: {rows:,} rows', flush=True)
        return saved, {'uri': self.uri(suffix), 'rows': rows, 'bytes': self.size(suffix),
                       'schema': saved.schema.jsonValue(),
                       'schema_check': 'full type tree; Parquet nullability flags ignored', 'status': 'PASS'}


def bronze_stage(spark, hdfs, inputs, sampling, report):
    from pyspark.sql import functions as F
    from pipeline.raw_validation import TABLES, profile_frame
    report['raw'] = {}
    report['bronze'] = {}
    hdfs.mkdir('raw')
    # Verify all six uploaded raw tables before publishing Bronze outputs.
    for name, spec in TABLES.items():
        path = inputs / spec['file']
        hdfs.upload(path, 'raw/' + spec['file'])
        frame = (spark.read.schema(spec['schema']).option('header', True)
                 .option('enforceSchema', False).option('mode', 'FAILFAST')
                 .option('multiLine', True).option('escape', '"').option('encoding', 'UTF-8')
                 .csv(hdfs.uri('raw/' + spec['file'])))
        profile = profile_frame(frame, name)
        profile['uri'] = hdfs.uri('raw/' + spec['file'])
        profile['sample_sha256'] = sha256_file(path)
        report['raw'][name] = profile
        require(profile['status'] == 'PASS', f'Raw data quality failed for {name}: {profile}')
        require(profile['row_count'] == sampling['rows'][name], f'CSV sample count differs: {name}')
    for name, spec in TABLES.items():
        source = (spark.read.schema(spec['schema']).option('header', True)
                  .option('enforceSchema', False).option('mode', 'FAILFAST')
                  .option('multiLine', True).option('escape', '"')
                  .csv(hdfs.uri('raw/' + spec['file'])))
        enriched = (source.withColumn('ingestion_time', F.current_timestamp())
                    .withColumn('source_file', F.input_file_name())
                    .withColumn('run_id', F.lit(report['run_id'])))
        saved, info = hdfs.save(enriched, 'bronze/' + name, sampling['rows'][name])
        nulls = saved.where(F.col('ingestion_time').isNull() | F.col('source_file').isNull()
                            | (F.col('source_file') == '') | F.col('run_id').isNull()).count()
        require(nulls == 0, f'Bronze traceability metadata invalid: {name}')
        info['metadata_null_rows'] = nulls
        report['bronze'][name] = info


def silver_stage(spark, hdfs, report):
    from pyspark.sql import functions as F
    from pipeline.raw_validation import TABLES, profile_frame
    from pipeline.silver_helpers import normalized, foreign_key_check
    tables = {}
    for name, spec in TABLES.items():
        frame = normalized(spark.read.parquet(hdfs.uri('bronze/' + name)), name)
        profile = profile_frame(frame, name)
        require(profile['status'] == 'PASS', f'Bronze readback quality failed: {name}')
        tables[name] = frame
    fks = {'products_aisle': foreign_key_check(tables['products'], tables['aisles'], 'aisle_id'),
           'products_department': foreign_key_check(tables['products'], tables['departments'], 'department_id')}
    for name in ('order_products_prior', 'order_products_train'):
        fks[name + '_order'] = foreign_key_check(tables[name], tables['orders'], 'order_id')
        fks[name + '_product'] = foreign_key_check(tables[name], tables['products'], 'product_id')
        fks[name + '_eval_set'] = (tables[name].join(tables['orders'].select('order_id', 'eval_set'), 'order_id')
                                  .where(F.col('eval_set') != name.removeprefix('order_products_')).count())
    report['foreign_keys'] = fks
    require(all(count == 0 for count in fks.values()), f'Foreign key failures: {fks}')
    report['silver'] = {}

    def save(name, frame, expected):
        result, info = hdfs.save(frame, 'silver/' + name, expected)
        report['silver'][name] = info
        return result

    raw_rows = {name: p['row_count'] for name, p in report['raw'].items()}
    orders = save('orders', tables['orders'], raw_rows['orders'])
    products = (tables['products'].join(F.broadcast(tables['aisles']), 'aisle_id')
                .join(F.broadcast(tables['departments']), 'department_id')
                .select('product_id', 'product_name', 'aisle_id', 'aisle', 'department_id', 'department'))
    products = save('products', products, raw_rows['products'])
    items = (tables['order_products_prior'].withColumn('eval_set', F.lit('prior'))
             .unionByName(tables['order_products_train'].withColumn('eval_set', F.lit('train')))
             .dropDuplicates(['order_id', 'product_id']))
    expected = raw_rows['order_products_prior'] + raw_rows['order_products_train']
    items = save('order_items', items, expected)
    enriched = (items.join(orders.drop('eval_set'), 'order_id')
                .join(F.broadcast(products), 'product_id')
                .select('order_id', 'user_id', 'product_id', 'product_name', 'aisle_id', 'aisle',
                        'department_id', 'department', 'eval_set', 'order_number', 'order_dow',
                        'order_hour_of_day', 'days_since_prior_order', 'add_to_cart_order', 'reordered'))
    save('order_items_enriched', enriched, expected)
    baskets = (items.join(orders.select('order_id', 'user_id'), 'order_id')
               .groupBy('order_id', 'user_id').agg(F.sort_array(F.collect_set('product_id')).alias('items'))
               .withColumn('basket_size', F.size('items')))
    baskets = save('baskets', baskets, items.select('order_id').distinct().count())
    integrity = baskets.agg(F.sum('basket_size').alias('total_items'),
        F.sum(F.when((F.col('basket_size') <= 0) |
                     (F.size(F.array_distinct('items')) != F.col('basket_size')) |
                     F.exists('items', lambda item: item.isNull() | (item <= 0)) |
                     F.col('order_id').isNull() | F.col('user_id').isNull(), 1).otherwise(0)).alias('invalid')).first().asDict()
    report['basket_integrity'] = integrity
    require(integrity['invalid'] == 0 and integrity['total_items'] == expected, 'Basket integrity failed')


def gold_stage(spark, hdfs, report, folder):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, LongType, DoubleType
    orders = spark.read.parquet(hdfs.uri('silver/orders'))
    items = spark.read.parquet(hdfs.uri('silver/order_items'))
    products = spark.read.parquet(hdfs.uri('silver/products'))
    baskets = spark.read.parquet(hdfs.uri('silver/baskets'))
    kpi = orders.agg(F.count('*').alias('total_orders'), F.countDistinct('user_id').alias('total_users'),
                     F.avg('days_since_prior_order').alias('avg_days_between_orders')).first().asDict()
    kpi.update(items.agg(F.count('*').alias('total_order_items'), F.avg('reordered').alias('reorder_rate')).first().asDict())
    kpi.update(baskets.agg(F.count('*').alias('observed_baskets'), F.avg('basket_size').alias('avg_basket_size'),
                           F.expr('percentile(basket_size, 0.5)').alias('median_basket_size'),
                           F.sum('basket_size').alias('basket_item_total')).first().asDict())
    require(kpi['basket_item_total'] == kpi['total_order_items'], 'Gold basket/item reconciliation failed')
    kpi['total_products'] = products.count()
    kpi['avg_orders_per_user'] = kpi['total_orders'] / kpi['total_users']
    kpi['orders_without_observed_items'] = kpi['total_orders'] - kpi['observed_baskets']
    # Nullable mean days is legitimate for very small samples.
    fields = [StructField(name, DoubleType() if isinstance(value, float) or value is None else LongType(), True)
              for name, value in kpi.items()]
    saved, info = hdfs.save(spark.createDataFrame([kpi], StructType(fields)), 'gold/kpi_summary', 1)
    require(saved.first().asDict() == kpi, 'Gold KPI values differ on HDFS readback')
    report['gold'] = {'kpi_summary': info}
    catalog = products.select('product_id', 'product_name', 'department')
    _, info = hdfs.save(catalog, 'gold/product_catalog', kpi['total_products'])
    report['gold']['product_catalog'] = info
    report['kpi'] = kpi
    dump(folder / 'kpi.json', kpi)
    with (folder / 'kpi.csv').open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(kpi))
        writer.writeheader()
        writer.writerow(kpi)


def render_figures(spark, hdfs, report, folder):
    from pyspark.sql import functions as F
    # Reuse the prototype's actual CSV + PNG + HTML chart renderer.
    eda = importlib.import_module('pipeline.04_eda')
    eda.ROOT = folder
    (folder / 'outputs/metrics').mkdir(parents=True, exist_ok=True)
    orders = spark.read.parquet(hdfs.uri('silver/orders'))
    baskets = spark.read.parquet(hdfs.uri('silver/baskets'))
    detail = spark.read.parquet(hdfs.uri('silver/order_items_enriched'))
    specs = [
        ('orders_by_hour', orders.groupBy('order_hour_of_day').count().orderBy('order_hour_of_day'),
         'order_hour_of_day', 'count', 'HDFS sample: Số đơn hàng theo giờ', 'Giờ (0–23)', 'Số đơn', False),
        ('orders_by_dow', orders.groupBy('order_dow').count().orderBy('order_dow'),
         'order_dow', 'count', 'HDFS sample: Số đơn theo mã ngày', 'Mã ngày (0–6)', 'Số đơn', False),
        ('top_20_products', detail.groupBy('product_id', 'product_name').count().orderBy(F.desc('count'), 'product_id').limit(20),
         'product_name', 'count', 'HDFS sample: Top 20 sản phẩm', 'Số lượt mua', 'Sản phẩm', True),
        ('top_departments', detail.groupBy('department_id', 'department').count().orderBy(F.desc('count'), 'department_id').limit(15),
         'department', 'count', 'HDFS sample: Top ngành hàng', 'Số lượt mua', 'Ngành hàng', True),
        ('basket_sizes', baskets.groupBy('basket_size').count().orderBy('basket_size'),
         'basket_size', 'count', 'HDFS sample: Kích thước giỏ hàng', 'Số sản phẩm duy nhất', 'Số giỏ', False),
    ]
    report['charts'] = []
    for name, frame, *arguments in specs:
        # Only a bounded aggregate is exported to the driver, never raw transactions.
        require(frame.limit(501).count() <= 500, f'Chart aggregate too large: {name}')
        report['charts'].append(eda.chart(name, frame.toPandas(), *arguments))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample-fraction', type=float, default=0.01)
    parser.add_argument('--hdfs-base', default='hdfs://namenode:9000/instacart/runs')
    parser.add_argument('--run-id')
    parser.add_argument('--skip-charts', action='store_true')
    args = parser.parse_args()
    sample_threshold(args.sample_fraction)
    # Full data requires an explicit separate capacity plan; this job is a pilot.
    require(args.sample_fraction <= 0.05, 'This ETL pilot permits at most 5%; use 0.01 first')
    base = validate_base(args.hdfs_base)
    run_id = args.run_id or datetime.now(timezone.utc).strftime('hdfs_etl_%Y%m%dT%H%M%SZ_') + uuid4().hex[:8]
    require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', run_id), 'Invalid run-id')
    folder = ROOT / 'outputs/hdfs_runs' / run_id
    folder.mkdir(parents=True, exist_ok=False)
    report = {'run_id': run_id, 'status': 'RUNNING', 'started_at': now(), 'stages': [],
              'hdfs_root': base + '/' + run_id, 'local_output': str(folder),
              'scope': 'ETL: raw, six Bronze tables, five Silver tables, Gold KPI and catalog',
              'model_training': 'NOT_RUN', 'dashboard_integration': 'UNCHANGED',
              'evaluation': 'NOT_RUN; order sample is not an independent ML test',
              'compute': 'Spark local[2] on one Docker host; HDFS one NameNode/one DataNode'}
    report_file = folder / 'report.json'
    spark = None
    started = time.perf_counter()

    def stage(name, action):
        record = {'name': name, 'status': 'RUNNING', 'started_at': now()}
        report['stages'].append(record)
        dump(report_file, report)
        start = time.perf_counter()
        print(f'START {name}', flush=True)
        try:
            result = action()
            record['status'] = 'PASS'
            return result
        except BaseException as error:
            record.update(status='FAIL', error=str(error))
            raise
        finally:
            record.update(seconds=round(time.perf_counter() - start, 3), ended_at=now())
            dump(report_file, report)
            print(f'END {name}: {record["status"]}', flush=True)

    try:
        dump(report_file, report)
        sampling = stage('sample_real_csv', lambda: prepare_sample(ROOT / 'data/raw', folder / 'inputs', args.sample_fraction))
        report['sampling'] = sampling
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.appName('InstacartHdfsETL-' + run_id).master('local[2]')
                 .config('spark.driver.memory', '1g')
                 .config('spark.sql.shuffle.partitions', '8')
                 .config('spark.sql.adaptive.enabled', 'true')
                 .config('spark.hadoop.fs.defaultFS', 'file:///')
                 .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        hdfs = Hdfs(spark, report['hdfs_root'])
        require(not hdfs.exists(''), f'Run already exists on HDFS: {hdfs.base}')
        hdfs.mkdir('')
        report['spark_version'] = spark.version
        report['hadoop_client_version'] = str(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
        stage('raw_to_bronze_hdfs', lambda: bronze_stage(spark, hdfs, folder / 'inputs', sampling, report))
        stage('bronze_to_silver_hdfs', lambda: silver_stage(spark, hdfs, report))
        stage('silver_to_gold_hdfs', lambda: gold_stage(spark, hdfs, report, folder))
        if not args.skip_charts:
            stage('charts_from_hdfs_silver', lambda: render_figures(spark, hdfs, report, folder))
        # Recount every published table from its final URI after the ETL completed.
        def reconcile():
            for layer in ('bronze', 'silver', 'gold'):
                for name, entry in report[layer].items():
                    actual = spark.read.parquet(entry['uri']).count()
                    require(actual == entry['rows'], f'Final reconciliation failed: {layer}/{name}')
                    entry['final_read_rows'] = actual
        stage('final_read_reconciliation', reconcile)
        with (folder / 'row_counts.csv').open('w', newline='', encoding='utf-8-sig') as stream:
            writer = csv.DictWriter(stream, fieldnames=['layer', 'table', 'rows', 'final_read_rows', 'bytes', 'uri'])
            writer.writeheader()
            for layer in ('bronze', 'silver', 'gold'):
                for name, entry in report[layer].items():
                    writer.writerow({'layer': layer, 'table': name,
                                     **{key: entry[key] for key in ['rows', 'final_read_rows', 'bytes', 'uri']}})
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        raise
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                report['spark_stop_warning'] = repr(error)
        report.update(ended_at=now(), seconds=round(time.perf_counter() - started, 3))
        dump(report_file, report)
        with (folder / 'stage_timings.csv').open('w', newline='', encoding='utf-8-sig') as stream:
            writer = csv.DictWriter(stream, fieldnames=['stage', 'status', 'seconds'])
            writer.writeheader()
            for entry in report['stages']:
                writer.writerow({'stage': entry['name'], 'status': entry['status'], 'seconds': entry['seconds']})
        if report['status'] == 'PASS':
            # A separate immutable report per run is authoritative. This pointer
            # only updates after PASS and never changes old prototype manifests.
            dump(ROOT / 'outputs/hdfs_runs/latest_pass.json',
                 {'run_id': run_id, 'status': 'PASS', 'report': str(report_file),
                  'hdfs_root': report['hdfs_root'], 'scope': report['scope']})
        print('HDFS_ETL_STATUS=' + report['status'], flush=True)
        print('REPORT=' + str(report_file), flush=True)


if __name__ == '__main__':
    main()
