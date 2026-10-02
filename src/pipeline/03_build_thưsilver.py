"""Build five validated Silver tables from the six existing Bronze tables."""
import sys
import time
from pathlib import Path
from uuid import uuid4
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import functions as F
from common.spark_session import create_spark_session
from common.logger import get_logger
from pipeline.raw_validation import TABLES, profile_frame
from pipeline.silver_helpers import ROOT, stage, require, write_json, normalized, foreign_key_check, directory_bytes


def main():
    logger = get_logger('eda_pipeline', ROOT / 'logs/eda_pipeline.log')
    report = {'status': 'RUNNING', 'tables': {}, 'bronze_validation': {}, 'foreign_keys': {}}
    start = time.perf_counter()
    spark = None
    destinations = ['orders', 'order_items', 'products', 'order_items_enriched', 'baskets']
    try:
        for name in TABLES:
            require((ROOT / 'data/bronze' / name / '_SUCCESS').is_file(), f'Missing or incomplete Bronze: {name}')
        for name in destinations:
            require(not (ROOT / 'data/silver' / name).exists(), f'Existing Silver: {name}; refusing to overwrite')
        spark = create_spark_session('InstacartSilver')
        tables = {}
        with stage('bronze_validation_and_normalization', report, logger):
            for name, spec in TABLES.items():
                source = spark.read.parquet(str(ROOT / 'data/bronze' / name))
                require(all(c in source.columns for c in spec['schema'].fieldNames()), f'Missing columns: {name}')
                df = normalized(source, name)
                profile = profile_frame(df, name)
                # Duplicate keys are cleaned below; other invalid data must not disappear silently.
                require(all(v for k,v in profile['checks'].items() if k != 'duplicate_keys'), f'Invalid Bronze data: {name}: {profile}')
                # Avoid repeatedly shuffling tens of millions of rows when validation
                # already proved key uniqueness. Deduplicate only when necessary.
                clean = df.dropDuplicates(spec['keys']) if profile['duplicate_excess_rows'] else df
                clean_rows = clean.count() if profile['duplicate_excess_rows'] else profile['row_count']
                report['bronze_validation'][name] = {**profile, 'deduplicated_rows': clean_rows,
                    'removed_duplicates': profile['row_count'] - clean_rows}
                tables[name] = clean
                logger.info('Bronze %s rows=%s clean=%s', name, profile['row_count'], clean_rows)
        with stage('foreign_keys', report, logger):
            checks = {
                'products_aisle': foreign_key_check(tables['products'], tables['aisles'], 'aisle_id'),
                'products_department': foreign_key_check(tables['products'], tables['departments'], 'department_id'),
            }
            for name in ('order_products_prior','order_products_train'):
                checks[name + '_order'] = foreign_key_check(tables[name], tables['orders'], 'order_id')
                checks[name + '_product'] = foreign_key_check(tables[name], tables['products'], 'product_id')
                expected_set = name.removeprefix('order_products_')
                checks[name + '_eval_set'] = tables[name].join(tables['orders'].select('order_id','eval_set'), 'order_id').where(F.col('eval_set') != expected_set).count()
            report['foreign_keys'] = checks
            require(all(v == 0 for v in checks.values()), f'Orphan/mismatched keys: {checks}')
        staging = ROOT / 'data/silver' / ('.staging_' + uuid4().hex)
        def save(name, df, expected_rows):
            with stage('write_' + name, report, logger):
                path = staging / name
                df.write.mode('errorifexists').option('compression','snappy').parquet(str(path))
                saved = spark.read.parquet(str(path))
                rows = saved.count()
                require(rows == expected_rows, f'Row loss in {name}: {rows} != {expected_rows}')
                report['tables'][name] = {'rows': rows, 'columns': saved.columns,
                    'size_bytes': directory_bytes(path), 'path': str(ROOT / 'data/silver' / name), 'status': 'PASS'}
                return saved
        orders = save('orders', tables['orders'], report['bronze_validation']['orders']['deduplicated_rows'])
        products = (tables['products'].join(F.broadcast(tables['aisles']), 'aisle_id')
                    .join(F.broadcast(tables['departments']), 'department_id')
                    .select('product_id','product_name','aisle_id','aisle','department_id','department'))
        products = save('products', products, report['bronze_validation']['products']['deduplicated_rows'])
        items = (tables['order_products_prior'].withColumn('eval_set', F.lit('prior'))
                 .unionByName(tables['order_products_train'].withColumn('eval_set', F.lit('train')))
                 .dropDuplicates(['order_id','product_id']))
        expected_items = sum(report['bronze_validation'][n]['deduplicated_rows'] for n in ('order_products_prior','order_products_train'))
        items = save('order_items', items, expected_items)
        enriched = (items.join(orders.drop('eval_set'), 'order_id')
                    .join(F.broadcast(products), 'product_id')
                    .select('order_id','user_id','product_id','product_name','aisle_id','aisle','department_id','department',
                            'eval_set','order_number','order_dow','order_hour_of_day','days_since_prior_order','add_to_cart_order','reordered'))
        enriched = save('order_items_enriched', enriched, expected_items)
        baskets = (items.join(orders.select('order_id','user_id'), 'order_id')
                   .groupBy('order_id','user_id').agg(F.sort_array(F.collect_set('product_id')).alias('items'))
                   .withColumn('basket_size', F.size('items')))
        observed_orders = items.select('order_id').distinct().count()
        baskets = save('baskets', baskets, observed_orders)
        with stage('basket_integrity', report, logger):
            basket_metrics = baskets.agg(F.sum('basket_size').alias('total_items'),
                F.sum(F.when((F.col('basket_size') <= 0) | (F.size(F.array_distinct('items')) != F.col('basket_size')) |
                             F.col('order_id').isNull() | F.col('user_id').isNull(), 1).otherwise(0)).alias('invalid')).first()
            require(basket_metrics['total_items'] == expected_items and basket_metrics['invalid'] == 0, 'Invalid baskets')
            report['basket_integrity'] = basket_metrics.asDict()
            report['orders_without_observed_items'] = orders.join(baskets.select('order_id'), 'order_id', 'left_anti').groupBy('eval_set').count().orderBy('eval_set').toPandas().to_dict('records')
            # Only the <=3 aggregate eval_set rows cross to Pandas.
        for name in destinations:
            (staging / name).rename(ROOT / 'data/silver' / name)
        staging.rmdir()
        with stage('final_read_reconciliation', report, logger):
            for name, profile in report['tables'].items():
                count = spark.read.parquet(profile['path']).count()
                require(count == profile['rows'], f'Final Silver read mismatch: {name}')
                profile['final_read_rows'] = count
        report['status'] = 'PASS'
    except Exception as exc:
        report.update(status='FAIL', error=str(exc))
        logger.exception('Silver pipeline failed')
        raise
    finally:
        if spark is not None:
            spark.stop()
        report['total_seconds'] = round(time.perf_counter() - start, 3)
        write_json(ROOT / 'outputs/metrics/silver_profile.json', report)
        logger.info('Silver %s total_seconds=%s', report['status'], report['total_seconds'])


if __name__ == '__main__':
    main()
