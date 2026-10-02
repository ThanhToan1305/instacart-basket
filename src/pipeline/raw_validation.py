"""Schemas, validation and report helpers for the original Instacart CSV files."""
import csv
import json
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

ROOT = Path(__file__).resolve().parents[2]


def schema(*columns):
    types = {'int': IntegerType, 'str': StringType, 'double': DoubleType}
    return StructType([StructField(name, types[kind](), True) for name, kind in columns])


TABLES = {
    'aisles': {'file': 'aisles.csv', 'keys': ['aisle_id'], 'schema': schema(('aisle_id','int'),('aisle','str'))},
    'departments': {'file': 'departments.csv', 'keys': ['department_id'], 'schema': schema(('department_id','int'),('department','str'))},
    'products': {'file': 'products.csv', 'keys': ['product_id'], 'schema': schema(('product_id','int'),('product_name','str'),('aisle_id','int'),('department_id','int'))},
    'orders': {'file': 'orders.csv', 'keys': ['order_id'], 'schema': schema(('order_id','int'),('user_id','int'),('eval_set','str'),('order_number','int'),('order_dow','int'),('order_hour_of_day','int'),('days_since_prior_order','double'))},
    'order_products_prior': {'file': 'order_products__prior.csv', 'keys': ['order_id','product_id'], 'schema': schema(('order_id','int'),('product_id','int'),('add_to_cart_order','int'),('reordered','int'))},
    'order_products_train': {'file': 'order_products__train.csv', 'keys': ['order_id','product_id'], 'schema': schema(('order_id','int'),('product_id','int'),('add_to_cart_order','int'),('reordered','int'))},
}


def inspect_files(raw_dir):
    """Check all six inputs before starting Spark or writing any Bronze table."""
    results = {}
    errors = []
    for name, spec in TABLES.items():
        path = Path(raw_dir) / spec['file']
        if not path.is_file():
            errors.append(f'Missing file: {path}')
            continue
        size = path.stat().st_size
        with path.open(encoding='utf-8-sig', newline='') as stream:
            header = next(csv.reader(stream), [])
        expected = spec['schema'].fieldNames()
        results[name] = {'source_path': str(path.resolve()), 'size_bytes': size,
                         'header': header, 'column_count': len(header),
                         'header_matches': header == expected,
                         'missing_columns': sorted(set(expected) - set(header))}
        if size == 0 or header != expected:
            errors.append(f'Invalid size/header: {path}; bytes={size}; expected={expected}; actual={header}')
    if errors:
        raise ValueError('\n'.join(errors))
    return results


def read_raw(spark, name, raw_dir):
    return (spark.read.schema(TABLES[name]['schema']).option('header', True)
            .option('enforceSchema', False).option('mode', 'FAILFAST')
            .option('multiLine', True).option('escape', '"').option('encoding', 'UTF-8')
            .csv(str(Path(raw_dir) / TABLES[name]['file'])))


def domain_rules(name):
    """Null checks are separate; days_since_prior_order is legitimately nullable."""
    rules = {}
    for field in TABLES[name]['schema']:
        c = F.col(field.name)
        if field.name.endswith('_id') or field.name in ('order_number', 'add_to_cart_order'):
            rules[field.name] = c <= 0
        elif field.name == 'order_dow':
            rules[field.name] = ~c.between(0, 6)
        elif field.name == 'order_hour_of_day':
            rules[field.name] = ~c.between(0, 23)
        elif field.name == 'reordered':
            rules[field.name] = ~c.isin(0, 1)
        elif field.name == 'eval_set':
            rules[field.name] = ~c.isin('prior', 'train', 'test')
        elif field.name == 'days_since_prior_order':
            rules[field.name] = ~c.between(0, 30) | F.isnan(c)
        else:
            rules[field.name] = F.length(F.trim(c)) == 0
    if name == 'orders':
        rules['days_since_prior_order_consistency'] = (
            ((F.col('order_number') == 1) & F.col('days_since_prior_order').isNotNull()) |
            ((F.col('order_number') > 1) & F.col('days_since_prior_order').isNull()))
    return rules


def count_when(expr):
    return F.coalesce(F.sum(F.when(expr, 1).otherwise(0)), F.lit(0))


def profile_frame(df, name):
    columns = df.columns
    rules = domain_rules(name)
    # Only a single aggregate row returns to Python; never materialize source rows.
    aggregate = df.agg(F.count('*').alias('rows'),
        *[count_when(F.col(c).isNull()).alias('null_' + c) for c in columns],
        *[count_when(rule).alias('invalid_' + c) for c, rule in rules.items()]).first().asDict()
    nulls = {c: aggregate['null_' + c] for c in columns}
    invalid = {c: aggregate['invalid_' + c] for c in rules}
    duplicates = (df.groupBy(*TABLES[name]['keys']).count().where(F.col('count') > 1)
                  .agg(F.count('*').alias('groups'),
                       F.coalesce(F.sum(F.col('count') - 1), F.lit(0)).alias('excess')).first())
    required = [c for c in columns if c != 'days_since_prior_order']
    schema_ok = [(f.name, f.dataType) for f in df.schema] == [(f.name, f.dataType) for f in TABLES[name]['schema']]
    checks = {'nonempty': aggregate['rows'] > 0, 'schema': schema_ok,
              'required_nulls': all(nulls[c] == 0 for c in required),
              'duplicate_keys': duplicates['excess'] == 0,
              'domains': all(n == 0 for n in invalid.values())}
    return {'row_count': aggregate['rows'], 'schema': df.schema.jsonValue(),
            'null_counts': nulls, 'key_null_counts': {c: nulls[c] for c in TABLES[name]['keys']},
            'invalid_counts': invalid, 'duplicate_key_groups': duplicates['groups'],
            'duplicate_excess_rows': duplicates['excess'], 'checks': checks,
            'status': 'PASS' if all(checks.values()) else 'FAIL'}


def validate_table(spark, name, raw_dir, metadata):
    start = time.perf_counter()
    df = read_raw(spark, name, raw_dir).persist(StorageLevel.DISK_ONLY)
    try:
        result = {**metadata, **profile_frame(df, name)}
        result['validation_seconds'] = round(time.perf_counter() - start, 3)
        return result
    finally:
        df.unpersist()


def write_reports(report):
    folder = ROOT / 'outputs/metrics'
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'raw_profile.json').write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
    fields = ['table','status','source_path','size_bytes','column_count','row_count','schema','null_counts','key_null_counts',
              'duplicate_key_groups','duplicate_excess_rows','invalid_counts','checks','validation_seconds',
              'bronze_path','bronze_rows','final_read_rows','bronze_size_bytes','ingestion_seconds','reconciliation','bronze_schema_matches','snappy','metadata_null_rows']
    with (folder / 'raw_profile.csv').open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for name, profile in report['tables'].items():
            row = {'table': name, **{k: profile.get(k, '') for k in fields if k != 'table'}}
            for k, v in row.items():
                if isinstance(v, (dict, list)):
                    row[k] = json.dumps(v, ensure_ascii=False)
            writer.writerow(row)
