"""Verified small Gold export, isolated serving bundle; never exports Silver rows."""
import argparse
import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

from hdfs_pipeline.helpers import ROOT, Hdfs, require, dump, now, valid_id, schema_signature
from hdfs_pipeline.publish import publication_contract, checked_read, validate_gold
from app.serving import MAX_ROWS, sha256, verify_bundle


def export_contract(publication, model, etl):
    publication_contract(model, etl, publication['model_run_id'], publication['min_confidence'])
    require(publication['status'] == 'PASS', 'Publication must be PASS')
    fraction = etl.get('sampling', {}).get('requested_fraction')
    require(isinstance(fraction, (int, float)) and 0 < fraction <= .05,
            'This serving exporter supports the verified ETL sample pilot only')
    valid_id(publication['publication_id'])
    require(publication['etl_run_id'] == model['etl_run_id'] and
            publication['model_root'] == model['model_root'] and publication['etl_root'] == model['etl_root'],
            'Publication lineage mismatch')
    for name, entry in publication['gold'].items():
        require(entry['status'] == 'PASS' and entry['uri'] == model['model_root'] + '/gold/' +
                publication['publication_id'] + '/' + name, 'Gold URI lineage mismatch')
        require(entry['rows'] == publication['counts'][name], 'Publication count mismatch')
    expected = model['model_root'] + '/gold/' + publication['publication_id'] + '/publication_manifest.json'
    require(publication['hdfs_manifest_uri'] == expected, 'Commit marker URI mismatch')
    require(etl['gold']['kpi_summary']['uri'] == model['etl_root'] + '/gold/kpi_summary', 'KPI lineage mismatch')


def compare_frames(source, local, keys):
    from pyspark.sql import functions as F
    require(schema_signature(source.schema.jsonValue()) == schema_signature(local.schema.jsonValue()),
            'HDFS/local schema mismatch')
    require(source.count() == local.count(), 'HDFS/local count mismatch')
    if not keys:
        require(source.count() == 1 and source.first().asDict() == local.first().asDict(), 'KPI values mismatch')
        return
    for frame in (source, local):
        require(frame.select(*keys).distinct().count() == frame.count(), 'Duplicate export keys')
    condition = F.col('h.' + keys[0]) == F.col('l.' + keys[0])
    for key in keys[1:]:
        condition = condition & (F.col('h.' + key) == F.col('l.' + key))
    joined = source.alias('h').join(local.alias('l'), condition, 'full')
    invalid = F.col('h.' + keys[0]).isNull() | F.col('l.' + keys[0]).isNull()
    for column in source.columns:
        invalid = invalid | ~F.col('h.' + column).eqNullSafe(F.col('l.' + column))
    require(joined.where(invalid).count() == 0, 'HDFS/local values mismatch')


def export_table(hdfs, entry, target, keys):
    import pyarrow.parquet as pq
    require(0 <= entry['rows'] <= MAX_ROWS, 'Table exceeds 100000 rows; use a separately designed filtered/paged loader')
    source = checked_read(hdfs, entry)
    require(not target.exists(), 'Local table already exists; refusing overwrite')
    target.parent.mkdir(parents=True, exist_ok=True)
    hdfs.fs.copyToLocalFile(False, hdfs.jpath(entry['uri']), hdfs.jpath(target.resolve().as_uri()), True)
    local = hdfs.spark.read.parquet(target.resolve().as_uri())
    compare_frames(source, local, keys)
    arrow = pq.ParquetDataset(target)
    rows = sum(pq.read_metadata(fragment.path).num_rows for fragment in arrow.fragments)
    require(rows == entry['rows'] and rows <= MAX_ROWS, 'PyArrow export count mismatch/limit')
    return dict(entry, arrow_schema=str(arrow.schema.remove_metadata()), reconciliation='PASS; full keyed value comparison')


def aggregates(spark, etl, folder):
    from pyspark.sql import functions as F
    source = Hdfs(spark, etl['hdfs_root'])
    orders = checked_read(source, etl['silver']['orders'])
    baskets = checked_read(source, etl['silver']['baskets'])
    detail = checked_read(source, etl['silver']['order_items_enriched'])
    specs = {
        'orders_by_hour': orders.groupBy('order_hour_of_day').count().orderBy('order_hour_of_day'),
        'orders_by_dow': orders.groupBy('order_dow').count().orderBy('order_dow'),
        'top_20_products': detail.groupBy('product_id', 'product_name').count().orderBy(F.desc('count'), 'product_id').limit(20),
        'top_15_departments': detail.groupBy('department_id', 'department').count().orderBy(F.desc('count'), 'department_id').limit(15),
        'basket_size_distribution': baskets.groupBy('basket_size').count().orderBy('basket_size'),
        'reorder_by_department': detail.groupBy('department_id', 'department').agg(F.avg('reordered').alias('reorder_rate')).orderBy('department_id')}
    results = {}
    for name, frame in specs.items():
        require(frame.limit(501).count() <= 500, 'Aggregate exceeds 500 rows: ' + name)
        frame.toPandas().to_csv(folder / (name + '.csv'), index=False)
        results[name] = {'rows': frame.count(), 'source': etl['hdfs_root'], 'bounded_maximum': 500}
    return results


def demo_checks(stage_root, publication_dir):
    import pandas as pd
    import pyarrow.parquet as pq
    from app.recommender import recommend
    def read(name):
        dataset = pq.ParquetDataset(stage_root / 'data/gold' / name)
        require(sum(pq.read_metadata(f.path).num_rows for f in dataset.fragments) <= MAX_ROWS, 'Demo table too large')
        return dataset.read().to_pandas()
    catalog, lookup = read('product_catalog'), read('recommendation_lookup')
    popular = pd.read_csv(stage_root / 'outputs/metrics/top_20_products.csv')
    cases = json.loads((publication_dir / 'integration_cases.json').read_text(encoding='utf-8'))
    selected_case = next((case for case in cases if case['recommendations']), None)
    records, demos = [], []
    if selected_case:
        basket = selected_case['context']
        result = recommend(basket, lookup, popular, catalog)
        require(result.product_id.tolist() == selected_case['recommendations'], 'Export recommendation differs from publication')
        demos.append({'label': 'Luật • order_id=' + str(selected_case['order_id']), 'context': basket})
        records.append({'case': 'real_rule_basket', 'context': basket, 'products': result.product_id.tolist(), 'source': 'Luật kết hợp'})
    antecedent_ids = {int(x) for items in lookup.antecedent for x in items}
    fallback_id = next((int(x) for x in catalog.product_id if int(x) not in antecedent_ids), None)
    if fallback_id is not None:
        result = recommend([fallback_id], lookup, popular, catalog)
        require(not result.empty and (result.source == 'Fallback phổ biến').all(), 'Expected popularity fallback')
        require(result[['support', 'confidence', 'lift']].isna().all().all(), 'Invented fallback metrics')
        require(fallback_id not in set(result.product_id) and result.product_id.nunique() == len(result) <= 5, 'Invalid fallback products')
        demos.append({'label': 'Fallback • product_id=' + str(fallback_id), 'context': [fallback_id]})
        records.append({'case': 'real_catalog_fallback', 'context': [fallback_id], 'products': result.product_id.tolist(),
                        'source': 'Fallback phổ biến', 'frequency_source': 'ETL observed prior+train item counts; not independent training popularity'})
    dump(stage_root / 'outputs/metrics/demo_cases.json', {'cases': demos, 'checks': records})
    return {'status': 'PASS', 'rule_case': selected_case is not None, 'fallback_case': fallback_id is not None,
            'scope': 'functional checks only; not evaluation'}


def run_worker(job):
    from pyspark.sql import SparkSession
    import pyarrow.parquet as pq
    report_path = job / 'report.json'
    report = json.loads(report_path.read_text(encoding='utf-8'))
    pub_path = Path(report['publication_manifest'])
    pub = json.loads(pub_path.read_text(encoding='utf-8'))
    model_path = ROOT / 'outputs/hdfs_models' / pub['model_run_id'] / 'report.json'
    model = json.loads(model_path.read_text(encoding='utf-8'))
    etl_path = ROOT / 'outputs/hdfs_runs' / pub['etl_run_id'] / 'report.json'
    etl = json.loads(etl_path.read_text(encoding='utf-8'))
    stage_root, final_root = Path(report['staging']), Path(report['serving_dir'])
    spark = None
    tick = time.perf_counter()

    def stage(name, action):
        entry = {'name': name, 'status': 'RUNNING', 'started_at': now()}
        report['stages'].append(entry)
        dump(report_path, report)
        start = time.perf_counter()
        try:
            value = action()
            entry['status'] = 'PASS'
            return value
        except BaseException as error:
            entry.update(status='FAIL', error=repr(error))
            raise
        finally:
            entry['seconds'] = round(time.perf_counter() - start, 3)
            dump(report_path, report)

    try:
        export_contract(pub, model, etl)
        require(sha256(model_path) == pub['model_report_sha256'] and sha256(etl_path) == pub['etl_report_sha256'], 'Source report fingerprint changed')
        require(not stage_root.exists() and not final_root.exists(), 'Export destination exists; use a new export ID')
        stage_root.mkdir(parents=True, exist_ok=False)
        metric_dir = stage_root / 'outputs/metrics'
        metric_dir.mkdir(parents=True)
        spark = (SparkSession.builder.master('local[2]').appName('InstacartHdfsExport-' + report['export_id'])
                 .config('spark.driver.memory', '1g').config('spark.task.cpus', '1')
                 .config('spark.sql.shuffle.partitions', '8').config('spark.sql.adaptive.enabled', 'false')
                 .config('spark.hadoop.fs.defaultFS', 'file:///')
                 .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        model_fs, etl_fs = Hdfs(spark, model['model_root']), Hdfs(spark, etl['hdfs_root'])

        def verify_commit():
            suffix = 'gold/' + pub['publication_id'] + '/publication_manifest.json'
            require(model_fs.exists(suffix) and model_fs.size(suffix) <= 2_000_000, 'Missing/oversized HDFS commit manifest')
            stream = model_fs.fs.open(model_fs.jpath(model_fs.uri(suffix)))
            try:
                remote = json.loads(str(spark._jvm.org.apache.commons.io.IOUtils.toString(stream, 'UTF-8')))
            finally:
                stream.close()
            for key in ('status', 'model_run_id', 'etl_run_id', 'publication_id', 'gold', 'counts', 'min_confidence',
                        'model_report_sha256', 'etl_report_sha256'):
                require(remote[key] == pub[key], 'HDFS/local publication manifest mismatch: ' + key)
        stage('verify_hdfs_commit', verify_commit)
        entries = {'product_catalog': etl['gold']['product_catalog'], 'kpi_summary': etl['gold']['kpi_summary'],
                   'association_rules': pub['gold']['association_rules'], 'recommendation_lookup': pub['gold']['recommendation_lookup']}
        table_info = {}
        keys = {'product_catalog': ['product_id'], 'kpi_summary': [], 'association_rules': ['rule_id'],
                'recommendation_lookup': ['rule_id', 'antecedent_product_id']}
        for name, entry in entries.items():
            fs = etl_fs if name in ('product_catalog', 'kpi_summary') else model_fs
            table_info[name] = stage('export_' + name, lambda name=name, entry=entry, fs=fs:
                                    export_table(fs, entry, stage_root / 'data/gold' / name, keys[name]))
        def validate_local_gold():
            local = {name: spark.read.parquet((stage_root / 'data/gold' / name).as_uri()) for name in entries}
            itemsets = checked_read(model_fs, pub['gold']['frequent_itemsets'])
            return validate_gold(itemsets, local['association_rules'], local['recommendation_lookup'],
                                 local['product_catalog'].select('product_id', 'product_name'), model['counts']['training_baskets'], pub['min_confidence'])
        report['gold_checks'] = stage('validate_local_gold', validate_local_gold)
        aggregate_info = stage('silver_hdfs_aggregates', lambda: aggregates(spark, etl, metric_dir))
        kpi = pq.read_table(stage_root / 'data/gold/kpi_summary').to_pylist()[0]
        dump(metric_dir / 'eda_summary.json', {'status': 'PASS', 'kpi': kpi, 'etl_run_id': pub['etl_run_id']})
        dump(metric_dir / 'model_summary.json', {'status': 'PASS', 'counts': pub['counts'], 'config': model['config'],
                                                'evaluation_status': 'NOT_EVALUATED'})
        report['integration'] = stage('local_recommendation_and_fallback', lambda: demo_checks(stage_root, pub_path.parent))
        timings = []
        for label, source in (('ETL', etl), ('Model pilot', model), ('Publication', pub), ('Export', report)):
            timings.extend({'Run': label, 'Stage': s['name'], 'Giây': s.get('seconds'), 'Trạng thái': s['status']}
                           for s in source['stages'] if s['status'] != 'RUNNING')
        counts = [{'Bảng': name, 'Dòng': info['rows'], 'URI': info['uri']} for name, info in table_info.items()]
        dump(metric_dir / 'hdfs_system.json', {'timings': timings, 'counts': counts})
        import pandas as pd
        pd.DataFrame(timings).to_csv(metric_dir / 'hdfs_stage_timings.csv', index=False)
        pd.DataFrame(counts).to_csv(metric_dir / 'hdfs_export_counts.csv', index=False)
        manifest = {'status': 'PASS', 'export_id': report['export_id'], 'created_at': now(),
                    'etl_run_id': pub['etl_run_id'], 'model_run_id': pub['model_run_id'],
                    'publication_id': pub['publication_id'], 'etl_root': pub['etl_root'],
                    'publication_manifest_uri': pub['hdfs_manifest_uri'], 'publication_report_sha256': sha256(pub_path),
                    'mode': 'Gold export từ HDFS', 'execution_mode': 'SAMPLE', 'evaluation_status': 'NOT_EVALUATED',
                    'model_config': model['config'], 'publication_min_confidence': pub['min_confidence'],
                    'compute': 'Spark local[2], task.cpus=1, driver=1g; HDFS on one Docker host',
                    'tables': table_info, 'aggregates': aggregate_info, 'integration': report['integration'],
                    'checksum_definition': 'SHA-256 of local copied files; logical rows compared separately via Spark keyed full join',
                    'files': {file.relative_to(stage_root).as_posix(): sha256(file)
                              for file in sorted(stage_root.rglob('*')) if file.is_file()}}
        dump(stage_root / 'serving_manifest.json', manifest)
        stage('verify_complete_bundle', lambda: verify_bundle(stage_root))
        stage_root.rename(final_root)
        stage('verify_final_bundle', lambda: verify_bundle(final_root))
        report.update(status='PASS', tables=table_info, serving_manifest=str(final_root / 'serving_manifest.json'))
        dump(ROOT / 'outputs/hdfs_serving/latest_pass.json', {
            'status': 'PASS', 'export_id': report['export_id'], 'model_run_id': pub['model_run_id'],
            'publication_id': pub['publication_id'], 'serving_dir': str(final_root.relative_to(ROOT)),
            'serving_manifest': str((final_root / 'serving_manifest.json').relative_to(ROOT))})
    except BaseException as error:
        report.update(status='FAIL', error=repr(error), recovery='Preserve incomplete export; select a new export ID')
        raise
    finally:
        report.update(seconds=round(time.perf_counter() - tick, 3), ended_at=now())
        dump(report_path, report)
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication-manifest')
    parser.add_argument('--export-id')
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        run_worker(Path(args.worker))
        return 0
    require(args.publication_manifest, '--publication-manifest is required')
    pub_path = Path(args.publication_manifest).resolve()
    pub = json.loads(pub_path.read_text(encoding='utf-8'))
    model_id = valid_id(pub['model_run_id'])
    valid_id(pub['etl_run_id'])
    valid_id(pub['publication_id'])
    require(pub['status'] == 'PASS', 'Publication must be PASS')
    export_id = valid_id(args.export_id or datetime.now(timezone.utc).strftime('export_%Y%m%dT%H%M%SZ_') + uuid4().hex[:8])
    model_dir = ROOT / 'outputs/hdfs_models' / model_id
    job = model_dir / 'exports' / export_id
    job.mkdir(parents=True, exist_ok=False)
    dump(job / 'report.json', {'status': 'RUNNING', 'stages': [], 'started_at': now(), 'export_id': export_id,
                               'publication_manifest': str(pub_path), 'model_run_id': model_id,
                               'publication_id': pub['publication_id'], 'etl_run_id': pub['etl_run_id'],
                               'staging': str(model_dir / 'serving' / ('.staging_' + export_id)),
                               'serving_dir': str(model_dir / 'serving' / export_id)})
    from hdfs_pipeline.train import supervise
    code = supervise(job, 1200, module='hdfs_pipeline.export_dashboard')
    print('REPORT=' + str(job / 'report.json'), flush=True)
    return code


if __name__ == '__main__':
    sys.exit(main())
