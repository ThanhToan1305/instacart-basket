"""Pilot FP-Growth on verified HDFS Silver; Linux/Docker isolated worker."""
import argparse
import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

from hdfs_pipeline.helpers import ROOT, Hdfs, dump, now, require, source_manifest, valid_id


def validate_config(cfg):
    for key in ('min_support', 'min_confidence'):
        require(isinstance(cfg[key], (int, float)) and math.isfinite(cfg[key]) and 0 < cfg[key] <= 1,
                'Invalid ' + key)
    for key in ('num_partitions', 'max_itemsets', 'max_rules', 'worker_timeout_seconds'):
        require(type(cfg[key]) is int and cfg[key] > 0, 'Invalid ' + key)


def prepare_training(baskets, orders, allowed_users=None):
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType
    from pipeline.fpgrowth_helpers import basket_invalid
    for frame in (baskets, orders):
        for key in ('order_id', 'user_id'):
            require(isinstance(frame.schema[key].dataType, IntegerType), 'Expected integer ' + key)
            require(frame.where(F.col(key).isNull() | (F.col(key) <= 0)).limit(1).count() == 0,
                    'Invalid ' + key)
        require(frame.groupBy('order_id').count().where('count > 1').limit(1).count() == 0,
                'Duplicate order_id')
    require(basket_invalid(baskets) == 0, 'Invalid source baskets')
    require(baskets.where(F.col('basket_size').isNull() | (F.col('basket_size') != F.size('items')))
            .limit(1).count() == 0, 'basket_size differs from items')
    require(orders.where(F.col('eval_set').isNull() | ~F.col('eval_set').isin('prior', 'train', 'test'))
            .limit(1).count() == 0, 'Invalid eval_set')
    keys = orders.select('order_id', 'user_id', 'eval_set')
    require(baskets.join(keys, ['order_id', 'user_id'], 'left_anti').limit(1).count() == 0,
            'Basket order/user foreign key mismatch')
    prior = baskets.join(keys, ['order_id', 'user_id']).where("eval_set = 'prior'")
    if allowed_users is not None:
        prior = prior.join(allowed_users.select('user_id').distinct(), 'user_id', 'left_semi')
    train = prior.where(F.size('items') >= 2).select('order_id', 'user_id', 'items')
    counts = {'source_baskets': baskets.count(), 'source_orders': orders.count(),
              'prior_baskets': prior.count(), 'training_baskets': train.count()}
    counts['excluded_single_item_baskets'] = counts['prior_baskets'] - counts['training_baskets']
    require(counts['training_baskets'] > 0 and basket_invalid(train, 2) == 0,
            'No valid eligible prior baskets')
    return train, counts


def fit_artifacts(train, cfg):
    """One fit per support, reused by pilot and independent refit workers."""
    from pyspark.ml.fpm import FPGrowth
    from pyspark import StorageLevel
    model = FPGrowth(itemsCol='items', minSupport=cfg['min_support'],
                     minConfidence=cfg['min_confidence'], numPartitions=cfg['num_partitions']).fit(train)
    itemsets = model.freqItemsets.persist(StorageLevel.DISK_ONLY)
    n = itemsets.limit(cfg['max_itemsets'] + 1).count()
    require(n <= cfg['max_itemsets'], 'Itemset safety cap exceeded')
    rules = model.associationRules.persist(StorageLevel.DISK_ONLY)
    r = rules.limit(cfg['max_rules'] + 1).count()
    require(r <= cfg['max_rules'], 'Rule safety cap exceeded')
    return itemsets, rules, {'frequent_itemsets': n, 'rules': r}


def run_worker(folder):
    from pyspark.sql import SparkSession, functions as F
    from pyspark.ml.fpm import FPGrowth
    from pyspark import StorageLevel
    report_file = folder / 'report.json'
    report = json.loads(report_file.read_text(encoding='utf-8'))
    cfg = report['config']
    spark = None
    started = time.perf_counter()

    def stage(name, action):
        record = {'name': name, 'status': 'RUNNING', 'started_at': now()}
        report['stages'].append(record)
        dump(report_file, report)
        tick = time.perf_counter()
        try:
            result = action()
            record['status'] = 'PASS'
            return result
        except BaseException as error:
            record.update(status='FAIL', error=repr(error))
            raise
        finally:
            record.update(seconds=round(time.perf_counter() - tick, 3), ended_at=now())
            dump(report_file, report)

    try:
        spark = (SparkSession.builder.master('local[2]').appName('InstacartHdfsModel-' + report['model_run_id'])
                 .config('spark.driver.memory', '1g').config('spark.task.cpus', '1')
                 .config('spark.sql.shuffle.partitions', str(cfg['num_partitions']))
                 .config('spark.hadoop.fs.defaultFS', 'file:///')
                 .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        report.update(spark_version=spark.version,
                      hadoop_client_version=str(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()),
                      python_version=sys.version, compute='Spark local[2], task.cpus=1, driver=1g; one Docker host')
        source = Hdfs(spark, report['etl_root'])
        target = Hdfs(spark, report['model_root'])
        require(not target.exists(''), 'Model namespace already exists; refusing overwrite')

        def verify_source():
            frames = {}
            for name, entry in report['inputs'].items():
                require(source.exists('silver/' + name + '/_SUCCESS'), 'Missing Silver _SUCCESS: ' + name)
                frame = spark.read.parquet(entry['uri'])
                from hdfs_pipeline.helpers import schema_signature
                require(schema_signature(frame.schema.jsonValue()) == schema_signature(entry['schema']),
                        'Silver schema mismatch: ' + name)
                require(frame.count() == entry['rows'], 'Silver count mismatch: ' + name)
                frames[name] = frame
            return frames

        frames = stage('verify_hdfs_silver', verify_source)
        train, counts = stage('validate_prior_baskets', lambda: prepare_training(frames['baskets'], frames['orders']))
        report['counts'] = counts
        target.mkdir('')
        report['artifacts'] = {}

        def save(name, frame, count):
            saved, entry = target.save(frame, 'artifacts/' + name, count)
            # All generated columns are mandatory; Parquet metadata nullability is ignored.
            invalid = None
            for column in saved.columns:
                term = F.col(column).isNull()
                invalid = term if invalid is None else invalid | term
            require(saved.where(invalid).limit(1).count() == 0, 'Null artifact value: ' + name)
            report['artifacts'][name] = entry
            return saved

        train = stage('save_training_baskets', lambda: save('training_baskets',
                      train.repartition(cfg['num_partitions']), counts['training_baskets']))

        def fit():
            itemsets, rules, counts = fit_artifacts(train, cfg)
            report['counts'].update(counts)
            return itemsets, rules

        itemsets, rules = stage('fit_pilot_fpgrowth', fit)
        stage('save_frequent_itemsets', lambda: save('frequent_itemsets', itemsets, counts['frequent_itemsets']))
        stage('save_rules', lambda: save('rules', rules, counts['rules']))
        # Re-read every final URI after all writes, rather than trusting cached frames.
        def reconcile():
            from pipeline.fpgrowth_helpers import basket_invalid
            for name, entry in report['artifacts'].items():
                require(spark.read.parquet(entry['uri']).count() == entry['rows'], 'Final count mismatch: ' + name)
            require(basket_invalid(spark.read.parquet(report['artifacts']['training_baskets']['uri']), 2) == 0,
                    'Invalid training readback')
        stage('final_read_reconciliation', reconcile)
        report['status'] = 'PASS'
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        raise
    finally:
        report.update(ended_at=now(), seconds=round(time.perf_counter() - started, 3))
        dump(report_file, report)
        if spark is not None:
            spark.stop()


def supervise(folder, timeout, module='hdfs_pipeline.train'):
    report_file = folder / 'report.json'
    reason = None
    with (folder / 'worker.log').open('w', encoding='utf-8') as output:
        process = subprocess.Popen([sys.executable, '-u', '-m', module, '--worker', str(folder)],
                                   stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                with (folder / 'worker.log').open('rb') as stream:
                    stream.seek(max(0, (folder / 'worker.log').stat().st_size - 65536))
                    tail = stream.read().decode('utf-8', errors='replace')
                if 'OutOfMemoryError' in tail or 'Java heap space' in tail:
                    reason = 'OUT_OF_MEMORY'
                    break
                if time.monotonic() >= deadline:
                    reason = 'TIMEOUT'
                    break
                time.sleep(1)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
    report = json.loads(report_file.read_text(encoding='utf-8'))
    if reason or process.returncode != 0 or report['status'] != 'PASS':
        report.update(status='FAIL', failure_reason=reason or 'WORKER_FAILED', exit_code=process.returncode, ended_at=now())
        for stage in report['stages']:
            if stage['status'] == 'RUNNING':
                stage.update(status='FAIL', error=report['failure_reason'])
        dump(report_file, report)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--etl-run-id')
    parser.add_argument('--hdfs-root')
    parser.add_argument('--model-run-id')
    parser.add_argument('--config', default=str(ROOT / 'config/hdfs_model.yaml'))
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        run_worker(__import__('pathlib').Path(args.worker))
        return 0
    require(args.etl_run_id, '--etl-run-id is required')
    etl_id = valid_id(args.etl_run_id)
    etl_path = ROOT / 'outputs/hdfs_runs' / etl_id / 'report.json'
    etl = json.loads(etl_path.read_text(encoding='utf-8'))
    base = source_manifest(etl, etl_id, args.hdfs_root)
    import yaml
    with open(args.config, encoding='utf-8') as stream:
        cfg = yaml.safe_load(stream)['fpgrowth']
    validate_config(cfg)
    model_id = valid_id(args.model_run_id or datetime.now(timezone.utc).strftime('hdfs_model_%Y%m%dT%H%M%SZ_') + uuid4().hex[:8])
    folder = ROOT / 'outputs/hdfs_models' / model_id
    folder.mkdir(parents=True, exist_ok=False)
    dump(folder / 'run_config.json', cfg)
    dump(folder / 'report.json', {
        'status': 'RUNNING', 'started_at': now(), 'etl_run_id': etl_id, 'model_run_id': model_id,
        'etl_root': base, 'model_root': base + '/models/' + model_id,
        'etl_report_sha256': hashlib.sha256(etl_path.read_bytes()).hexdigest(),
        'inputs': {name: etl['silver'][name] for name in ('baskets', 'orders')},
        'config': cfg, 'stages': [], 'scope': 'sample pilot integration; not optimized; no independent evaluation',
        'sampling': etl.get('sampling', {}).get('rule'),
        'resource_limits': 'isolated process group; timeout/OOM monitor; post-fit caps cannot prevent fit OOM',
        'worker_log': str(folder / 'worker.log')})
    code = supervise(folder, cfg['worker_timeout_seconds'])
    print('REPORT=' + str(folder / 'report.json'), flush=True)
    return code


if __name__ == '__main__':
    sys.exit(main())
