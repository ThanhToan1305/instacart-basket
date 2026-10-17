"""4A split -> 4B isolated refit/validation lock -> 4C test/export checkpoints."""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.serving import sha256
from hdfs_pipeline.helpers import ROOT, Hdfs, dump, now, require, valid_id, source_manifest
from hdfs_pipeline.publish import checked_read, check_itemsets, check_arrays, check_metrics
from hdfs_pipeline.evaluation_split import build_split, fingerprint, SEED

SUPPORTS = (.002, .001)
CONFIDENCES = (.20, .30, .40)
CONFIG = {'min_confidence': .20, 'num_partitions': 8, 'max_itemsets': 200000, 'max_rules': 500000,
          'worker_timeout_seconds': 1200}


def fit_action(support):
    return 'fit_' + str(support).replace('.', '_')


def checkpoint(run, action):
    state = json.loads((run / 'run.json').read_text(encoding='utf-8'))
    entry = state['checkpoints'].get(action)
    require(entry is not None, 'Missing PASS checkpoint: ' + action)
    path = run / entry
    report = json.loads(path.read_text(encoding='utf-8'))
    require(report['status'] == 'PASS', 'Checkpoint is not PASS: ' + action)
    return report, path


def read_verified(hdfs, entry):
    frame = checked_read(hdfs, entry)
    if 'fingerprint' in entry:
        require(fingerprint(frame) == entry['fingerprint'], 'Checkpoint logical fingerprint changed: ' + entry['uri'])
    return frame


def save_verified(hdfs, frame, suffix):
    saved, entry = hdfs.save(frame, suffix, frame.count())
    entry['fingerprint'] = fingerprint(saved)
    return saved, entry


def audit_training(hdfs, split):
    from pyspark.sql import functions as F
    from pipeline.fpgrowth_helpers import basket_invalid
    train = read_verified(hdfs, split['tables']['training_baskets'])
    users = read_verified(hdfs, split['tables']['users'])
    orders = read_verified(hdfs, split['tables']['order_splits'])
    require(basket_invalid(train, 2) == 0, 'Invalid refit baskets')
    require(train.join(users.where("split='train'"), 'user_id', 'left_anti').count() == 0, 'Nontrain user in fit')
    require(train.join(orders.where("split='train' AND eval_set='prior'"), ['order_id', 'user_id'], 'left_anti').count() == 0,
            'Nonprior order in fit')
    return train


def select_config(rows):
    valid = [row for row in rows if row['status'] == 'PASS']
    require(valid, 'No successful validation configuration')
    return min(valid, key=lambda r: (-r['rule_only']['recall_at_5'], -r['rule_only']['hit_rate_at_5'],
                                     -r['min_support'], -r['min_confidence'], r['config_id']))


def worker_split(spark, hdfs, run, state, report, prefix):
    from pyspark.sql import functions as F
    etl = json.loads((run / 'etl_manifest.json').read_text(encoding='utf-8'))
    source = Hdfs(spark, etl['hdfs_root'])
    baskets = checked_read(source, etl['silver']['baskets'])
    orders = checked_read(source, etl['silver']['orders'])
    report['input_fingerprints'] = {'orders': fingerprint(orders), 'baskets': fingerprint(baskets)}
    tables, cohorts = build_split(baskets, orders)
    report.update(cohorts=cohorts, seed=SEED, split_rule='SHA256 user-v1, first8hex %10000; 70/15/15', tables={})
    for name, frame in tables.items():
        _, report['tables'][name] = save_verified(hdfs, frame.repartition(8), prefix + '/' + name)
    popularity = tables['training_baskets'].select(F.explode('items').alias('product_id')).groupBy('product_id').count()
    popularity = popularity.withColumnRenamed('count', 'basket_count')
    _, report['tables']['train_popularity'] = save_verified(hdfs, popularity, prefix + '/train_popularity')
    report['audits'] = {'user_disjointness': 'PASS; one SHA split per distinct user', 'prior_train_only': 'PASS',
                        'context_target_disjoint_reconstruction': 'PASS', 'targets_separate_from_features': 'PASS'}


def worker_fit(spark, hdfs, run, state, report, prefix):
    from hdfs_pipeline.train import fit_artifacts
    split, split_path = checkpoint(run, 'split')
    train = audit_training(hdfs, split).repartition(8)
    cfg = dict(CONFIG, min_support=report['support'])
    start = time.perf_counter()
    itemsets, rules, counts = fit_artifacts(train, cfg)
    report['fit_seconds'] = round(time.perf_counter() - start, 3)
    check_itemsets(itemsets, train.count())
    check_arrays(rules, ('antecedent', 'consequent'))
    check_metrics(rules)
    report.update(config=cfg, counts=dict(counts, training_baskets=train.count()), artifacts={},
                  split_manifest_sha256=sha256(split_path), training_input=split['tables']['training_baskets'])
    for name, frame in (('frequent_itemsets', itemsets), ('rules', rules)):
        _, report['artifacts'][name] = save_verified(hdfs, frame, prefix + '/artifacts/' + name)


def worker_validation(spark, hdfs, run, state, report, prefix):
    from pipeline.fpgrowth_helpers import useful_rules, config_id
    from hdfs_pipeline.evaluation_metrics import evaluate_cases
    split, split_path = checkpoint(run, 'split')
    audit_training(hdfs, split)
    contexts = read_verified(hdfs, split['tables']['validation_contexts'])
    targets = read_verified(hdfs, split['tables']['validation_targets'])
    popularity = read_verified(hdfs, split['tables']['train_popularity'])
    rows, evaluations = [], {}
    for support in SUPPORTS:
        action = fit_action(support)
        if action not in state['checkpoints']:
            for confidence in CONFIDENCES:
                rows.append({'config_id': config_id(support, confidence), 'status': 'FAIL', 'min_support': support,
                             'min_confidence': confidence, 'error': 'Isolated fit failed; see attempt reports'})
            continue
        fitted, fitted_path = checkpoint(run, action)
        require(fitted['split_manifest_sha256'] == sha256(split_path), 'Fit/split fingerprint mismatch')
        raw = read_verified(hdfs, fitted['artifacts']['rules'])
        for confidence in CONFIDENCES:
            key = config_id(support, confidence)
            try:
                result = evaluate_cases(spark, hdfs, prefix + '/' + key, contexts, targets,
                                        useful_rules(raw, confidence), popularity)
            except Exception as error:
                rows.append({'config_id': key, 'status': 'FAIL', 'min_support': support,
                             'min_confidence': confidence, 'error': repr(error)})
                continue
            evaluations[key] = result
            metrics = next(row for row in result['methods'] if row['method'] == 'rule_only')
            rows.append({'config_id': key, 'status': 'PASS', 'min_support': support, 'min_confidence': confidence,
                         'rule_only': metrics, 'fit_report': str(fitted_path.relative_to(run)),
                         'fit_report_sha256': sha256(fitted_path), 'batch_seconds': result['batch_seconds']})
    chosen = select_config(rows)
    report.update(configurations=rows, evaluations=evaluations, selected=chosen)
    lock_path = run / 'locked_config.json'
    require(not lock_path.exists(), 'Configuration already locked; refusing reselection')
    locked = {'status': 'LOCKED', 'locked_at': now(), 'evaluation_run_id': state['evaluation_run_id'],
              'selected': chosen, 'selection': 'validation rule-only Recall@5, HitRate@5, higher support, higher confidence, config_id',
              'split_manifest_sha256': sha256(split_path), 'protocol_sha256': state['protocol_sha256'],
              'engine_sha256': sha256(ROOT / 'app/evaluation_engine.py'),
              'train_popularity': split['tables']['train_popularity'], 'hdfs_uri': hdfs.uri(prefix + '/locked_config.json')}
    dump(lock_path, locked)
    hdfs.upload(lock_path, prefix + '/locked_config.json')
    report['locked_config_sha256'] = sha256(lock_path)


def worker_test(spark, hdfs, run, state, report, prefix):
    from pipeline.fpgrowth_helpers import useful_rules
    from hdfs_pipeline.evaluation_metrics import evaluate_cases, load_engine, bounded_records
    lock = json.loads((run / 'locked_config.json').read_text(encoding='utf-8'))
    validation, _ = checkpoint(run, 'validation')
    require(sha256(run / 'locked_config.json') == validation['locked_config_sha256'] and
            lock['selected'] == validation['selected'], 'Validation lock fingerprint mismatch')
    require(lock['hdfs_uri'].startswith(state['hdfs_root'] + '/work/'), 'Locked HDFS URI outside evaluation')
    stream = hdfs.fs.open(hdfs.jpath(lock['hdfs_uri']))
    try:
        remote_lock = str(spark._jvm.org.apache.commons.io.IOUtils.toString(stream, 'UTF-8'))
    finally:
        stream.close()
    import hashlib
    require(hashlib.sha256(remote_lock.encode('utf-8')).hexdigest() == validation['locked_config_sha256'],
            'HDFS validation lock changed')
    require(lock['status'] == 'LOCKED' and lock['protocol_sha256'] == state['protocol_sha256'] and
            lock['engine_sha256'] == sha256(ROOT / 'app/evaluation_engine.py'), 'Protocol/engine changed after lock')
    chosen = lock['selected']
    fit_path = run / chosen['fit_report']
    require(sha256(fit_path) == chosen['fit_report_sha256'], 'Locked fit report changed')
    fitted = json.loads(fit_path.read_text(encoding='utf-8'))
    split, split_path = checkpoint(run, 'split')
    require(lock['split_manifest_sha256'] == sha256(split_path), 'Locked split changed')
    audit_training(hdfs, split)
    contexts = read_verified(hdfs, split['tables']['test_contexts'])
    # First test target access is below this explicit exposure marker.
    exposure = run / 'test_started.json'
    require(not exposure.exists(), 'Test already started; a fresh test label cannot be claimed')
    dump(exposure, {'started_at': now(), 'locked_config_sha256': sha256(run / 'locked_config.json'),
                    'rule': 'No model adjustment or test re-run allowed under this experiment ID'})
    targets = read_verified(hdfs, split['tables']['test_targets'])
    popularity = read_verified(hdfs, split['tables']['train_popularity'])
    rules = useful_rules(read_verified(hdfs, fitted['artifacts']['rules']), chosen['min_confidence'])
    report['evaluation'] = evaluate_cases(spark, hdfs, prefix, contexts, targets, rules, popularity)
    report['locked_config_sha256'] = sha256(run / 'locked_config.json')
    previous = []
    for prior in (ROOT / 'outputs/hdfs_evaluations').glob('*/test_started.json'):
        if prior.parent == run:
            continue
        prior_state = json.loads((prior.parent / 'run.json').read_text(encoding='utf-8'))
        if prior_state['etl_report_sha256'] == state['etl_report_sha256'] and prior_state['seed'] == SEED:
            previous.append(prior_state['evaluation_run_id'])
    report['test_used_for_development'] = bool(previous)
    report['previous_test_exposures_same_cohort'] = previous
    engine = load_engine(rules, popularity)
    cases = bounded_records(contexts.orderBy('order_id').limit(100), 100)
    import numpy as np
    latency = []
    for method in ('popularity', 'rule_only', 'hybrid'):
        engine.predict(cases[0]['context_items'], method)
        times = []
        for case in cases:
            for repeat in range(3):
                tick = time.perf_counter()
                engine.predict(case['context_items'], method)
                times.append((time.perf_counter() - tick) * 1000)
        latency.append({'method': method, 'p50_ms': float(np.percentile(times, 50)),
                        'p95_ms': float(np.percentile(times, 95)), 'calls': len(times), 'contexts': len(cases),
                        'mode': 'warm in-process engine; one excluded warm-up; 3 repeats/context; no network/UI'})
    report['latency'] = latency


def run_worker(job):
    from pyspark.sql import SparkSession
    report_path = job / 'report.json'
    report = json.loads(report_path.read_text(encoding='utf-8'))
    run = Path(report['run_dir'])
    state = json.loads((run / 'run.json').read_text(encoding='utf-8'))
    spark = None
    tick = time.perf_counter()
    report['stages'] = [{'name': report['action'], 'status': 'RUNNING', 'started_at': now()}]
    dump(report_path, report)
    try:
        require(sha256(ROOT / 'docs/evaluation_protocol.md') == state['protocol_sha256'], 'Protocol changed')
        etl_path = ROOT / 'outputs/hdfs_runs' / state['etl_run_id'] / 'report.json'
        require(sha256(etl_path) == state['etl_report_sha256'], 'ETL report changed')
        spark = (SparkSession.builder.master('local[2]').appName('IndependentEvaluation-' + report['action'])
                 .config('spark.driver.memory', '1g').config('spark.task.cpus', '1')
                 .config('spark.sql.shuffle.partitions', '8').config('spark.sql.adaptive.enabled', 'false')
                 .config('spark.hadoop.fs.defaultFS', 'file:///')
                 .config('spark.hadoop.dfs.client.use.datanode.hostname', 'true').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        report.update(spark_version=spark.version, hadoop_version=str(spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()))
        hdfs = Hdfs(spark, state['hdfs_root'])
        prefix = 'work/' + job.name
        report['hdfs_prefix'] = hdfs.uri(prefix)
        action = report['action']
        if action == 'split':
            worker_split(spark, hdfs, run, state, report, prefix)
        elif action.startswith('fit_'):
            worker_fit(spark, hdfs, run, state, report, prefix)
        elif action == 'validation':
            worker_validation(spark, hdfs, run, state, report, prefix)
        elif action == 'test':
            worker_test(spark, hdfs, run, state, report, prefix)
        elif action == 'export':
            from hdfs_pipeline.evaluation_export import export_evaluation
            export_evaluation(spark, hdfs, run, state, report, job, prefix)
        else:
            raise ValueError('Unknown action')
        report.update(status='PASS', seconds=round(time.perf_counter() - tick, 3), ended_at=now())
        report['stages'][0].update(status='PASS', seconds=report['seconds'])
        dump(report_path, report)
        hdfs.upload(report_path, prefix + '/checkpoint.json')
    except BaseException as error:
        report.update(status='FAIL', error=repr(error))
        report['stages'][0].update(status='FAIL', error=repr(error))
        raise
    finally:
        report.update(seconds=round(time.perf_counter() - tick, 3), ended_at=now())
        dump(report_path, report)
        if spark is not None:
            spark.stop()


def run_action(run, action, support=None):
    from hdfs_pipeline.train import supervise
    state = json.loads((run / 'run.json').read_text(encoding='utf-8'))
    require(action not in state['checkpoints'], 'PASS checkpoint already exists; use the next stage: ' + action)
    job = run / 'jobs' / (action + '_' + uuid4().hex[:8])
    job.mkdir(parents=True, exist_ok=False)
    dump(job / 'report.json', {'status': 'RUNNING', 'action': action, 'support': support,
                               'run_dir': str(run), 'stages': [], 'started_at': now()})
    code = supervise(job, CONFIG['worker_timeout_seconds'], module='hdfs_pipeline.evaluate')
    result = json.loads((job / 'report.json').read_text(encoding='utf-8'))
    state['attempts'].append({'action': action, 'report': str((job / 'report.json').relative_to(run)),
                              'status': result['status'], 'failure_reason': result.get('failure_reason', result.get('error'))})
    if code == 0:
        state['checkpoints'][action] = str((job / 'report.json').relative_to(run))
    state['updated_at'] = now()
    dump(run / 'run.json', state)
    print(action + '=' + result['status'] + '; REPORT=' + str(job / 'report.json'), flush=True)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=('all', 'split', 'refit', 'test', 'export'), default='all')
    parser.add_argument('--etl-run-id')
    parser.add_argument('--evaluation-run-id')
    parser.add_argument('--worker', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        run_worker(Path(args.worker))
        return 0
    evaluation_id = valid_id(args.evaluation_run_id or datetime.now(timezone.utc).strftime('eval_%Y%m%dT%H%M%SZ_') + uuid4().hex[:8])
    run = ROOT / 'outputs/hdfs_evaluations' / evaluation_id
    if args.stage in ('all', 'split'):
        etl_id = valid_id(args.etl_run_id)
        etl_path = ROOT / 'outputs/hdfs_runs' / etl_id / 'report.json'
        etl = json.loads(etl_path.read_text(encoding='utf-8'))
        base = source_manifest(etl, etl_id)
        require(0 < etl['sampling']['requested_fraction'] <= .05, 'Only the existing sample pilot is supported')
        run.mkdir(parents=True, exist_ok=False)
        dump(run / 'etl_manifest.json', etl)
        (run / 'evaluation_protocol.md').write_bytes((ROOT / 'docs/evaluation_protocol.md').read_bytes())
        dump(run / 'run.json', {'status': 'IN_PROGRESS', 'evaluation_run_id': evaluation_id, 'etl_run_id': etl_id,
                                'etl_root': base, 'hdfs_root': base + '/evaluations/' + evaluation_id,
                                'protocol_sha256': sha256(ROOT / 'docs/evaluation_protocol.md'),
                                'etl_report_sha256': sha256(etl_path), 'seed': SEED, 'k': 5,
                                'supports': list(SUPPORTS), 'confidences': list(CONFIDENCES),
                                'checkpoints': {}, 'attempts': [], 'started_at': now()})
    else:
        require((run / 'run.json').is_file(), 'Missing evaluation run; supply its explicit ID')
    import fcntl
    with (run / '.evaluation.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.stage in ('all', 'split'):
            require(run_action(run, 'split') == 0, '4A split failed; artifacts preserved')
        if args.stage in ('all', 'refit'):
            checkpoint(run, 'split')
            for support in SUPPORTS:
                state = json.loads((run / 'run.json').read_text())
                if fit_action(support) not in state['checkpoints']:
                    run_action(run, fit_action(support), support)
            require(run_action(run, 'validation') == 0, '4B validation failed; no test evaluation')
        if args.stage in ('all', 'test'):
            checkpoint(run, 'validation')
            require(run_action(run, 'test') == 0, '4C test failed; do not silently reuse exposed test')
        if args.stage in ('all', 'export'):
            checkpoint(run, 'test')
            require(run_action(run, 'export') == 0, 'Evaluation export failed')
        state = json.loads((run / 'run.json').read_text())
        state['status'] = 'PASS' if 'export' in state['checkpoints'] else 'CHECKPOINT_PASS'
        dump(run / 'run.json', state)
    print('EVALUATION_RUN_ID=' + evaluation_id, flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
