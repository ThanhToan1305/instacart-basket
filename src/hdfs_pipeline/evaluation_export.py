"""Selected refit Gold and independent experiment serving; preserves pilot bundles."""
import json
from pathlib import Path

from app.serving import sha256, verify_bundle
from hdfs_pipeline.helpers import ROOT, Hdfs, dump, require, now
from hdfs_pipeline.publish import build_gold, validate_gold, atomic_publish, checked_read
from hdfs_pipeline.export_dashboard import export_table, aggregates
from hdfs_pipeline.evaluation_metrics import summary, bounded_records, load_engine


def plots(folder, cohort, methods, latency=None):
    import pandas as pd
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import plotly.express as px
    data = pd.DataFrame(methods)
    outputs = []
    for name, columns, title in (
        ('quality', ['hit_rate_at_5', 'precision_at_5', 'recall_at_5'], 'Basket completion quality'),
        ('coverage', ['recommendation_coverage', 'rule_coverage'], 'Case coverage; rules separated from fallback')):
        source = data.melt(id_vars='method', value_vars=columns, var_name='metric', value_name='value')
        stem = cohort + '_' + name
        source.to_csv(folder / (stem + '.csv'), index=False)
        fig, ax = plt.subplots(figsize=(10, 5))
        data.set_index('method')[columns].plot.bar(ax=ax, rot=0)
        ax.set_title(cohort + ': ' + title)
        ax.set_ylim(0, 1)
        fig.tight_layout()
        fig.savefig(folder / (stem + '.png'), dpi=160)
        plt.close(fig)
        px.bar(source, x='method', y='value', color='metric', barmode='group',
               title=cohort + ': ' + title).write_html(str(folder / (stem + '.html')), include_plotlyjs=True)
        outputs.append(stem)
    if latency is not None:
        timing = pd.DataFrame(latency)
        timing.to_csv(folder / 'warm_latency.csv', index=False)
        fig, ax = plt.subplots(figsize=(9, 5))
        timing.set_index('method')[['p50_ms', 'p95_ms']].plot.bar(ax=ax, rot=0)
        ax.set_ylabel('Milliseconds, warm in-process, no network/UI')
        fig.tight_layout()
        fig.savefig(folder / 'warm_latency.png', dpi=160)
        plt.close(fig)
        melted = timing.melt(id_vars='method', value_vars=['p50_ms', 'p95_ms'], var_name='percentile', value_name='ms')
        px.bar(melted, x='method', y='ms', color='percentile', barmode='group',
               title='Warm engine latency; 3 repeats per context').write_html(str(folder / 'warm_latency.html'), include_plotlyjs=True)
        outputs.append('warm_latency')
    return outputs


def export_evaluation(spark, hdfs, run, state, report, job, prefix):
    from pyspark.sql import functions as F
    from hdfs_pipeline.evaluate import checkpoint, read_verified
    import pandas as pd
    import pyarrow.parquet as pq
    split, split_path = checkpoint(run, 'split')
    validation, _ = checkpoint(run, 'validation')
    test, test_path = checkpoint(run, 'test')
    lock_path = run / 'locked_config.json'
    lock = json.loads(lock_path.read_text(encoding='utf-8'))
    require(test['locked_config_sha256'] == sha256(lock_path) == validation['locked_config_sha256'], 'Evaluation lock changed')
    require(lock['engine_sha256'] == sha256(ROOT / 'app/evaluation_engine.py'), 'Engine changed after test')
    chosen = lock['selected']
    fitted = json.loads((run / chosen['fit_report']).read_text(encoding='utf-8'))
    require(sha256(run / chosen['fit_report']) == chosen['fit_report_sha256'], 'Selected fit changed')
    etl = json.loads((run / 'etl_manifest.json').read_text(encoding='utf-8'))
    etl_fs = Hdfs(spark, etl['hdfs_root'])
    catalog = checked_read(etl_fs, etl['gold']['product_catalog'])
    itemsets = read_verified(hdfs, fitted['artifacts']['frequent_itemsets'])
    raw = read_verified(hdfs, fitted['artifacts']['rules'])
    popularity = read_verified(hdfs, split['tables']['train_popularity'])
    require(popularity.join(catalog, 'product_id', 'left_anti').count() == 0, 'Unknown training popularity ID')
    require(popularity.where((F.col('basket_count') <= 0) |
                             (F.col('basket_count') > fitted['counts']['training_baskets'])).count() == 0,
            'Invalid training basket frequency')
    rules, lookup = build_gold(raw, catalog.select('product_id', 'product_name'), chosen['min_confidence'])
    gold = {}
    staging = prefix + '/.staging_gold'
    final = prefix + '/gold'
    for name, frame in (('frequent_itemsets', itemsets), ('association_rules', rules), ('recommendation_lookup', lookup)):
        _, info = hdfs.save(frame, staging + '/' + name, frame.count())
        gold[name] = dict(info, uri=hdfs.uri(final + '/' + name))
    validate_gold(itemsets, rules, lookup, catalog.select('product_id', 'product_name'),
                  fitted['counts']['training_baskets'], chosen['min_confidence'])
    atomic_publish(hdfs, staging, final)
    for info in gold.values():
        checked_read(hdfs, info)
    val_result = validation['evaluations'][chosen['config_id']]
    for result in (val_result, test['evaluation']):
        actual = summary(checked_read(hdfs, result['per_case_scores']))
        for a, b in zip(actual, result['methods']):
            for key in a:
                require(abs(a[key] - b[key]) <= 1e-12 if isinstance(a[key], float) else a[key] == b[key],
                        'Export metrics do not reproduce saved per-case scores')
    source_manifest = {'status': 'PASS', 'evaluation_run_id': state['evaluation_run_id'], 'gold': gold,
                       'locked_config_sha256': sha256(lock_path), 'test_report_sha256': sha256(test_path)}
    dump(job / 'evaluation_gold_manifest.json', source_manifest)
    hdfs.upload(job / 'evaluation_gold_manifest.json', final + '/evaluation_gold_manifest.json')
    export_id = job.name
    serving_parent = run / 'serving'
    stage = serving_parent / ('.staging_' + export_id)
    target = serving_parent / export_id
    require(not stage.exists() and not target.exists(), 'Evaluation serving already exists')
    stage.mkdir(parents=True, exist_ok=False)
    metric_dir = stage / 'outputs/metrics'
    metric_dir.mkdir(parents=True)
    entries = {'product_catalog': etl['gold']['product_catalog'], 'kpi_summary': etl['gold']['kpi_summary'],
               'association_rules': gold['association_rules'], 'recommendation_lookup': gold['recommendation_lookup'],
               'train_popularity': split['tables']['train_popularity']}
    keys = {'product_catalog': ['product_id'], 'kpi_summary': [], 'association_rules': ['rule_id'],
            'recommendation_lookup': ['rule_id', 'antecedent_product_id'], 'train_popularity': ['product_id']}
    tables = {}
    for name, entry in entries.items():
        fs = etl_fs if name in ('product_catalog', 'kpi_summary') else hdfs
        tables[name] = export_table(fs, entry, stage / 'data/gold' / name, keys[name])
    aggregates_info = aggregates(spark, etl, metric_dir)
    evaluation = {'status': 'PASS', 'evaluation_run_id': state['evaluation_run_id'],
                  'protocol': 'basket completion v1; user-disjoint 70/15/15; fixed hash context/target; K=5',
                  'protocol_sha256': state['protocol_sha256'], 'split_sizes': split['cohorts'],
                  'locked_config': lock, 'validation': val_result, 'test': test['evaluation'],
                  'test_used_for_development': test['test_used_for_development'],
                  'previous_test_exposures_same_cohort': test['previous_test_exposures_same_cohort'],
                  'latency': test['latency'], 'limitation': 'Order sample, incomplete user history, one host/local[2]'}
    dump(metric_dir / 'evaluation_summary.json', evaluation)
    baseline = [dict(row, split=cohort) for cohort, result in (('validation', val_result), ('test', test['evaluation']))
                for row in result['methods']]
    pd.DataFrame(baseline).to_csv(metric_dir / 'baseline_comparison.csv', index=False)
    rows = []
    for row in validation['configurations']:
        rows.append({key: value for key, value in row.items() if key not in ('rule_only',)})
        rows[-1].update(row.get('rule_only', {}))
    pd.DataFrame(rows).to_csv(metric_dir / 'validation_grid.csv', index=False)
    figures = plots(metric_dir, 'validation', val_result['methods'])
    figures += plots(metric_dir, 'test', test['evaluation']['methods'], test['latency'])
    pd.DataFrame([{'split': cohort, 'batch_seconds': result['batch_seconds'], 'scope': result['timing_scope']}
                  for cohort, result in (('validation', val_result), ('test', test['evaluation']))]).to_csv(metric_dir / 'batch_times.csv', index=False)
    kpi = pq.read_table(stage / 'data/gold/kpi_summary').to_pylist()[0]
    dump(metric_dir / 'eda_summary.json', {'status': 'PASS', 'kpi': kpi, 'etl_run_id': state['etl_run_id']})
    dump(metric_dir / 'model_summary.json', {'status': 'PASS', 'config': fitted['config'],
                                          'counts': {name: value['rows'] for name, value in gold.items()},
                                          'evaluation_status': 'EVALUATED_BASKET_COMPLETION'})
    timings = []
    for attempt in state['attempts']:
        value = json.loads((run / attempt['report']).read_text(encoding='utf-8'))
        timings.append({'Run': state['evaluation_run_id'], 'Stage': value['action'], 'Giây': value.get('seconds'),
                        'Trạng thái': value['status']})
    counts = [{'Bảng': name, 'Dòng': value['rows'], 'URI': value['uri']} for name, value in tables.items()]
    dump(metric_dir / 'hdfs_system.json', {'timings': timings, 'counts': counts})
    pd.DataFrame(timings).to_csv(metric_dir / 'hdfs_stage_timings.csv', index=False)
    pd.DataFrame(counts).to_csv(metric_dir / 'hdfs_export_counts.csv', index=False)
    scores = checked_read(hdfs, test['evaluation']['per_case_scores'])
    demos = []
    from app.evaluation_engine import RuleEngine
    local_engine = RuleEngine(pq.read_table(stage / 'data/gold/association_rules').to_pylist(),
                              pq.read_table(stage / 'data/gold/train_popularity').to_pylist())
    for label, condition in (('Luật', "method='hybrid' AND rule_count>0"), ('Fallback', "method='hybrid' AND rule_count=0")):
        example = scores.where(condition).orderBy('order_id').select('order_id', 'context_items', 'recommendations').limit(1).first()
        if example:
            require([row['product_id'] for row in local_engine.predict(example['context_items'], 'hybrid')] ==
                    [row['product_id'] for row in example['recommendations']], 'Serving demo differs from scored model')
            demos.append({'label': label + ' • test order_id=' + str(example['order_id']), 'context': example['context_items']})
    dump(metric_dir / 'demo_cases.json', {'cases': demos, 'source': 'test contexts only; targets not exposed to UI'})
    manifest = {'status': 'PASS', 'created_at': now(), 'export_id': export_id,
                'mode': 'Gold export từ HDFS', 'execution_mode': 'SAMPLE',
                'evaluation_status': 'EVALUATED_BASKET_COMPLETION', 'evaluation_run_id': state['evaluation_run_id'],
                'etl_run_id': state['etl_run_id'], 'etl_root': state['etl_root'], 'evaluation_root': state['hdfs_root'],
                'model_run_id': Path(chosen['fit_report']).parent.name, 'publication_id': job.name,
                'gold_root': hdfs.uri(final), 'train_popularity_uri': entries['train_popularity']['uri'],
                'publication_manifest_uri': hdfs.uri(final + '/evaluation_gold_manifest.json'),
                'model_config': fitted['config'], 'publication_min_confidence': chosen['min_confidence'],
                'compute': 'Spark local[2], driver=1g, task.cpus=1; one Docker host',
                'test_used_for_development': test['test_used_for_development'],
                'tables': tables, 'aggregates': aggregates_info, 'figures': figures,
                'protocol_sha256': state['protocol_sha256'], 'locked_config_sha256': sha256(lock_path),
                'checksum_definition': 'Local SHA-256 inventory; HDFS/local logical values independently compared',
                'files': {file.relative_to(stage).as_posix(): sha256(file) for file in sorted(stage.rglob('*')) if file.is_file()}}
    dump(stage / 'serving_manifest.json', manifest)
    verify_bundle(stage)
    stage.rename(target)
    verify_bundle(target)
    report.update(serving_dir=str(target), serving_manifest=str(target / 'serving_manifest.json'), gold=gold,
                  counts={name: value['rows'] for name, value in tables.items()}, figures=figures)
    for pointer in (ROOT / 'outputs/hdfs_evaluations/latest_pass.json', ROOT / 'outputs/hdfs_serving/latest_pass.json'):
        dump(pointer, {'status': 'PASS', 'evaluation_run_id': state['evaluation_run_id'], 'export_id': export_id,
                       'serving_dir': str(target.relative_to(ROOT)), 'serving_manifest': str((target / 'serving_manifest.json').relative_to(ROOT))})
