"""Offline evaluated bundle lineage; pilot compatibility remains independently tested."""
import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from test_hdfs_export import fixture
from app.serving import verify_bundle, read_manifest, sha256


class EvaluationServingTests(unittest.TestCase):
    def test_evaluated_bundle_requires_own_train_popularity(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            data = fixture(root)
            evaluation_root = data['etl_root'] + '/evaluations/eval1'
            gold_root = evaluation_root + '/work/export_1/gold'
            data.update(evaluation_status='EVALUATED_BASKET_COMPLETION', evaluation_run_id='eval1',
                        evaluation_root=evaluation_root, gold_root=gold_root, publication_id='export_1',
                        train_popularity_uri=evaluation_root + '/work/split_1/train_popularity')
            for name in ('association_rules', 'recommendation_lookup'):
                data['tables'][name]['uri'] = gold_root + '/' + name
            folder = root / 'data/gold/train_popularity'
            folder.mkdir()
            table = pa.table({'product_id': [1, 2], 'basket_count': [3, 2]})
            pq.write_table(table, folder / 'part.parquet')
            (folder / '_SUCCESS').touch()
            data['tables']['train_popularity'] = {'uri': data['train_popularity_uri'], 'rows': 2, 'arrow_schema': str(table.schema)}
            metric_dir = root / 'outputs/metrics'
            metric_dir.mkdir(parents=True)
            (metric_dir / 'evaluation_summary.json').write_text(json.dumps({'locked_config': {
                'engine_sha256': sha256(Path(__file__).resolve().parents[3] / 'app/evaluation_engine.py')}}), encoding='utf-8')
            data['files'] = {file.relative_to(root).as_posix(): sha256(file) for file in root.rglob('*')
                             if file.is_file() and file.name != 'serving_manifest.json'}
            (root / 'serving_manifest.json').write_text(json.dumps(data), encoding='utf-8')
            self.assertEqual(verify_bundle(root)['evaluation_run_id'], 'eval1')
            data['train_popularity_uri'] = data['etl_root'] + '/gold/eda_popularity'
            (root / 'serving_manifest.json').write_text(json.dumps(data), encoding='utf-8')
            with self.assertRaises(ValueError):
                read_manifest(root)
            data['train_popularity_uri'] = evaluation_root + '/work/split_1/train_popularity'
            metadata_file = metric_dir / 'evaluation_summary.json'
            metadata_file.write_text(json.dumps({'locked_config': {'engine_sha256': '0' * 64}}), encoding='utf-8')
            data['files']['outputs/metrics/evaluation_summary.json'] = sha256(metadata_file)
            (root / 'serving_manifest.json').write_text(json.dumps(data), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'Engine hiện tại khác fingerprint'):
                read_manifest(root)


if __name__ == '__main__':
    unittest.main()
