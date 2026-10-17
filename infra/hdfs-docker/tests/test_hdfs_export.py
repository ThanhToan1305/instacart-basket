"""Serving contract fixtures, independent of project datasets."""
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from app import data_loader
from app.serving import read_manifest, verify_bundle, sha256


def fixture(root, export_id='e1'):
    import pyarrow as pa
    import pyarrow.parquet as pq
    base = 'hdfs://namenode:9000/project/etl1'
    manifest = {'status': 'PASS', 'mode': 'Gold export từ HDFS', 'execution_mode': 'SAMPLE',
                'evaluation_status': 'NOT_EVALUATED', 'export_id': export_id,
                'etl_run_id': 'etl1', 'model_run_id': 'model1', 'publication_id': 'pub1',
                'etl_root': base, 'tables': {}, 'files': {}}
    manifest.update(created_at='2026-01-01T00:00:00+00:00', model_config={})
    for name in ('product_catalog', 'kpi_summary', 'association_rules', 'recommendation_lookup'):
        folder = root / 'data/gold' / name
        folder.mkdir(parents=True)
        table = pa.table({'product_id': [1, 2], 'product_name': ['one', 'two']})
        pq.write_table(table, folder / 'part.parquet')
        (folder / '_SUCCESS').touch()
        uri = (base + '/gold/' + name if name in ('product_catalog', 'kpi_summary')
               else base + '/models/model1/gold/pub1/' + name)
        manifest['tables'][name] = {'uri': uri, 'rows': 2, 'arrow_schema': str(table.schema)}
    for file in root.rglob('*'):
        if file.is_file():
            manifest['files'][file.relative_to(root).as_posix()] = sha256(file)
    (root / 'serving_manifest.json').write_text(json.dumps(manifest), encoding='utf-8')
    return manifest


class ServingTests(unittest.TestCase):
    def setUp(self):
        import streamlit as st
        st.cache_data.clear()

    def test_missing_failed_incomplete_lineage_and_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaises(ValueError):
                read_manifest(root)
            good = fixture(root)
            self.assertEqual(verify_bundle(root)['status'], 'PASS')
            for change in ('status', 'lineage', 'rows', 'required'):
                bad = copy.deepcopy(good)
                if change == 'status':
                    bad['status'] = 'FAIL'
                elif change == 'lineage':
                    bad['tables']['association_rules']['uri'] = 'hdfs://wrong:9000/other'
                elif change == 'rows':
                    bad['tables']['product_catalog']['rows'] = 100001
                else:
                    bad.pop('created_at')
                (root / 'serving_manifest.json').write_text(json.dumps(bad), encoding='utf-8')
                with self.assertRaises(ValueError):
                    read_manifest(root)
            (root / 'serving_manifest.json').write_text(json.dumps(good), encoding='utf-8')
            (root / 'data/gold/association_rules/_SUCCESS').unlink()
            with self.assertRaises(ValueError):
                read_manifest(root)

    def test_integrity_schema_count_and_path_escape(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            good = fixture(root)
            with patch.dict(os.environ, {'HDFS_SERVING_DIR': str(root)}):
                self.assertEqual(len(data_loader.gold('product_catalog')), 2)
                with self.assertRaises(data_loader.DataUnavailable):
                    data_loader.load('../../data/raw/orders.csv')
                for change in ('schema', 'count'):
                    bad = copy.deepcopy(good)
                    if change == 'schema':
                        bad['tables']['product_catalog']['arrow_schema'] = 'wrong'
                    else:
                        bad['tables']['product_catalog']['rows'] = 3
                    (root / 'serving_manifest.json').write_text(json.dumps(bad), encoding='utf-8')
                    self.setUp()
                    with self.assertRaises(data_loader.DataUnavailable):
                        data_loader.gold('product_catalog')
                (root / 'serving_manifest.json').write_text(json.dumps(good), encoding='utf-8')
                (root / 'data/gold/product_catalog/part.parquet').write_bytes(b'broken')
                with self.assertRaises(data_loader.DataUnavailable):
                    data_loader.gold('product_catalog')

    def test_switch_run_cache_and_local_mode(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first, second = root / 'first', root / 'second'
            fixture(first, 'e1')
            fixture(second, 'e2')
            with patch.dict(os.environ, {'HDFS_SERVING_DIR': str(first)}):
                one = data_loader.signature('data/gold/product_catalog')
                self.assertEqual(data_loader.serving_context()['export_id'], 'e1')
            with patch.dict(os.environ, {'HDFS_SERVING_DIR': str(second)}):
                two = data_loader.signature('data/gold/product_catalog')
                self.assertNotEqual(one, two)
                self.assertEqual(data_loader.serving_context()['export_id'], 'e2')
            with patch.dict(os.environ, {'HDFS_SERVING_DIR': ''}), patch.object(data_loader, 'ROOT', first):
                self.assertIsNone(data_loader.serving_context())
                self.assertEqual(data_loader.gold('product_catalog').product_name.tolist(), ['one', 'two'])

    def test_invalid_export_ui_stops_with_message(self):
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {'HDFS_SERVING_DIR': temp}):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[3] / 'app/dashboard.py')).run()
            self.assertFalse(app.exception)
            self.assertTrue(app.error)


class FrameComparisonTests(unittest.TestCase):
    def test_hdfs_local_count_schema_value_and_duplicate(self):
        from pyspark.sql import SparkSession
        from hdfs_pipeline.export_dashboard import compare_frames
        spark = SparkSession.builder.master('local[2]').appName('ExportFixtures').getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        try:
            frame = spark.createDataFrame([(1, 'one'), (2, 'two')], 'product_id int, product_name string')
            compare_frames(frame, frame, ['product_id'])
            for bad in (frame.limit(1), spark.createDataFrame([(1, 'wrong'), (2, 'two')], frame.schema),
                        spark.createDataFrame([(1, 'one'), (1, 'one')], frame.schema),
                        frame.selectExpr('cast(product_id as bigint) product_id', 'product_name')):
                with self.assertRaises(ValueError):
                    compare_frames(frame, bad, ['product_id'])
        finally:
            spark.stop()


if __name__ == '__main__':
    unittest.main()
