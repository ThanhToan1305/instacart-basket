"""Run explicitly against a completed serving bundle; never manufactures metrics."""
import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


@unittest.skipUnless(os.environ.get('HDFS_SERVING_DIR'), 'Needs explicit real HDFS serving bundle')
class RealServingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app.serving import verify_bundle
        cls.root = Path(os.environ['HDFS_SERVING_DIR'])
        cls.manifest = verify_bundle(cls.root)

    def test_export_source_contract(self):
        import copy
        from hdfs_pipeline.export_dashboard import export_contract
        repo = Path(__file__).resolve().parents[3]
        model_dir = repo / 'outputs/hdfs_models' / self.manifest['model_run_id']
        publication = json.loads((model_dir / 'publications' / self.manifest['publication_id'] / 'report.json').read_text())
        model = json.loads((model_dir / 'report.json').read_text())
        etl = json.loads((repo / 'outputs/hdfs_runs' / self.manifest['etl_run_id'] / 'report.json').read_text())
        export_contract(publication, model, etl)
        bad = copy.deepcopy(publication)
        bad['status'] = 'FAIL'
        with self.assertRaises(ValueError):
            export_contract(bad, model, etl)
        bad = copy.deepcopy(etl)
        bad['sampling']['requested_fraction'] = 1.
        with self.assertRaises(ValueError):
            export_contract(publication, model, bad)

    def test_all_five_pages_and_demo_rule_fallback(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[3] / 'app/dashboard.py'), default_timeout=60).run()
        self.assertFalse(app.exception)
        for page in ['Tổng quan', 'Hành vi mua sắm', 'Luật kết hợp', 'Gợi ý sản phẩm', 'Hiệu năng hệ thống']:
            app.sidebar.radio[0].set_value(page).run()
            self.assertFalse(app.exception, page)
            self.assertFalse(app.error, page)
        app.sidebar.radio[0].set_value('Gợi ý sản phẩm').run()
        self.assertTrue(any('Chọn ít nhất' in info.value for info in app.info))
        demos = json.loads((self.root / 'outputs/metrics/demo_cases.json').read_text())
        for case in demos['cases']:
            app.multiselect[0].set_value(case['context']).run()
            self.assertFalse(app.exception)
            self.assertFalse(app.error)
            self.assertGreater(len(app.dataframe[0].value), 0)
            if case['label'].startswith('Fallback'):
                self.assertTrue(any('Fallback:' in warning.value for warning in app.warning))
            else:
                self.assertTrue(app.success)
        app.sidebar.radio[0].set_value('Luật kết hợp').run()
        app.number_input[0].set_value(1.0).run()
        self.assertFalse(app.exception)
        self.assertEqual(app.metric[0].value, '0')

    def test_real_matching_partial_exclusion_unknown_and_fallback(self):
        from app.data_loader import gold, metrics
        from app.recommender import recommend
        catalog, lookup = gold('product_catalog'), gold('recommendation_lookup')
        popular = metrics('top_20_products.csv')
        demos = metrics('demo_cases.json')
        for case in demos['cases']:
            result = recommend(case['context'], lookup, popular, catalog)
            self.assertLessEqual(len(result), 5)
            self.assertEqual(len(result), result.product_id.nunique())
            self.assertFalse(set(result.product_id) & set(case['context']))
            self.assertTrue(set(result.product_id) <= set(catalog.product_id))
            if case['label'].startswith('Fallback'):
                self.assertTrue(result[['support', 'confidence', 'lift']].isna().all().all())
        multi = lookup[lookup.antecedent_size > 1]
        if not multi.empty:
            rule = multi.iloc[0]
            partial = recommend(list(rule.antecedent)[:-1], lookup[lookup.rule_id == rule.rule_id], popular, catalog)
            self.assertTrue((partial.source == 'Fallback phổ biến').all())
        unknown = recommend([999999999], lookup, popular, catalog)
        self.assertTrue((unknown.source == 'Fallback phổ biến').all())
        self.assertTrue(set(unknown.product_id) <= set(catalog.product_id))


if __name__ == '__main__':
    unittest.main()
