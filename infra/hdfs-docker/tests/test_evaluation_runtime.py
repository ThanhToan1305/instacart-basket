"""Explicit real-serving tests for evaluated basket completion and hybrid UI."""
import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))


@unittest.skipUnless(os.environ.get('HDFS_SERVING_DIR'), 'Needs explicit evaluated serving export')
class EvaluatedRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from app.serving import verify_bundle
        cls.root = Path(os.environ['HDFS_SERVING_DIR'])
        cls.manifest = verify_bundle(cls.root)
        if cls.manifest['evaluation_status'] != 'EVALUATED_BASKET_COMPLETION':
            raise unittest.SkipTest('This suite requires the independent evaluation bundle')

    def test_frozen_model_and_training_popularity(self):
        from app.serving import sha256
        from app.data_loader import gold, metrics
        evaluation = metrics('evaluation_summary.json')
        lock = evaluation['locked_config']
        self.assertEqual(lock['engine_sha256'], sha256(Path(__file__).resolve().parents[3] / 'app/evaluation_engine.py'))
        popularity, catalog = gold('train_popularity'), gold('product_catalog')
        self.assertEqual(popularity.product_id.nunique(), len(popularity))
        self.assertTrue(set(popularity.product_id) <= set(catalog.product_id))
        self.assertGreater(popularity.basket_count.min(), 0)
        self.assertLessEqual(popularity.basket_count.max(), evaluation['split_sizes']['training']['training_baskets'])
        baseline = metrics('baseline_comparison.csv')
        self.assertEqual(len(baseline), 6)
        for cohort in ('validation', 'test'):
            rows = baseline[baseline['split'] == cohort]
            self.assertEqual(set(rows.method), {'popularity', 'rule_only', 'hybrid'})
            self.assertTrue((rows.eligible_cases == evaluation['split_sizes'][cohort]['eligible_cases']).all())
        self.assertEqual(evaluation['test_used_for_development'], bool(evaluation['previous_test_exposures_same_cohort']))

    def test_all_pages_hybrid_rule_only_and_popularity(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path(__file__).resolve().parents[3] / 'app/dashboard.py'), default_timeout=60).run()
        for page in ['Tổng quan', 'Hành vi mua sắm', 'Luật kết hợp', 'Gợi ý sản phẩm', 'Hiệu năng hệ thống']:
            app.sidebar.radio[0].set_value(page).run()
            self.assertFalse(app.exception, page)
            self.assertFalse(app.error, page)
        app.sidebar.radio[0].set_value('Gợi ý sản phẩm').run()
        demos = json.loads((self.root / 'outputs/metrics/demo_cases.json').read_text())
        for case in demos['cases']:
            next(widget for widget in app.radio if widget.label == 'Giỏ minh họa từ dữ liệu thật').set_value(case['label']).run()
            next(widget for widget in app.button if widget.label == 'Áp dụng giỏ minh họa').click().run()
            self.assertEqual(app.multiselect[0].value, case['context'])
            for method in ('Hybrid', 'Rule-only', 'Popularity'):
                next(widget for widget in app.selectbox if widget.label == 'Phương pháp').set_value(method).run()
                self.assertFalse(app.exception)
                self.assertFalse(app.error)
                if app.dataframe:
                    output = app.dataframe[0].value
                    self.assertLessEqual(len(output), 5)
                    self.assertEqual(output.product_id.nunique(), len(output))
                    self.assertFalse(set(output.product_id) & set(case['context']))
                    if method == 'Hybrid':
                        self.assertEqual(len(output), 5)
                    if method == 'Popularity':
                        self.assertTrue(output[['Support', 'Confidence', 'Lift']].isna().all().all())
                    if method == 'Rule-only':
                        self.assertTrue((output['Nguồn'] == 'Luật kết hợp').all())


if __name__ == '__main__':
    unittest.main()
