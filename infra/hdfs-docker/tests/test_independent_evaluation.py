"""Leakage, split reproducibility, denominator, and shared engine regressions."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from app.evaluation_engine import RuleEngine, score_case
from hdfs_pipeline.evaluation_split import user_split, split_items, build_split
from hdfs_pipeline.evaluate import select_config


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.rules = [dict(rule_id='pair', antecedent=[1, 2], consequent=[3], confidence=.8, lift=2., support=.1),
                      dict(rule_id='single', antecedent=[1], consequent=[3], confidence=.7, lift=2., support=.1),
                      dict(rule_id='tie', antecedent=[1], consequent=[4], confidence=.7, lift=2., support=.1)]
        self.popular = [dict(product_id=i, basket_count=20-i) for i in range(1, 11)]

    def test_partial_full_distinct_exclusion_and_hybrid(self):
        engine = RuleEngine(self.rules + self.rules, self.popular)
        pair_only = RuleEngine(self.rules[:1], self.popular)
        self.assertEqual(pair_only.predict([1], 'rule_only'), [])
        self.assertEqual([r['product_id'] for r in engine.predict([1, 2], 'rule_only')], [3, 4])
        hybrid = engine.predict([1, 2], 'hybrid')
        self.assertEqual([r['product_id'] for r in hybrid], [3, 4, 5, 6, 7])
        self.assertEqual([r['source'] for r in hybrid], ['rule', 'rule', 'popularity', 'popularity', 'popularity'])
        self.assertTrue(all(r['confidence'] is None for r in hybrid if r['source'] == 'popularity'))
        self.assertEqual(hybrid, RuleEngine(list(reversed(self.rules)), list(reversed(self.popular))).predict([2, 1, 1], 'hybrid'))
        self.assertFalse({1, 2, 3} & {r['product_id'] for r in engine.predict([1, 2, 3], 'hybrid')})

    def test_precision_fixed_five_and_zero_denominator_cases(self):
        engine = RuleEngine(self.rules[:1], self.popular)
        hit = score_case(engine.predict([1, 2], 'rule_only'), [3, 9])
        zero = score_case(engine.predict([8], 'rule_only'), [3])
        self.assertEqual(hit['precision_at_5'], .2)
        self.assertEqual(hit['recall_at_5'], .5)
        self.assertEqual((hit['hit_rate_at_5'] + zero['hit_rate_at_5']) / 2, .5)
        self.assertEqual(zero['recommendation_coverage'], 0.)

    def test_selection_uses_validation_not_time(self):
        rows = [dict(status='PASS', config_id='a', min_support=.002, min_confidence=.2,
                     rule_only={'recall_at_5': .1, 'hit_rate_at_5': .2}, batch_seconds=999),
                dict(status='PASS', config_id='b', min_support=.001, min_confidence=.4,
                     rule_only={'recall_at_5': .1, 'hit_rate_at_5': .2}, batch_seconds=.01),
                dict(status='FAIL', config_id='failed')]
        self.assertEqual(select_config(rows)['config_id'], 'a')
        rows[1]['rule_only']['recall_at_5'] = .11
        self.assertEqual(select_config(rows)['config_id'], 'b')

    def test_split_hash_and_items_reproducible(self):
        self.assertEqual(user_split(123), user_split(123))
        first = split_items(123, 456, [1, 2, 9, 900])
        self.assertEqual(first, split_items(123, 456, [900, 2, 1, 9]))
        self.assertFalse(set(first[0]) & set(first[1]))
        self.assertEqual(set(first[0]) | set(first[1]), {1, 2, 9, 900})


class SplitSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('IndependentEvaluationFixtures')
                     .config('spark.sql.shuffle.partitions', '2').config('spark.sql.adaptive.enabled', 'false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_disjoint_prior_only_and_python_spark_reproducibility(self):
        ids = {split: next(i for i in range(1, 1000) if user_split(i) == split)
               for split in ('train', 'validation', 'test')}
        orders = self.spark.createDataFrame([
            (1, ids['train'], 'prior', 1), (2, ids['train'], 'train', 2),
            (3, ids['validation'], 'prior', 1), (4, ids['validation'], 'train', 2),
            (5, ids['test'], 'prior', 1), (6, ids['test'], 'test', 2)],
            'order_id int, user_id int, eval_set string, order_number int')
        baskets = self.spark.createDataFrame([
            (1, ids['train'], [1, 2], 2), (2, ids['train'], [2, 3], 2),
            (3, ids['validation'], [1, 8], 2), (4, ids['validation'], [1, 2, 9, 900], 4),
            (5, ids['test'], [1, 7, 999], 3)],
            'order_id int, user_id int, items array<int>, basket_size int')
        tables, counts = build_split(baskets, orders)
        self.assertEqual([r.order_id for r in tables['training_baskets'].collect()], [1])
        self.assertEqual({r.user_id: r.split for r in tables['users'].collect()},
                         {value: split for split, value in ids.items()})
        for split, order, items in [('validation', 4, [1, 2, 9, 900]), ('test', 5, [1, 7, 999])]:
            context, target = split_items(ids[split], order, items)
            self.assertEqual(tables[split + '_contexts'].first().context_items, context)
            self.assertEqual(tables[split + '_targets'].first().target_items, target)
            self.assertNotIn('target_items', tables[split + '_contexts'].columns)
            self.assertEqual(tables[split + '_selected_baskets'].first().order_id, order)
        self.assertEqual(counts['validation']['selected_sources'], {'train': 1})
        self.assertEqual(counts['test']['selected_sources'], {'prior': 1})

    def test_spark_metrics_keep_zero_case_and_precision_divides_five(self):
        from hdfs_pipeline.evaluation_metrics import score_predictions, summary
        predictions = self.spark.createDataFrame([
            (1, 10, [1, 2], 'rule_only', [(3, 'rule', 'r', .5, 2., .1, None)]),
            (2, 11, [7], 'rule_only', [])],
            'order_id int,user_id int,context_items array<int>,method string,recommendations array<struct<product_id:int,source:string,rule_id:string,confidence:double,lift:double,support:double,basket_count:bigint>>')
        targets = self.spark.createDataFrame([(1, 10, [3, 8]), (2, 11, [9])],
                                             'order_id int,user_id int,target_items array<int>')
        result = summary(score_predictions(predictions, targets))[0]
        self.assertEqual(result['eligible_cases'], 2)
        self.assertEqual(result['precision_at_5'], .1)
        self.assertEqual(result['recall_at_5'], .25)
        self.assertEqual(result['hit_rate_at_5'], .5)
        self.assertEqual(result['zero_recommendation_cases'], 1)
        self.assertEqual(result['fewer_than_5_cases'], 2)


if __name__ == '__main__':
    unittest.main()
