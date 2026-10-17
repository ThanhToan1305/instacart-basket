"""Publication regression fixtures; no fixture is used as project evidence."""
import copy
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'app'))
from hdfs_pipeline.publish import (atomic_publish, build_gold, checked_read, check_itemsets,
                                   check_metrics, publication_contract, validate_gold)


class PublicationContractTests(unittest.TestCase):
    def test_duplicate_and_incomplete_publications_rejected(self):
        hdfs = Mock()
        hdfs.exists.return_value = True
        with self.assertRaisesRegex(ValueError, 'refusing overwrite'):
            atomic_publish(hdfs, 'staging', 'final')
        hdfs.exists.side_effect = [False, False]
        with self.assertRaisesRegex(ValueError, 'Staging publication is missing'):
            atomic_publish(hdfs, 'staging', 'final')

    def test_missing_success_rejected_before_read(self):
        hdfs = Mock(base='hdfs://namenode:9000/project')
        hdfs.exists.return_value = False
        with self.assertRaisesRegex(ValueError, 'Missing _SUCCESS'):
            checked_read(hdfs, {'uri': hdfs.base + '/rules'})
        hdfs.spark.read.parquet.assert_not_called()

    def test_confidence_and_lineage(self):
        base = 'hdfs://namenode:9000/project/etl1'
        root = base + '/models/model1'
        etl = {'status': 'PASS', 'run_id': 'etl1', 'hdfs_root': base,
               'silver': {n: {'status': 'PASS', 'uri': base + '/silver/' + n} for n in ('orders', 'baskets')}}
        model = {'status': 'PASS', 'model_run_id': 'model1', 'etl_run_id': 'etl1',
                 'etl_root': base, 'model_root': root, 'config': {'min_confidence': .2},
                 'counts': {n: 1 for n in ('training_baskets', 'frequent_itemsets', 'rules')},
                 'artifacts': {n: {'status': 'PASS', 'rows': 1, 'uri': root + '/artifacts/' + n}
                               for n in ('training_baskets', 'frequent_itemsets', 'rules')}}
        model['inputs'] = copy.deepcopy(etl['silver'])
        etl['gold'] = {'product_catalog': {'status': 'PASS', 'uri': base + '/gold/product_catalog'}}
        self.assertEqual(publication_contract(model, etl, 'model1', .3), base)
        for threshold in (.1, float('nan'), 1.1):
            with self.assertRaises(ValueError):
                publication_contract(model, etl, 'model1', threshold)
        bad = copy.deepcopy(model)
        bad['artifacts']['rules']['uri'] = base + '/wrong/rules'
        with self.assertRaises(ValueError):
            publication_contract(bad, etl, 'model1', .2)


class PublicationSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master('local[2]').appName('PublicationFixtures')
                     .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.catalog = self.spark.createDataFrame([(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')],
                                                  'product_id int, product_name string')
        self.schema = 'antecedent array<int>, consequent array<int>, support double, confidence double, lift double'
        self.rows = [([2, 1], [3], .1, .6, 2.), ([1, 2], [3], .1, .6, 2.),
                     ([1], [3], .1, .5, 2.), ([1], [4], .1, .5, 2.)]

    def rules(self, rows=None):
        return self.spark.createDataFrame(self.rows if rows is None else rows, self.schema)

    def test_stable_ids_names_lookup_and_threshold(self):
        from pyspark.sql import functions as F
        rules, lookup = build_gold(self.rules(), self.catalog, .2)
        self.assertEqual(rules.count(), 3)
        self.assertEqual(lookup.count(), 4)
        pair = rules.where(F.size('antecedent') == 2).first()
        self.assertEqual(pair.antecedent, [1, 2])
        self.assertEqual(pair.antecedent_names, ['one', 'two'])
        shuffled, _ = build_gold(self.rules(list(reversed(self.rows))), self.catalog, .2)
        self.assertEqual({r.rule_id for r in rules.collect()}, {r.rule_id for r in shuffled.collect()})
        higher, _ = build_gold(self.rules(), self.catalog, .55)
        self.assertEqual(higher.count(), 1)
        empty, index = build_gold(self.rules(), self.catalog, .9)
        self.assertEqual(empty.count(), 0)
        self.assertEqual(index.count(), 0)

    def test_catalog_and_conflicting_duplicates_fail(self):
        for catalog in (self.catalog.where('product_id != 3'), self.catalog.unionByName(self.catalog.limit(1))):
            with self.assertRaises(ValueError):
                build_gold(self.rules(), catalog, .2)
        with self.assertRaisesRegex(ValueError, 'Conflicting duplicate'):
            build_gold(self.rules(self.rows + [([1], [3], .1, .8, 2.)]), self.catalog, .2)

    def test_gold_readback_counts_and_values(self):
        from pyspark.sql import functions as F
        itemsets = self.spark.createDataFrame([([1], 2), ([1, 2], 1)], 'items array<int>, freq long')
        rules, lookup = build_gold(self.rules(), self.catalog, .2)
        counts = validate_gold(itemsets, rules, lookup, self.catalog, 10, .2)
        self.assertEqual(counts['recommendation_lookup'], 4)
        with self.assertRaises(ValueError):
            validate_gold(itemsets, rules, lookup.limit(1), self.catalog, 10, .2)
        with self.assertRaises(ValueError):
            validate_gold(itemsets, rules, lookup.unionByName(lookup.limit(1)), self.catalog, 10, .2)
        with self.assertRaises(ValueError):
            validate_gold(itemsets, rules.withColumn('antecedent_names', F.array(F.lit('wrong'))),
                          lookup, self.catalog, 10, .2)

    def test_schema_null_and_numeric_validation(self):
        for row in ([([1], 11)], [([1, None], 1)], [([1], 0)]):
            with self.assertRaises(ValueError):
                check_itemsets(self.spark.createDataFrame(row, 'items array<int>, freq long'), 10)
        with self.assertRaises(ValueError):
            check_itemsets(self.spark.createDataFrame([([1], 1)], 'items array<bigint>, freq long'), 10)
        for metric in (float('nan'), float('inf'), -1.):
            with self.assertRaises(ValueError):
                check_metrics(self.rules([([1], [3], .1, .5, metric)]))
        hdfs = Mock(base='hdfs://namenode:9000/project')
        frame = self.rules()
        hdfs.spark.read.parquet.return_value = frame
        entry = {'uri': hdfs.base + '/rules', 'schema': frame.schema.jsonValue(), 'rows': 999}
        with self.assertRaisesRegex(ValueError, 'Count mismatch'):
            checked_read(hdfs, entry)
        with self.assertRaisesRegex(ValueError, 'Schema mismatch'):
            checked_read(hdfs, dict(entry, schema={'type': 'struct', 'fields': []}))

    def test_recommend_partial_duplicate_and_tie_break(self):
        import pandas as pd
        from recommender import recommend
        rules, lookup = build_gold(self.rules(), self.catalog, .2)
        popular = pd.DataFrame(columns=['count', 'product_id', 'product_name'])
        catalog = self.catalog.toPandas()
        partial = lookup.where('antecedent_size = 2').toPandas()
        self.assertTrue(recommend([1], partial, popular, catalog).empty)
        all_rules = lookup.toPandas()
        result = recommend([1, 2], pd.concat([all_rules, all_rules]), popular, catalog)
        self.assertEqual(result.product_id.tolist(), [3, 4])
        self.assertEqual(recommend([1, 2, 3], all_rules, popular, catalog).product_id.tolist(), [4])
        self.assertEqual(recommend([2, 1], all_rules.iloc[::-1], popular, catalog).product_id.tolist(), [3, 4])

    def test_empty_rule_charts_are_honest(self):
        import tempfile
        from hdfs_pipeline.publish import charts
        empty, _ = build_gold(self.rules(), self.catalog, .9)
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            result = charts(empty, folder, .002)
            self.assertEqual([entry['rows'] for entry in result], [0, 0])
            for name in ('lift_distribution', 'top_rules'):
                self.assertTrue((folder / (name + '.png')).is_file())
                self.assertTrue((folder / (name + '.html')).is_file())

    def test_integration_sampling_and_no_applicable_rule(self):
        import tempfile
        from hdfs_pipeline.publish import integration_check
        baskets = self.spark.createDataFrame([(10, [1, 2]), (11, [1]), (12, [3, 4])],
                                             'order_id int, items array<int>')
        rules, lookup = build_gold(self.rules(), self.catalog, .2)
        with tempfile.TemporaryDirectory() as temp:
            result = integration_check(baskets, rules, lookup, self.catalog, Path(temp))
            self.assertEqual(result['status'], 'PASS')
            self.assertEqual(result['cases'], 3)
            self.assertEqual(result['cases_with_rules'], 2)
        empty, index = build_gold(self.rules(), self.catalog, .9)
        with tempfile.TemporaryDirectory() as temp:
            result = integration_check(baskets, empty, index, self.catalog, Path(temp))
            self.assertEqual(result['status'], 'NO_APPLICABLE_RULE')
            self.assertEqual(result['cases_with_rules'], 0)


if __name__ == '__main__':
    unittest.main()
