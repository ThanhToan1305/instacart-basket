"""Fixtures test logic only; never model evidence."""
import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'src'))
from hdfs_pipeline.helpers import Hdfs, source_manifest, valid_id, schema_signature
from hdfs_pipeline.train import validate_config


class ContractTests(unittest.TestCase):
    def test_existing_hdfs_output_is_rejected_before_write(self):
        hdfs = Hdfs.__new__(Hdfs)
        hdfs.base = 'hdfs://namenode:9000/project/models/run1'
        hdfs.exists = lambda suffix: True
        with self.assertRaisesRegex(ValueError, 'refusing overwrite'):
            hdfs.save(None, 'artifacts/rules', 10)

    def test_run_id_rejects_traversal(self):
        for value in ('../x', '/x', '', 'x/y'):
            with self.assertRaises(ValueError):
                valid_id(value)

    def test_lineage(self):
        root = 'hdfs://namenode:9000/instacart/runs/etl1'
        report = {'run_id': 'etl1', 'status': 'PASS', 'hdfs_root': root,
                  'silver': {n: {'status': 'PASS', 'uri': root + '/silver/' + n}
                             for n in ('orders', 'baskets')}}
        self.assertEqual(source_manifest(report, 'etl1'), root)
        for change in ('status', 'uri', 'root'):
            bad = copy.deepcopy(report)
            if change == 'status':
                bad['status'] = 'FAIL'
            elif change == 'uri':
                bad['silver']['orders']['uri'] = 'file:///tmp/orders'
            else:
                bad['hdfs_root'] = 'file:///tmp/etl1'
            with self.assertRaises(ValueError):
                source_manifest(bad, 'etl1')
        with self.assertRaises(ValueError):
            source_manifest(report, 'etl1', root + '/other')

    def test_schema_preserves_element_type(self):
        a = {'type': 'array', 'elementType': 'integer', 'containsNull': False}
        b = dict(a, containsNull=True)
        self.assertEqual(schema_signature(a), schema_signature(b))
        self.assertNotEqual(schema_signature(a), schema_signature(dict(b, elementType='long')))

    def test_invalid_config(self):
        cfg = dict(min_support=.002, min_confidence=.2, num_partitions=8,
                   max_itemsets=100, max_rules=100, worker_timeout_seconds=60)
        validate_config(cfg)
        for key, value in [('min_support', float('nan')), ('min_confidence', 0), ('num_partitions', 0)]:
            with self.assertRaises(ValueError):
                validate_config(dict(cfg, **{key: value}))


class BasketTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = SparkSession.builder.master('local[2]').appName('HdfsModelUnitFixtures').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_prior_filter_and_invalid_inputs(self):
        from hdfs_pipeline.train import prepare_training
        orders = self.spark.createDataFrame([(1, 10, 'prior'), (2, 10, 'train'), (3, 11, 'prior')],
                                            'order_id int, user_id int, eval_set string')
        rows = [(1, 10, [1, 2], 2), (2, 10, [2, 3], 2), (3, 11, [1], 1)]
        schema = 'order_id int, user_id int, items array<int>, basket_size int'
        baskets = self.spark.createDataFrame(rows, schema)
        train, counts = prepare_training(baskets, orders)
        self.assertEqual(train.first().order_id, 1)
        self.assertEqual(counts['training_baskets'], 1)
        self.assertEqual(counts['excluded_single_item_baskets'], 1)
        invalid = [rows + [rows[0]], [(1, 99, [1, 2], 2)], [(1, 10, [1, 1], 2)],
                   [(1, 10, [1, None], 2)], [(1, 10, [0, 2], 2)], [(1, 10, [1, 2], 3)]]
        for values in invalid:
            with self.assertRaises(ValueError):
                prepare_training(self.spark.createDataFrame(values, schema), orders)
        with self.assertRaises(ValueError):
            prepare_training(self.spark.createDataFrame(rows,
                             schema.replace('array<int>', 'array<bigint>')), orders)


if __name__ == '__main__':
    unittest.main()
