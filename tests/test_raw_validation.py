"""Tests use the actual supplied CSVs; no fabricated dataset."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from common.spark_session import create_spark_session
from pipeline.raw_validation import ROOT, TABLES, inspect_files, read_raw, profile_frame


class FileValidationTests(unittest.TestCase):
    def test_missing_files_lists_all_exact_paths(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError) as caught:
                inspect_files(folder)
            for spec in TABLES.values():
                self.assertIn(str(Path(folder) / spec['file']), str(caught.exception))

    def test_actual_headers_and_sizes(self):
        results = inspect_files(ROOT / 'data/raw')
        self.assertEqual(len(results), 6)
        for name, profile in results.items():
            self.assertGreater(profile['size_bytes'], 0)
            self.assertEqual(profile['header'], TABLES[name]['schema'].fieldNames())
            self.assertTrue(profile['header_matches'])

    def test_composite_keys(self):
        for name in ('order_products_prior', 'order_products_train'):
            self.assertEqual(TABLES[name]['keys'], ['order_id', 'product_id'])


class SparkValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session('InstacartRawValidationTests')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_real_dimension_tables(self):
        for name in ('aisles', 'departments'):
            with self.subTest(table=name):
                result = profile_frame(read_raw(self.spark, name, ROOT / 'data/raw'), name)
                self.assertEqual(result['status'], 'PASS')
                self.assertEqual(result['duplicate_excess_rows'], 0)
                self.assertTrue(all(result['checks'].values()))

    def test_detect_duplicates_in_repeated_actual_rows(self):
        df = read_raw(self.spark, 'aisles', ROOT / 'data/raw')
        result = profile_frame(df.unionByName(df), 'aisles')
        self.assertEqual(result['status'], 'FAIL')
        self.assertEqual(result['duplicate_excess_rows'], df.count())
        self.assertFalse(result['checks']['duplicate_keys'])

    def test_product_csv_quoted_names_preserved(self):
        import csv
        with (ROOT / 'data/raw/products.csv').open(newline='', encoding='utf-8') as stream:
            expected = next(row['product_name'] for row in csv.DictReader(stream) if row['product_id'] == '6816')
        df = read_raw(self.spark, 'products', ROOT / 'data/raw')
        actual = df.where('product_id = 6816').select('product_name').first()
        self.assertIsNotNone(actual)
        self.assertEqual(actual['product_name'], expected)

    def test_detect_null_and_invalid_key_on_actual_rows(self):
        from pyspark.sql import functions as F
        df = read_raw(self.spark, 'aisles', ROOT / 'data/raw')
        for value, check in [(None, 'required_nulls'), (0, 'domains')]:
            with self.subTest(value=value):
                altered = df.withColumn('aisle_id', F.lit(value).cast('int'))
                result = profile_frame(altered, 'aisles')
                self.assertEqual(result['status'], 'FAIL')
                self.assertFalse(result['checks'][check])


if __name__ == '__main__':
    unittest.main()
