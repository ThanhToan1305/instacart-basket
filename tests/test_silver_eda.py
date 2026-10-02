"""Independent checks of the actual published Silver/EDA artifacts."""
import bisect
import csv
import json
import unittest
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]


def read_csv(name):
    with (ROOT / 'outputs/metrics' / (name + '.csv')).open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))


class SilverEDATests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.silver = json.loads((ROOT / 'outputs/metrics/silver_profile.json').read_text())
        cls.eda = json.loads((ROOT / 'outputs/metrics/eda_summary.json').read_text())
        cls.kpi = cls.eda['kpi']

    def test_published_silver_and_foreign_keys(self):
        self.assertEqual(self.silver['status'], 'PASS')
        self.assertEqual(len(self.silver['tables']), 5)
        self.assertTrue(all(n == 0 for n in self.silver['foreign_keys'].values()))
        for name, profile in self.silver['tables'].items():
            path = ROOT / 'data/silver' / name
            self.assertTrue((path / '_SUCCESS').is_file())
            self.assertEqual(ds.dataset(path, format='parquet').schema.names, profile['columns'])
            self.assertEqual(profile['rows'], profile['final_read_rows'])
            for file in path.glob('*.parquet'):
                metadata = pq.ParquetFile(file).metadata
                for i in range(metadata.num_row_groups):
                    for j in range(metadata.num_columns):
                        self.assertEqual(metadata.row_group(i).column(j).compression, 'SNAPPY')

    def test_time_chart_totals(self):
        self.assertEqual(sum(int(r['count']) for r in read_csv('orders_by_hour')), self.kpi['total_orders'])
        self.assertEqual(sum(int(r['count']) for r in read_csv('orders_by_dow')), self.kpi['total_orders'])

    def test_histogram_mean_and_exact_median(self):
        rows = sorted(read_csv('basket_size_distribution'), key=lambda r: int(r['basket_size']))
        total = sum(int(r['count']) for r in rows)
        item_total = sum(int(r['count']) * int(r['basket_size']) for r in rows)
        self.assertEqual(total, self.kpi['observed_baskets'])
        self.assertEqual(item_total, self.kpi['total_order_items'])
        self.assertAlmostEqual(item_total / total, self.kpi['avg_basket_size'])
        cumulative, running = [], 0
        for row in rows:
            running += int(row['count'])
            cumulative.append(running)
        lower = int(rows[bisect.bisect_right(cumulative, (total - 1) // 2)]['basket_size'])
        upper = int(rows[bisect.bisect_right(cumulative, total // 2)]['basket_size'])
        self.assertEqual((lower + upper) / 2, self.kpi['median_basket_size'])

    def test_weighted_department_reorder(self):
        rows = read_csv('reorder_by_department')
        items = sum(int(r['item_count']) for r in rows)
        reordered = sum(int(r['reordered_items']) for r in rows)
        self.assertEqual(items, self.kpi['total_order_items'])
        self.assertAlmostEqual(reordered / items, self.kpi['reorder_rate'])
        for row in rows:
            self.assertAlmostEqual(int(row['reordered_items']) / int(row['item_count']), float(row['reorder_rate']))

    def test_gold_and_top_product(self):
        gold = ds.dataset(ROOT / 'data/gold/kpi_summary', format='parquet').to_table().to_pylist()
        self.assertEqual(gold, [self.kpi])  # One aggregate row, never a detail table.
        top = read_csv('top_20_products')[0]
        self.assertEqual(int(top['product_id']), self.kpi['most_purchased_product_id'])
        self.assertEqual(int(top['count']), self.kpi['most_purchased_product_count'])
        self.assertEqual(int(read_csv('eda_summary')[0]['total_orders']), self.kpi['total_orders'])

    def test_six_chart_artifacts(self):
        self.assertEqual(self.eda['status'], 'PASS')
        self.assertEqual(len(self.eda['charts']), 6)
        for chart in self.eda['charts']:
            with Image.open(chart['png']) as image:
                self.assertGreaterEqual(image.width, 1000)
                self.assertGreaterEqual(image.height, 800)
                image.verify()
            self.assertEqual(len(read_csv(chart['name'])), chart['rows'])
            self.assertIn('Plotly.newPlot', Path(chart['html']).read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
