"""Validate the actual training baskets, exported rules and holdout results."""
import json
import csv
import math
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pyspark.sql import functions as F
from pipeline.fpgrowth_helpers import spark_session, basket_invalid
from pipeline.silver_helpers import ROOT


class FPGrowthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.summary=json.loads((ROOT/'outputs/metrics/model_summary.json').read_text())
        cls.spark=spark_session('InstacartFPGrowthTests')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_training_baskets_are_integer_unique_and_at_least_two(self):
        baskets=self.spark.read.parquet(str(Path(self.summary['run_dir'])/'training_baskets'))
        self.assertEqual(basket_invalid(baskets,2),0)
        self.assertEqual(baskets.count(),self.summary['preparation']['training_baskets'])
        orders=self.spark.read.parquet(str(ROOT/'data/silver/orders'))
        self.assertEqual(baskets.join(orders.select('order_id','eval_set'),'order_id').where("eval_set!='prior'").count(),0)

    def test_rule_structure_and_valid_metrics(self):
        chosen=self.summary['selected']
        rules=self.spark.read.parquet(str(ROOT/'data/gold/association_rules'))
        invalid=rules.where(F.col('antecedent').isNull()|F.col('consequent').isNull()|
            (F.size('antecedent')<1)|(F.size('consequent')!=1)|
            (F.size(F.array_intersect('antecedent','consequent'))>0)|
            (F.size(F.array_distinct('antecedent'))!=F.size('antecedent'))|
            F.col('confidence').isNull()|F.col('lift').isNull()|F.col('support').isNull()|
            F.isnan('confidence')|F.isnan('lift')|F.isnan('support')|
            (F.col('confidence')<chosen['min_confidence'])|(F.col('confidence')>1)|
            (F.col('lift')<=1)|(F.col('support')<chosen['min_support'])|(F.col('support')>1))
        self.assertEqual(invalid.count(),0)
        count=rules.count()
        self.assertGreater(count,0)
        self.assertEqual(count,self.summary['final_rule_count'])
        self.assertEqual(count,rules.select('antecedent','consequent').distinct().count())
        self.assertEqual(rules.where((F.size('antecedent_names')!=F.size('antecedent'))|
                                    (F.size('consequent_names')!=F.size('consequent'))).count(),0)

    def test_evaluation_metrics_against_actual_user_results(self):
        chosen=self.summary['selected']
        users=self.spark.read.parquet(self.summary['evaluation']['per_user_path'])
        self.assertEqual(users.where((F.size('recommendations')>5)|
            (F.size('recommendations')!=F.size(F.array_distinct('recommendations')))|
            (F.size(F.array_intersect('context_items','recommendations'))>0)).count(),0)
        row=users.agg(F.count('*').alias('eligible'),F.sum(F.col('covered').cast('long')).alias('covered'),
            F.sum(F.col('hit').cast('long')).alias('hits')).first()
        self.assertEqual(row['eligible'],chosen['eligible_users'])
        self.assertEqual(row['covered'],chosen['covered_users'])
        self.assertEqual(row['hits'],chosen['hit_users'])
        self.assertAlmostEqual(row['covered']/row['eligible'],chosen['coverage'])
        self.assertAlmostEqual(row['hits']/row['eligible'],chosen['hitrate_at_5'])
        self.assertGreaterEqual(chosen['coverage'],chosen['hitrate_at_5'])
        for key in ('coverage','hitrate_at_5','avg_confidence','avg_support'):
            self.assertTrue(math.isfinite(chosen[key]))
            self.assertGreaterEqual(chosen[key],0)
            self.assertLessEqual(chosen[key],1)

    def test_lookup_index_integrity(self):
        lookup=self.spark.read.parquet(str(ROOT/'data/gold/recommendation_lookup'))
        self.assertEqual(lookup.where(~F.array_contains('antecedent',F.col('antecedent_product_id'))|
              F.array_contains('antecedent',F.col('recommended_product_id'))|
              (F.col('antecedent_size')!=F.size('antecedent'))).count(),0)
        self.assertEqual(lookup.select('rule_id').distinct().count(),self.summary['final_rule_count'])
        # Index rows are seeds for full-antecedent matching, not independent unary rules.

    def test_frequent_itemset_frequencies(self):
        itemsets=self.spark.read.parquet(str(ROOT/'data/gold/frequent_itemsets'))
        self.assertEqual(itemsets.where(F.col('items').isNull()|(F.size('items')<1)|
            (F.size('items')!=F.size(F.array_distinct('items')))|
            (F.col('freq')<=0)|(F.col('freq')>self.summary['selected']['training_baskets'])).count(),0)
        self.assertEqual(itemsets.count(),self.summary['selected']['frequent_itemsets'])

    def test_configuration_grid_and_selection(self):
        comparisons=self.summary['comparisons']
        self.assertEqual(len(comparisons),9)
        self.assertEqual(len({(r['min_support'],r['min_confidence']) for r in comparisons}),9)
        self.assertEqual(self.summary['status'],'PASS')
        candidates=[r for r in comparisons if r['status']=='PASS' and r['useful_rules']>0 and r['coverage']>0]
        self.assertEqual(self.summary['selected']['selection_score'],max(r['selection_score'] for r in candidates))

    def test_five_figures_and_aggregate_exports(self):
        from PIL import Image
        self.assertEqual(len(self.summary['charts']),5)
        for chart in self.summary['charts']:
            with Image.open(chart['png']) as image:
                image.verify()
            self.assertTrue(Path(chart['html']).is_file())
            with Path(chart['csv']).open(encoding='utf-8',newline='') as stream:
                rows=list(csv.DictReader(stream))
            self.assertEqual(len(rows),chart['rows'])
            if chart['name']=='model_lift_distribution':
                self.assertEqual(sum(int(r['count']) for r in rows),self.summary['final_rule_count'])
            elif chart['name']=='model_confidence_lift':
                self.assertEqual(sum(int(r['rule_count']) for r in rows),self.summary['final_rule_count'])
            elif chart['name']=='model_top_20_rules':
                self.assertTrue(all(float(r['support'])>=self.summary['config']['plot_min_support'] for r in rows))


if __name__=='__main__':
    unittest.main()
