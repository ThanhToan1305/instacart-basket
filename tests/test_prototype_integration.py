"""Integration and DQ on real sampled baskets and all published Gold tables."""
import sys
import unittest
from pathlib import Path
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import prototype_checks as checks
from app.recommender import recommend

ROOT=checks.ROOT

class PrototypeIntegrationTests(unittest.TestCase):
    def test_bronze_traceability_metadata(self):
        for name in checks.report('raw_profile.json')['tables']:
            for file in (ROOT/'data/bronze'/name).glob('*.parquet'):
                pf=pq.ParquetFile(file)
                self.assertTrue({'run_id','source_file','ingestion_time'}.issubset(pf.schema_arrow.names))
                if pf.metadata.num_rows==0:
                    continue
                batch=next(pf.iter_batches(batch_size=32,columns=['run_id','source_file','ingestion_time']))
                self.assertTrue(all(batch.column(n).null_count==0 for n in batch.schema.names))

    def test_isolated_raw_to_gold_sample_has_real_results(self):
        manifest=ROOT/'outputs/metrics/sample_integration.json'
        if not manifest.exists():
            self.skipTest('Isolated sample run has not been requested in this workspace')
        import json
        s=json.loads(manifest.read_text());self.assertEqual(s['status'],'PASS');self.assertEqual(s['execution_mode'],'sample')
        workspace=Path(s['workspace']);profile=json.loads((workspace/'outputs/metrics/raw_profile.json').read_text())
        prototype=json.loads(Path(s['summary_path']).read_text())
        self.assertEqual(prototype['status'],'PASS');self.assertEqual(prototype['execution_mode'],'sample')
        self.assertGreater(profile['tables']['orders']['row_count'],0)
        self.assertLess(profile['tables']['orders']['row_count'],checks.report('raw_profile.json')['tables']['orders']['row_count'])
        for name,v in prototype['gold'].items():
            self.assertEqual(pq.read_table(workspace/'data/gold'/name).num_rows,v['rows'])

    def test_all_gold_full_reads_and_reconciliation(self):
        tables=checks.gold_check()['tables']
        self.assertEqual(set(tables),{'kpi_summary','association_rules','frequent_itemsets','recommendation_lookup','product_catalog'})
        self.assertEqual(tables['association_rules']['rows'],checks.report('model_summary.json')['selected']['useful_rules'])

    def test_real_sample_basket_to_recommendation(self):
        file=next((ROOT/'data/silver/baskets').glob('*.parquet'))
        batch=next(pq.ParquetFile(file).iter_batches(batch_size=64,columns=['order_id','user_id','items','basket_size']))
        samples=batch.to_pylist();self.assertGreater(len(samples),0)
        catalog=pq.read_table(ROOT/'data/gold/product_catalog').to_pandas()
        lookup=pq.read_table(ROOT/'data/gold/recommendation_lookup').to_pandas()
        import pandas as pd
        popular=pd.read_csv(ROOT/'outputs/metrics/top_20_products.csv')
        product_ids=set(catalog.product_id)
        for row in samples:
            self.assertEqual(row['basket_size'],len(row['items']))
            self.assertEqual(len(row['items']),len(set(row['items'])))
            self.assertTrue(set(row['items']).issubset(product_ids))
            result=recommend(row['items'],lookup,popular,catalog)
            self.assertLessEqual(len(result),5)
            self.assertFalse(set(row['items'])&set(result.product_id))
            self.assertTrue(set(result.product_id).issubset(product_ids))

    def test_gold_rules_reference_real_products(self):
        catalog=pq.read_table(ROOT/'data/gold/product_catalog').to_pandas()
        names=dict(zip(catalog.product_id,catalog.product_name))
        rules=pq.read_table(ROOT/'data/gold/association_rules').to_pylist()
        self.assertGreater(len(rules),0)
        for rule in rules:
            for column in ['antecedent','consequent']:
                self.assertEqual([names[i] for i in rule[column]],rule[column+'_names'])
            self.assertFalse(set(rule['antecedent'])&set(rule['consequent']))

if __name__=='__main__':unittest.main()
