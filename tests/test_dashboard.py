"""Real Gold data and Streamlit UI integration checks; no synthetic dataset."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import streamlit as st
from streamlit.testing.v1 import AppTest
from app import data_loader
from app.data_loader import gold, metrics, DataUnavailable
from app.recommender import recommend

ROOT = Path(__file__).resolve().parents[1]

class DashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog=gold('product_catalog')
        cls.lookup=gold('recommendation_lookup')
        cls.popular=metrics('top_20_products.csv')

    def test_full_rule_match_and_ranking(self):
        for basket in [list(x) for x in self.lookup.antecedent.head(30)]:
            result=recommend(basket,self.lookup,self.popular,self.catalog)
            self.assertLessEqual(len(result),5)
            self.assertEqual(len(result),result.product_id.nunique())
            self.assertFalse(set(basket)&set(result.product_id))
            for row in result.to_dict('records'):
                if row['source']=='Luật kết hợp':
                    rule=self.lookup[self.lookup.rule_id==row['rule_id']].iloc[0]
                    self.assertTrue(set(rule.antecedent).issubset(basket))
                    self.assertEqual(row['score'],float(rule.confidence))
            if not result.empty and result.iloc[0]['source']=='Luật kết hợp':
                keys=[(-r.confidence,-r.lift,-r.support,r.product_id) for r in result.itertuples()]
                self.assertEqual(keys,sorted(keys))

    def test_partial_antecedent_does_not_fire(self):
        rule=self.lookup[self.lookup.antecedent_size>1].iloc[0]
        only_rule=self.lookup[self.lookup.rule_id==rule.rule_id]
        result=recommend(list(rule.antecedent)[:-1],only_rule,self.popular,self.catalog)
        self.assertTrue((result.source=='Fallback phổ biến').all())

    def test_fallback_real_counts_no_invented_metrics(self):
        rule_products=set(int(x) for a in self.lookup.antecedent for x in a)
        product=next(int(x) for x in self.catalog.product_id if int(x) not in rule_products)
        basket=[product,int(self.popular.iloc[0].product_id)]
        # Isolate the real unmatched product to ensure no matched rule.
        result=recommend([product],self.lookup,self.popular,self.catalog)
        self.assertTrue((result.source=='Fallback phổ biến').all())
        self.assertTrue(result[['score','support','confidence','lift']].isna().all().all())
        expected=self.popular.sort_values(['count','product_id'],ascending=[False,True])
        self.assertEqual(result.product_id.tolist(),expected[~expected.product_id.isin([product])].head(5).product_id.tolist())
        empty_lookup=self.lookup.iloc[:0]
        excluded=recommend(basket,empty_lookup,self.popular,self.catalog)
        self.assertFalse(set(excluded.product_id)&set(basket))

    def test_all_pages_render_actual_data(self):
        app=AppTest.from_file(str(ROOT/'app/dashboard.py'),default_timeout=30).run()
        self.assertEqual(len(app.exception),0)
        self.assertEqual(len(app.error),0)
        for page in ['Hành vi mua sắm','Luật kết hợp','Gợi ý sản phẩm','Hiệu năng hệ thống']:
            app.sidebar.radio[0].set_value(page).run()
            self.assertEqual(len(app.exception),0,page)
            self.assertEqual(len(app.error),0,page)
        self.assertTrue(any('Cấu hình Spark' in h.value for h in app.subheader))

    def test_rule_filter_empty_and_recommendation_ui(self):
        app=AppTest.from_file(str(ROOT/'app/dashboard.py'),default_timeout=30).run()
        app.sidebar.radio[0].set_value('Luật kết hợp').run()
        app.number_input[0].set_value(1.0).run()
        self.assertEqual(len(app.exception),0)
        self.assertEqual(app.metric[0].value,'0')
        app.sidebar.radio[0].set_value('Gợi ý sản phẩm').run()
        basket=[int(x) for x in self.lookup.iloc[0].antecedent]
        app.multiselect[0].set_value(basket).run()
        self.assertEqual(len(app.exception),0)
        self.assertGreater(len(app.dataframe[0].value),0)
        self.assertFalse(set(basket)&set(app.dataframe[0].value.product_id))
        rule_products=set(int(x) for a in self.lookup.antecedent for x in a)
        product=next(int(x) for x in self.catalog.product_id if int(x) not in rule_products)
        app.multiselect[0].set_value([product]).run()
        self.assertEqual(len(app.exception),0)
        self.assertTrue(any('Fallback:' in w.value for w in app.warning))

    def test_missing_inputs_do_not_crash(self):
        with tempfile.TemporaryDirectory() as folder,patch.object(data_loader,'ROOT',Path(folder)):
            st.cache_data.clear()
            app=AppTest.from_file(str(ROOT/'app/dashboard.py'),default_timeout=30).run()
            for page in ['Tổng quan','Hành vi mua sắm','Luật kết hợp','Gợi ý sản phẩm','Hiệu năng hệ thống']:
                app.sidebar.radio[0].set_value(page).run()
                self.assertEqual(len(app.exception),0,page)
                self.assertGreater(len(app.warning),0,page)
                self.assertGreater(len(app.code),0,page)
        st.cache_data.clear()

    def test_loader_blocks_raw_silver_and_catalog_matches_kpi(self):
        with self.assertRaises(DataUnavailable):data_loader.load('data/silver/products')
        with self.assertRaises(DataUnavailable):data_loader.load('data/raw/orders.csv')
        summary=metrics('eda_summary.json')
        self.assertEqual(summary['kpi']['total_products'],len(self.catalog))

if __name__=='__main__':unittest.main()
