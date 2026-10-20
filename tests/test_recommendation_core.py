"""Hand-calculated ranking/metric checks independent of Spark."""
import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from recommendation_core import Recommender, case_metrics, evaluate, selection_key, digest_json


def rule(a, c, confidence, lift=2.0, support=0.2):
    return {"antecedent": a, "consequent": [c], "confidence": confidence, "lift": lift, "support": support}


class RecommenderTests(unittest.TestCase):
    def setUp(self):
        self.pop = [{"product_id": p, "frequency": 100 - p} for p in range(1, 9)]

    def test_full_antecedent_required(self):
        engine = Recommender([rule([1, 2], 3, .9), rule([1], 4, .8)], self.pop)
        self.assertEqual(engine.recommend([1])["fpgrowth_rule_only"], [4])

    def test_best_rule_dedup_context_exclusion_and_hybrid_order(self):
        engine = Recommender([rule([1, 2], 3, .9), rule([1], 4, .8), rule([2], 4, .95), rule([1], 2, .99)], self.pop)
        result = engine.recommend([1, 2])
        self.assertEqual(result["fpgrowth_rule_only"], [4, 3])
        self.assertEqual(result["popularity"], [3, 4, 5, 6, 7])
        self.assertEqual(result["hybrid"], [4, 3, 5, 6, 7])
        self.assertEqual(engine.recommend([1, 2], details=True)["hybrid"][0]["antecedent"], [2])

    def test_no_rules_fallback_and_short_lists(self):
        engine = Recommender([], self.pop)
        result = engine.recommend([1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(result["fpgrowth_rule_only"], [])
        self.assertEqual(result["hybrid"], [8])
        self.assertEqual(result["popularity"], [8])

    def test_rule_tie_uses_product_id(self):
        engine = Recommender([rule([1], 4, .8), rule([1], 3, .8)], self.pop)
        self.assertEqual(engine.recommend([1])["fpgrowth_rule_only"], [3, 4])

    def test_hand_calculated_metrics(self):
        scores = case_metrics([10, 20, 30], [20, 40])
        self.assertEqual(scores["hit_rate_at_5"], 1)
        self.assertEqual(scores["precision_at_5"], .2)
        self.assertEqual(scores["recall_at_5"], .5)
        self.assertAlmostEqual(scores["ndcg_at_5"], (1 / math.log2(3)) / (1 + 1 / math.log2(3)))
        self.assertEqual(scores["coverage_at_5"], 1)
        self.assertEqual(scores["full_list_rate_at_5"], 0)
        self.assertTrue(all(value == 0 for value in case_metrics([], [999]).values()))

    def test_zero_hit_and_unseen_targets_remain_in_denominator(self):
        cases = [{"case_id": "a", "context": [1], "target": [2]}, {"case_id": "b", "context": [1], "target": [999]}]
        scores, records = evaluate(cases, Recommender([], self.pop))
        self.assertEqual(scores["popularity"]["cases"], 2)
        self.assertEqual(scores["popularity"]["hit_rate_at_5"], .5)
        self.assertEqual(scores["fpgrowth_rule_only"]["coverage_at_5"], 0)
        a = [r["recommendations"] for r in records if r["case_id"] == "a"]
        b = [r["recommendations"] for r in records if r["case_id"] == "b"]
        self.assertEqual(a, b)

    def test_selection_tie_breaks(self):
        def candidate(count, support, confidence):
            return {"validation_metrics": {"hybrid": {"hit_rate_at_5": .5, "recall_at_5": .3}},
                    "rule_count": count, "minSupport": support, "minConfidence": confidence}
        a, b, c = candidate(4, .001, .2), candidate(3, .001, .6), candidate(3, .005, .4)
        self.assertIs(min([a, b, c], key=selection_key), c)

    def test_bad_prediction_lists_and_bad_rules_are_rejected(self):
        with self.assertRaises(ValueError):
            case_metrics([1, 1], [1])
        with self.assertRaises(ValueError):
            Recommender([rule([1], 999, .9)], self.pop)
        with self.assertRaises(ValueError):
            Recommender([rule([1], 2, float("nan"))], self.pop)

    def test_digest_ignores_dictionary_key_order(self):
        self.assertEqual(digest_json({"a": 1, "b": 2}), digest_json({"b": 2, "a": 1}))


if __name__ == "__main__":
    unittest.main()
