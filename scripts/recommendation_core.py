"""Deterministic context-only serving and ranking metrics; standard Python only."""
import hashlib
import json
import math
from collections import defaultdict

MODELS = ("popularity", "fpgrowth_rule_only", "hybrid")
METRICS = ("hit_rate_at_5", "precision_at_5", "recall_at_5", "ndcg_at_5", "coverage_at_5", "full_list_rate_at_5")


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def rule_key(rule):
    # Antecedent resolves explanation ties for the same product without changing product rank.
    return (-rule["confidence"], -rule["lift"], -rule["support"], rule["consequent"][0], tuple(rule["antecedent"]))


class Recommender:
    def __init__(self, rules, popularity):
        self.popularity = sorted(popularity, key=lambda row: (-row["frequency"], row["product_id"]))
        self.frequencies = {int(row["product_id"]): int(row["frequency"]) for row in self.popularity}
        if len(self.frequencies) != len(popularity) or any(p <= 0 or n <= 0 for p, n in self.frequencies.items()):
            raise ValueError("Invalid/duplicate popularity entries")
        self.buckets = defaultdict(list)
        for rule in rules:
            a, c = rule["antecedent"], rule["consequent"]
            if not a or len(a) != len(set(a)) or len(c) != 1 or c[0] in a:
                raise ValueError("Rules must have unique nonempty antecedents and one new consequent")
            if any(p not in self.frequencies for p in a + c):
                raise ValueError("Rule contains an item outside the training vocabulary")
            if (not all(math.isfinite(rule[key]) for key in ("confidence", "lift", "support"))
                    or not 0 <= rule["confidence"] <= 1 or not 0 < rule["support"] <= 1 or rule["lift"] <= 0):
                raise ValueError("Invalid rule statistics")
            normalized = dict(rule, antecedent=sorted(a))
            self.buckets[min(a)].append((frozenset(a), normalized))

    def recommend(self, context, k=5, details=False):
        if k != 5:
            raise ValueError("This frozen protocol uses k=5")
        existing = set(context)
        best = {}
        for anchor in existing:
            for antecedent, rule in self.buckets.get(anchor, ()):
                product = rule["consequent"][0]
                if product not in existing and antecedent.issubset(existing):
                    if product not in best or rule_key(rule) < rule_key(best[product]):
                        best[product] = rule
        ranked = sorted(best.values(), key=rule_key)[:k]
        rule_rows = [{"product_id": r["consequent"][0], "source": "rule", "antecedent": r["antecedent"],
                      "confidence": r["confidence"], "lift": r["lift"], "support": r["support"]} for r in ranked]
        pop_rows = []
        for row in self.popularity:
            if row["product_id"] not in existing:
                pop_rows.append({"product_id": row["product_id"], "source": "popularity", "frequency": row["frequency"]})
                if len(pop_rows) == k:
                    break
        hybrid = list(rule_rows)
        used = existing | {row["product_id"] for row in hybrid}
        # Iterate the complete popularity ranking, since rule candidates may overlap its prefix.
        for row in self.popularity:
            if len(hybrid) == k:
                break
            if row["product_id"] not in used:
                hybrid.append({"product_id": row["product_id"], "source": "popularity", "frequency": row["frequency"]})
                used.add(row["product_id"])
        result = {"popularity": pop_rows, "fpgrowth_rule_only": rule_rows, "hybrid": hybrid}
        return result if details else {name: [row["product_id"] for row in rows] for name, rows in result.items()}


def case_metrics(predictions, target, k=5):
    if k != 5 or not target or len(target) != len(set(target)):
        raise ValueError("Expected k=5 and a nonempty unique target")
    if len(predictions) > k or len(predictions) != len(set(predictions)):
        raise ValueError("Invalid recommendation list")
    truth = set(target)
    hits = sum(p in truth for p in predictions)
    dcg = sum(1 / math.log2(rank + 1) for rank, product in enumerate(predictions, 1) if product in truth)
    ideal = sum(1 / math.log2(rank + 1) for rank in range(1, min(k, len(target)) + 1))
    return {"hit_rate_at_5": float(hits > 0), "precision_at_5": hits / k,
            "recall_at_5": hits / len(target), "ndcg_at_5": dcg / ideal,
            "coverage_at_5": float(bool(predictions)), "full_list_rate_at_5": float(len(predictions) == k)}


def evaluate(cases, recommender):
    if not cases or len({c["case_id"] for c in cases}) != len(cases):
        raise ValueError("Evaluation cases must be nonempty and unique")
    sums = {name: {metric: 0.0 for metric in METRICS} for name in MODELS}
    records = []
    for case in cases:
        context, target = case["context"], case["target"]
        if not context or len(context) != len(set(context)) or set(context) & set(target):
            raise ValueError("Invalid context/target partition")
        # Target is deliberately never passed to the serving function.
        recommendations = recommender.recommend(context)
        for name in MODELS:
            predicted = recommendations[name]
            if set(predicted) & set(context):
                raise ValueError("Recommendation contains an existing context product")
            scores = case_metrics(predicted, target)
            for metric in METRICS:
                sums[name][metric] += scores[metric]
            records.append({"case_id": case["case_id"], "model": name, "context": context,
                            "target": target, "recommendations": predicted, **scores})
    return {name: {"cases": len(cases), **{key: value / len(cases) for key, value in scores.items()}}
            for name, scores in sums.items()}, records


def selection_key(candidate):
    hybrid = candidate["validation_metrics"]["hybrid"]
    return (-hybrid["hit_rate_at_5"], -hybrid["recall_at_5"], candidate["rule_count"],
            -candidate["minSupport"], -candidate["minConfidence"])
