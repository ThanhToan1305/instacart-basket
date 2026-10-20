"""Explicitly synthetic, small fixture for UI/export tests. Never production data."""
import csv
import json
from pathlib import Path

from dashboard_bundle import file_hash
from recommendation_core import Recommender, evaluate, digest_json


def make_fixture(parent):
    parent = Path(parent)
    root = parent / "synthetic-model"
    (root / "eda").mkdir(parents=True)
    popularity = [{"product_id": i, "frequency": 100 - i} for i in range(1, 9)]
    rules = [{"antecedent": [1], "consequent": [2], "confidence": .8, "lift": 2., "support": .2},
             {"antecedent": [2], "consequent": [3], "confidence": .7, "lift": 1.5, "support": .1}]
    cases = [{"case_id": "a", "context": [1], "target": [2]},
             {"case_id": "b", "context": [2], "target": [999]}]
    scores, predictions = evaluate(cases, Recommender(rules, popularity))
    selected = {"minSupport": .002, "minConfidence": .4, "rule_count": 2,
                "frequent_itemsets": 5, "spark_model_path": "synthetic"}
    serving = {"model_run": "synthetic-model", "source_split_run": "synthetic-split", "source_preparation_run": "synthetic-preparation",
               "protocol_sha256": "synthetic", "selected": selected, "popularity": popularity, "rules": rules}
    model = {"train_baskets": 10, "selected": selected, "validation_metrics": scores,
             "candidates": [{"minSupport": .002, "minConfidence": .4, "rule_count": 2, "validation_metrics": scores}]}
    bundle = {"data_kind": "synthetic", "model_run": "synthetic-model", "serving_sha256": digest_json(serving),
              "source_split_run": "synthetic-split", "source_preparation_run": "synthetic-preparation",
              "model_report": model, "test_report": {"test_metrics": scores}, "sample": {"users": 4},
              "kpis": {"distinct_users": 4, "all_orders": 5, "labeled_orders": 4, "catalog_products": 8,
                       "avg_labeled_basket_size": 3., "max_labeled_basket_size": 4, "item_reorder_rate": 1 / 3},
              "paired_test": {"cases": 2, "both_hit": 1, "hybrid_only_hit": 0, "popularity_only_hit": 0, "neither_hit": 1}}
    names = ["Milk", "Bread", "Eggs", "Banana", "Apple", "Rice", "Coffee", "Tea"]

    def write_csv(name, fields, rows):
        with (root / name).open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(fields)
            writer.writerows(rows)

    write_csv("catalog.csv", ["product_id", "product_name", "aisle_id", "aisle", "department_id", "department"],
              [[i, name, 1, "Aisle", 1, "Food"] for i, name in enumerate(names, 1)])
    write_csv("eda/product_popularity_eda.csv", ["product_id", "product_name", "order_count"],
              [[i, name, 9 - i] for i, name in enumerate(names, 1)])
    write_csv("eda/orders_by_hour.csv", ["order_hour_of_day", "order_count"], [[12, 3], [14, 2]])
    write_csv("eda/orders_by_dow.csv", ["order_dow", "order_count"], [[2, 3], [3, 2]])
    write_csv("eda/orders_by_eval_set.csv", ["eval_set", "order_count"], [["prior", 3], ["train", 1], ["test", 1]])
    write_csv("eda/basket_size_distribution.csv", ["basket_size", "order_count"], [[2, 1], [3, 2], [4, 1]])
    write_csv("eda/department_activity_eda.csv", ["department", "item_occurrences"], [["Food", 12]])
    (root / "serving.json").write_text(json.dumps(serving), encoding="utf-8")
    (root / "bundle.json").write_text(json.dumps(bundle), encoding="utf-8")
    manifest = {"files": {str(p.relative_to(root)): file_hash(p) for p in root.rglob("*") if p.is_file()}}
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    pointer = parent / "latest.json"
    pointer.write_text(json.dumps({"model_run": "synthetic-model", "bundle_dir": "synthetic-model",
                                   "manifest_sha256": file_hash(root / "manifest.json")}), encoding="utf-8")
    return pointer, predictions, scores
