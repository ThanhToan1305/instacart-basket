"""Verify stored prediction auditing; no refit or SparkSession is needed."""
from copy import deepcopy
from recommendation_core import Recommender, evaluate
from export_dashboard import audit_saved_predictions


def main():
    pop = [{"product_id": i, "frequency": 20 - i} for i in range(1, 8)]
    rule = {"antecedent": [1], "consequent": [7], "confidence": .8, "lift": 2., "support": .2}
    cases = [{"case_id": "a", "context": [1], "target": [7]},
             {"case_id": "b", "context": [1], "target": [2]},
             {"case_id": "c", "context": [1], "target": [999]}]
    scores, records = evaluate(cases, Recommender([rule], pop))
    paired = audit_saved_predictions(records, scores)
    assert paired == {"cases": 3, "both_hit": 1, "hybrid_only_hit": 1, "popularity_only_hit": 0, "neither_hit": 1}, paired
    bad = deepcopy(records)
    bad[0]["precision_at_5"] += .1
    try:
        audit_saved_predictions(bad, scores)
    except ValueError:
        pass
    else:
        raise AssertionError("Corrupt per-case metrics were accepted")
    try:
        audit_saved_predictions(records[:-1], scores)
    except ValueError:
        pass
    else:
        raise AssertionError("Missing prediction case was accepted")
    print("DASHBOARD_EXPORT_TEST_PASS scope=synthetic_saved_prediction_audit", flush=True)


if __name__ == "__main__":
    main()
