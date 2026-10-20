"""Small real Spark FP-Growth fit, model save/load, grid and serving roundtrip."""
import json
import tempfile
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowthModel
from recommendation_core import Recommender, evaluate, digest_json
from split_evaluation import protocol
from train_validate import fit_grid, collect_rules


def main():
    spark = SparkSession.builder.appName("test-fpgrowth-pipeline-synthetic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        train = spark.createDataFrame([([1, 2, 3],), ([1, 2],), ([1, 3],), ([1, 2, 3],),
                                       ([2, 4],), ([2, 4],), ([5],)], "items ARRAY<INT>").cache()
        cases = [{"case_id": "a", "context": [1], "target": [2, 3]},
                 {"case_id": "b", "context": [2], "target": [4]},
                 {"case_id": "cold", "context": [5], "target": [999]}]
        default_fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS", "file:///")
        root = (default_fs.rstrip("/") + "/instacart/checks/model-test-" + uuid4().hex if default_fs.startswith("hdfs:")
                else "file://" + tempfile.mkdtemp(prefix="model-pipeline-test-") + "/models")
        report = {}
        pop, rules, _ = fit_grid(spark, train, cases, protocol(42), root, report, lambda _: None)
        assert len(report["candidates"]) == 9
        assert len(report["fit_runs"]) == 3
        assert {row["product_id"] for row in pop} == {1, 2, 3, 4, 5}
        assert next(row for row in pop if row["product_id"] == 2)["frequency"] == 5
        model = FPGrowthModel.load(root + "/selected_model")
        loaded = collect_rules(model.associationRules)
        match = next(r for r in loaded if r["antecedent"] == [1] and r["consequent"] == [2])
        assert abs(match["confidence"] - .75) < 1e-12
        assert abs(match["support"] - 3 / 7) < 1e-12
        serving = {"rules": rules, "popularity": pop}
        restored = json.loads(json.dumps(serving))
        assert digest_json(serving) == digest_json(restored)
        scores, _ = evaluate(cases, Recommender(restored["rules"], restored["popularity"]))
        assert scores == report["validation_metrics"]
        assert scores["hybrid"]["cases"] == 3
        assert abs(scores["hybrid"]["hit_rate_at_5"] - 2 / 3) < 1e-12
        assert scores["hybrid"]["coverage_at_5"] == 1
        train.unpersist()
        print("MODEL_PIPELINE_TEST_PASS scope=synthetic_spark_fit_save_load_grid_and_serving", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
