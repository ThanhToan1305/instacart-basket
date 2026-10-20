"""Synthetic tests for user isolation, latest-basket eligibility and deterministic masking."""
import tempfile
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F
from split_evaluation import assign_users, latest_heldout_baskets, mask_cases, validate_cases, build_split_dataset, protocol


def main():
    spark = SparkSession.builder.appName("test-evaluation-split-synthetic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        users = spark.range(1, 121).select(F.col("id").cast("int").alias("user_id"))
        splits = assign_users(users, 42)
        excluded_user = splits.filter(F.col("split") == "validation").orderBy("user_id").first()["user_id"]
        rows = []
        for user in range(1, 121):
            if user == excluded_user:
                continue
            rows.append((user * 10 + 1, user, "prior", 1, [1, 2, 3], 3))
            items = [2, 3, 4] if user % 10 else [2]
            rows.append((user * 10 + 2, user, "prior", 2, items, len(items)))
            if user % 2:
                items = [10000 + user * 10 + j for j in range(4)]
                rows.append((user * 10 + 3, user, "train", 3, items, 4))
        baskets = spark.createDataFrame(rows,
            "order_id INT, user_id INT, eval_set STRING, order_number INT, items ARRAY<INT>, basket_size INT")
        source = {"sample": {"users": 120, "baskets": len(rows)}}
        default_fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS", "file:///")
        if default_fs.startswith("hdfs:"):
            root = default_fs.rstrip("/") + "/instacart/checks/split-test-" + uuid4().hex
        else:
            root = "file://" + tempfile.mkdtemp(prefix="evaluation-split-test-")
        report = {"checks": [], "tables": {}}
        tables = build_split_dataset(spark, users, baskets, source, root, report, lambda _: None, seed=42)
        assert all(c["pass"] for c in report["checks"]), report["checks"]
        assert sum(report["user_counts"].values()) == 120
        assert tables["train_baskets"].filter(F.col("eval_set") != "prior").count() == 0
        assert tables["train_vocabulary"].filter(F.col("product_id") > 10000).count() == 0
        # Unseen targets survive and remain in the eventual metric denominator.
        assert all(report["case_counts"][name]["unseen_target_item_occurrences"] > 0 for name in ("validation", "test"))
        excluded = tables["case_eligibility"].filter(F.col("user_id") == excluded_user).first()
        assert excluded["exclusion_reason"] == "no_observed_labeled_basket" and not excluded["eligible"]
        single = tables["case_eligibility"].filter(F.col("exclusion_reason") == "single_item_latest_basket")
        assert single.count() > 0
        assert all(r["order_number"] == 2 for r in single.collect())

        # User assignment and mask are invariant to input row/array ordering and partitions.
        again_users = assign_users(users.select(F.col("user_id").cast("long").alias("user_id")).repartition(3), 42)
        assert splits.select("user_id", "split").exceptAll(again_users.select("user_id", "split")).count() == 0
        latest = latest_heldout_baskets(baskets, splits)
        first = mask_cases(latest, 42)
        second = mask_cases(latest.withColumn("items", F.reverse("items")).repartition(3), 42)
        columns = ["case_id", "context", "target"]
        assert first.select(*columns).exceptAll(second.select(*columns)).count() == 0
        assert second.select(*columns).exceptAll(first.select(*columns)).count() == 0
        assert validate_cases(first) == 0
        assert validate_cases(first.withColumn("target", F.col("context"))) > 0
        assert first.filter(F.size("context") == 1).count() > 0
        assert protocol(42)["mask"]["retain_unseen_targets"] is True
        print(f"EVALUATION_SPLIT_TEST_PASS checks={len(report['checks'])} scope=synthetic_user_isolation_masking_and_cold_targets", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
