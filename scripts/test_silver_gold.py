"""Synthetic Spark checks for basket semantics, full user history and EDA totals."""
import csv
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from ingest_bronze import SCHEMAS
from prepare_silver_gold import prepare, select_users, build_order_items, build_baskets


def main():
    spark = SparkSession.builder.appName("test-silver-gold-synthetic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        data = {
            "aisles": [(1, "Cold"), (2, "Bread"), (3, "Unused")],
            "departments": [(1, "Dairy"), (2, "Bakery"), (3, "Unused")],
            "products": [(1, 'Milk, "Fresh"', 1, 1), (2, "Bread", 2, 2), (3, "Unused", 3, 3)],
            "orders": [(101, 1, "prior", 1, 0, 12, None), (102, 1, "train", 2, 1, 13, 7.0),
                       (103, 2, "prior", 1, 2, 14, None), (104, 2, "test", 2, 3, 15, 3.0)],
            "order_products__prior": [(101, 2, 1, 0), (101, 1, 2, 0), (103, 1, 1, 0)],
            "order_products__train": [(102, 1, 1, 1)],
        }
        bronze = {name: spark.createDataFrame(rows, SCHEMAS[name]) for name, rows in data.items()}
        source = {"tables": {name: {"rows": len(rows)} for name, rows in data.items()},
                  "orders_by_eval_set": {"prior": 2, "train": 1, "test": 1}, "distinct_users": 2}
        default_fs = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS", "file:///")
        from uuid import uuid4
        if default_fs.startswith("hdfs:"):
            root = default_fs.rstrip("/") + "/instacart/checks/silver-test-" + uuid4().hex
        else:
            root = "file://" + tempfile.mkdtemp(prefix="silver-gold-test-")
        with tempfile.TemporaryDirectory() as folder:
            report = {"checks": [], "silver_tables": {}, "gold_tables": {}}
            prepare(spark, bronze, source, root + "/silver", root + "/gold", Path(folder), report,
                    lambda _: None, sample_bps=10000, seed=42)
            assert all(c["pass"] for c in report["checks"]), report["checks"]
            assert report["sample"]["users"] == 2
            assert report["sample"]["orders"] == 4
            assert report["sample"]["baskets"] == 3
            assert report["kpis"]["item_occurrences"] == 4
            assert report["kpis"]["item_reorder_rate"] == 0.25
            assert abs(report["kpis"]["avg_labeled_basket_size"] - 4 / 3) < 1e-12
            baskets = spark.read.parquet(root + "/silver/tables/baskets")
            actual = {r["order_id"]: r["items"] for r in baskets.select("order_id", "items").collect()}
            assert actual == {101: [1, 2], 102: [1], 103: [1]}, actual
            products = {r["product_id"]: r.asDict() for r in spark.read.parquet(root + "/gold/tables/product_popularity_eda").collect()}
            assert products[1]["order_count"] == 3 and products[1]["labeled_order_support"] == 1.0
            assert products[3]["order_count"] == 0 and products[3]["reorder_rate"] is None
            with (Path(folder) / "eda/product_popularity_eda.csv").open(encoding="utf-8-sig", newline="") as stream:
                exported = {int(r["product_id"]): r for r in csv.DictReader(stream)}
            assert exported[1]["product_name"] == 'Milk, "Fresh"'

        # Sampling uses users, independent of row/partition order and integer width.
        users = spark.range(1, 2001).select(F.col("id").cast("int").alias("user_id"))
        picked = select_users(users, 42, 100)
        reversed_long = users.select(F.col("user_id").cast("long").alias("user_id")).orderBy(F.desc("user_id")).repartition(3)
        again = select_users(reversed_long, 42, 100)
        a = {r["user_id"] for r in picked.collect()}
        b = {r["user_id"] for r in again.collect()}
        assert a and a == b, (a, b)
        history = users.crossJoin(spark.range(1, 4).select(F.col("id").alias("order_number")))
        selected_history = history.join(F.broadcast(picked.select("user_id")), "user_id", "left_semi")
        assert selected_history.count() == len(a) * 3
        assert select_users(users, 42, 10000).count() == 2000

        # A duplicate raw item must be detectable by item_row_count versus set size.
        items = build_order_items(bronze, bronze["orders"])
        duplicate = items.filter((F.col("order_id") == 101) & (F.col("product_id") == 1))
        bad = build_baskets(items.unionByName(duplicate), bronze["orders"])
        assert bad.filter(F.col("basket_size") != F.col("item_row_count")).count() == 1
        print(f"SILVER_GOLD_TEST_PASS reconciliation_checks={len(report['checks'])} scope=synthetic sample_users={len(a)}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
