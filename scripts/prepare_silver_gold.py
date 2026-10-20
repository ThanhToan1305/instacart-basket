"""Create full Silver baskets, user-based sample and descriptive Gold tables."""
import argparse
import csv
import json
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F
from ingest_bronze import SCHEMAS, read_json_hdfs, write_json_hdfs


def build_products(bronze):
    return (bronze["products"].join(F.broadcast(bronze["aisles"]), "aisle_id")
            .join(F.broadcast(bronze["departments"]), "department_id")
            .select("product_id", "product_name", "aisle_id", "aisle", "department_id", "department"))


def build_order_items(bronze, orders):
    items = bronze["order_products__prior"].unionByName(bronze["order_products__train"])
    return items.join(orders, "order_id").select(
        "order_id", "user_id", "product_id", "add_to_cart_order", "reordered", "eval_set",
        "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order")


def build_baskets(order_items, orders):
    grouped = order_items.groupBy("order_id").agg(
        F.sort_array(F.collect_set("product_id")).alias("items"),
        F.count("*").alias("item_row_count"), F.sum("reordered").alias("reordered_item_count"))
    return (grouped.join(orders, "order_id").withColumn("basket_size", F.size("items"))
            .select("order_id", "user_id", "eval_set", "order_number", "order_dow", "order_hour_of_day",
                    "days_since_prior_order", "items", "basket_size", "item_row_count", "reordered_item_count"))


def select_users(orders, seed, sample_bps):
    # Hash explicit long inputs so the selection survives CSV/Parquet integer widths.
    return (orders.select("user_id").distinct()
            .withColumn("sample_bucket", F.pmod(F.xxhash64(F.lit(seed).cast("long"), F.col("user_id").cast("long")), F.lit(10000)))
            .filter(F.col("sample_bucket") < sample_bps))


def build_gold(orders, baskets, order_items, products, departments, kpis):
    result = {}
    for column, name in [("order_dow", "orders_by_dow"), ("order_hour_of_day", "orders_by_hour")]:
        all_orders = orders.groupBy(column).agg(F.count("*").alias("order_count"))
        labeled = baskets.groupBy(column).agg(F.count("*").alias("labeled_order_count"),
                                               F.avg("basket_size").alias("avg_labeled_basket_size"))
        result[name] = all_orders.join(labeled, column, "left").fillna({"labeled_order_count": 0})
    result["orders_by_eval_set"] = orders.groupBy("eval_set").agg(F.count("*").alias("order_count"))
    result["basket_size_distribution"] = baskets.groupBy("basket_size").agg(F.count("*").alias("order_count"))
    frequency = order_items.groupBy("product_id").agg(
        F.count("*").alias("order_count"), F.sum("reordered").alias("reordered_item_count"))
    result["product_popularity_eda"] = (products.join(frequency, "product_id", "left")
        .fillna({"order_count": 0, "reordered_item_count": 0})
        .withColumn("labeled_order_support", F.col("order_count") / F.lit(kpis["labeled_orders"]))
        .withColumn("reorder_rate", F.when(F.col("order_count") > 0, F.col("reordered_item_count") / F.col("order_count"))))
    activity = (order_items.join(F.broadcast(products.select("product_id", "department_id")), "product_id")
                .groupBy("department_id").agg(F.count("*").alias("item_occurrences"),
                                             F.sum("reordered").alias("reordered_item_count")))
    result["department_activity_eda"] = (departments.join(activity, "department_id", "left")
        .fillna({"item_occurrences": 0, "reordered_item_count": 0})
        .withColumn("item_share", F.col("item_occurrences") / F.lit(kpis["item_occurrences"]))
        .withColumn("reorder_rate", F.when(F.col("item_occurrences") > 0,
                                           F.col("reordered_item_count") / F.col("item_occurrences"))))
    return result


def export_csv(df, path):
    """Only aggregate/catalog tables enter this function, never full transactions."""
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(df.columns)
        for row in df.toLocalIterator():
            writer.writerow(list(row))


def prepare(spark, bronze, source, silver_root, gold_root, local_out, report, progress, sample_bps=100, seed=42):
    def check(name, actual, expected):
        passed = actual == expected
        report["checks"].append({"name": name, "actual": int(actual), "expected": int(expected), "pass": passed})
        progress(f"CHECK {'PASS' if passed else 'FAIL'} {name}: {actual} (expected {expected})")
        if not passed:
            raise RuntimeError(f"Reconciliation failed: {name}")

    def save(df, destination):
        df.write.mode("errorifexists").option("compression", "snappy").option("maxRecordsPerFile", 1000000).parquet(destination)
        return spark.read.parquet(destination)

    def silver(name, df):
        progress(f"SILVER_TABLE {name}")
        destination = f"{silver_root}/tables/{name}"
        result = save(df, destination)
        report["silver_tables"][name] = {"path": destination}
        return result

    products = silver("products", build_products(bronze))
    orders = silver("orders", bronze["orders"])
    check("silver.products_rows", products.count(), source["tables"]["products"]["rows"])
    check("silver.orders_rows", orders.count(), source["tables"]["orders"]["rows"])
    items = silver("order_items", build_order_items(bronze, orders))
    by_set = {r["eval_set"]: r["count"] for r in items.groupBy("eval_set").count().collect()}
    for split in ("prior", "train"):
        check(f"silver.{split}_item_rows", by_set.get(split, 0), source["tables"][f"order_products__{split}"]["rows"])
    check("silver.unexpected_item_eval_sets", sum(n for key, n in by_set.items() if key not in ("prior", "train")), 0)
    baskets = silver("baskets", build_baskets(items, orders))
    stats = baskets.agg(F.count("*").alias("n"), F.sum("basket_size").alias("items"),
                        F.min("basket_size").alias("min_size"), F.max("basket_size").alias("max_size"),
                        F.avg("basket_size").alias("avg_size"), F.sum("reordered_item_count").alias("reordered")).first()
    labeled_orders = source["orders_by_eval_set"].get("prior", 0) + source["orders_by_eval_set"].get("train", 0)
    expected_items = sum(source["tables"][f"order_products__{s}"]["rows"] for s in ("prior", "train"))
    check("silver.labeled_baskets", stats["n"], labeled_orders)
    check("silver.basket_item_total", stats["items"] or 0, expected_items)
    check("silver.unique_basket_order_ids", baskets.select("order_id").distinct().count(), labeled_orders)
    check("silver.test_baskets", baskets.filter(F.col("eval_set") == "test").count(), 0)
    invalid = ((F.col("basket_size") < 1) | (F.col("basket_size") != F.col("item_row_count"))
               | (F.col("items") != F.sort_array(F.array_distinct("items")))
               | F.expr("exists(items, x -> x is null)"))
    check("silver.invalid_baskets", baskets.filter(invalid).count(), 0)
    users = orders.select("user_id").distinct().count()
    check("silver.distinct_users", users, source["distinct_users"])
    report["kpis"] = {
        "all_orders": source["tables"]["orders"]["rows"], "distinct_users": users,
        "catalog_products": source["tables"]["products"]["rows"], "labeled_orders": stats["n"],
        "unlabeled_test_orders": source["orders_by_eval_set"].get("test", 0),
        "item_occurrences": expected_items, "avg_labeled_basket_size": stats["avg_size"],
        "min_labeled_basket_size": stats["min_size"], "max_labeled_basket_size": stats["max_size"],
        "reordered_item_occurrences": stats["reordered"], "item_reorder_rate": stats["reordered"] / expected_items,
    }

    progress("SELECT_USER_SAMPLE")
    selected = silver("sample_users", select_users(orders, seed, sample_bps))
    n_users = selected.count()
    check("sample.nonempty", int(n_users > 0), 1)
    chosen = F.broadcast(selected.select("user_id"))
    expected_orders = orders.join(chosen, "user_id", "left_semi")
    expected_baskets = baskets.join(chosen, "user_id", "left_semi")
    sample_orders = silver("sample_orders", expected_orders)
    sample_baskets = silver("sample_baskets", expected_baskets)
    check("sample.orders_missing_from_history", expected_orders.exceptAll(sample_orders).count(), 0)
    check("sample.orders_extra", sample_orders.exceptAll(expected_orders).count(), 0)
    check("sample.baskets_missing_from_history", expected_baskets.exceptAll(sample_baskets).count(), 0)
    check("sample.baskets_extra", sample_baskets.exceptAll(expected_baskets).count(), 0)
    report["sample"] = {
        "method": "pmod(xxhash64(seed_long,user_id_long),10000)<sample_bps",
        "seed_as_hash_input": seed, "sample_bps": sample_bps,
        "requested_user_fraction": sample_bps / 10000, "actual_user_fraction": n_users / users,
        "users": n_users, "orders": sample_orders.count(), "baskets": sample_baskets.count(),
        "baskets_by_eval_set": {r["eval_set"]: r["count"] for r in sample_baskets.groupBy("eval_set").count().collect()},
        "split_status": "NOT_SPLIT", "history_preserved": True,
    }

    gold = build_gold(orders, baskets, items, products, bronze["departments"], report["kpis"])
    materialized = {}
    for name, frame in gold.items():
        progress(f"GOLD_TABLE {name}")
        destination = f"{gold_root}/tables/{name}"
        materialized[name] = save(frame, destination)
        report["gold_tables"][name] = {"path": destination, "usage": "descriptive_eda_only_not_for_model_fitting"}
    for name in ("orders_by_dow", "orders_by_hour", "orders_by_eval_set"):
        total = materialized[name].agg(F.sum("order_count")).first()[0] or 0
        check(f"gold.{name}_order_total", total, report["kpis"]["all_orders"])
    check("gold.basket_size_distribution_total",
          materialized["basket_size_distribution"].agg(F.sum("order_count")).first()[0] or 0, labeled_orders)
    check("gold.product_item_total", materialized["product_popularity_eda"].agg(F.sum("order_count")).first()[0] or 0, expected_items)
    check("gold.department_item_total", materialized["department_activity_eda"].agg(F.sum("item_occurrences")).first()[0] or 0, expected_items)
    check("gold.product_catalog_rows", materialized["product_popularity_eda"].count(), report["kpis"]["catalog_products"])
    check("gold.department_catalog_rows", materialized["department_activity_eda"].count(), source["tables"]["departments"]["rows"])
    progress("EXPORT_EDA_CSV")
    csv_dir = local_out / "eda"
    csv_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in materialized.items():
        export_csv(frame, csv_dir / f"{name}.csv")
    report["top_products_eda"] = [r.asDict() for r in materialized["product_popularity_eda"]
                                  .orderBy(F.desc("order_count"), F.asc("product_id")).limit(10).collect()]
    for name, entry in report["silver_tables"].items():
        if name == "orders": entry["rows"] = report["kpis"]["all_orders"]
        elif name == "products": entry["rows"] = report["kpis"]["catalog_products"]
        elif name == "order_items": entry["rows"] = expected_items
        elif name == "baskets": entry["rows"] = labeled_orders
        else: entry["rows"] = {"sample_users": n_users, "sample_orders": report["sample"]["orders"],
                                "sample_baskets": report["sample"]["baskets"]}[name]


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", args.bronze_run):
        raise ValueError("Invalid Bronze run identifier")
    if not 1 <= args.sample_bps <= 10000 or not -(2**31) <= args.seed < 2**31:
        raise ValueError("sample-bps must be 1..10000 and seed a signed 32-bit integer")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    bronze_root = f"hdfs://namenode:9000/instacart/bronze/{args.bronze_run}"
    silver_root = f"hdfs://namenode:9000/instacart/silver/{run_id}"
    gold_root = f"hdfs://namenode:9000/instacart/gold/{run_id}"
    metadata_root = f"hdfs://namenode:9000/instacart/prepared/{run_id}"
    out = Path("/workspace/outputs/preparation") / run_id
    out.mkdir(parents=True, exist_ok=False)
    report = {"status": "RUNNING", "run_id": run_id, "source_bronze_run": args.bronze_run,
              "bronze_path": bronze_root, "silver_path": silver_root, "gold_path": gold_root,
              "prepared_path": metadata_root, "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "checks": [], "silver_tables": {}, "gold_tables": {},
              "gold_scope": "All observed labeled baskets (prior+train); all orders for order timing. EDA only.",
              "sample_scope": "Selected users with all available orders and labeled baskets; no train/validation/test split yet."}
    started = time.monotonic()

    def progress(message):
        (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(message, flush=True)

    progress(f"PREPARATION_STARTED run_id={run_id}")
    spark = None
    fs = None
    reserved = False
    try:
        spark = SparkSession.builder.appName("instacart-silver-gold").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.shuffle.partitions", "64")
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Expected project Spark Standalone cluster")
        report["spark_version"] = spark.version
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(metadata_root), spark.sparkContext._jsc.hadoopConfiguration())
        ready = read_json_hdfs(spark, fs, f"{bronze_root}/_READY.json")
        source = read_json_hdfs(spark, fs, f"{bronze_root}/_AUDIT.json")
        if (ready.get("status") != "READY" or ready.get("run_id") != args.bronze_run or source.get("status") != "PASS"
                or source.get("run_id") != args.bronze_run or not source.get("checks")
                or not all(c["pass"] for c in source["checks"])):
            raise RuntimeError("Source Bronze run is not READY with passing audit")
        report["source_snapshot"] = source["source_snapshot"]
        for root in (metadata_root, silver_root, gold_root):
            path = spark._jvm.org.apache.hadoop.fs.Path(root)
            if fs.exists(path) or not fs.mkdirs(path):
                raise RuntimeError(f"Destination already exists or cannot be created: {root}")
        write_json_hdfs(spark, fs, f"{metadata_root}/_STARTED.json", {"run_id": run_id})
        reserved = True
        bronze = {name: spark.read.parquet(f"{bronze_root}/tables/{name}") for name in SCHEMAS}
        prepare(spark, bronze, source, silver_root, gold_root, out, report, progress, args.sample_bps, args.seed)
        report.update(status="PASS", finished_at_utc=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        write_json_hdfs(spark, fs, f"{metadata_root}/_REPORT.json", report)
        progress(f"PREPARATION_ALL_CHECKS_PASS run_id={run_id}; publishing READY")
        write_json_hdfs(spark, fs, f"{metadata_root}/_READY.json", {
            "status": "READY", "run_id": run_id, "source_bronze_run": args.bronze_run,
            "silver_path": silver_root, "gold_path": gold_root})
        try:
            (out.parent / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as error:
            print(f"WARNING: latest.json not updated: {error}; use {out / 'report.json'}", flush=True)
        print(f"SILVER_GOLD_PREPARE_PASS run_id={run_id} report={out / 'report.json'}", flush=True)
    except Exception as error:
        report.update(status="FAILED", error=str(error), finished_at_utc=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        progress(f"SILVER_GOLD_PREPARE_FAILED run_id={run_id} report={out / 'report.json'}")
        if reserved:
            try:
                write_json_hdfs(spark, fs, f"{metadata_root}/_FAILED.json", report)
            except Exception:
                traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-run", required=True)
    parser.add_argument("--sample-bps", type=int, default=100, help="100 basis points = about 1%% of users")
    parser.add_argument("--seed", type=int, default=42)
    main(parser.parse_args())
