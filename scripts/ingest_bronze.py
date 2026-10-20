"""Read a READY Raw snapshot, preserve typed rows as Parquet, audit and publish Bronze."""
import argparse
import json
import re
import time
import traceback
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F

SCHEMAS = {
    "aisles": "aisle_id INT, aisle STRING",
    "departments": "department_id INT, department STRING",
    "products": "product_id INT, product_name STRING, aisle_id INT, department_id INT",
    "orders": "order_id INT, user_id INT, eval_set STRING, order_number INT, order_dow INT, order_hour_of_day INT, days_since_prior_order DOUBLE",
    "order_products__prior": "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
    "order_products__train": "order_id INT, product_id INT, add_to_cart_order INT, reordered INT",
}
PRIMARY_KEYS = {
    "aisles": ["aisle_id"], "departments": ["department_id"],
    "products": ["product_id"], "orders": ["order_id"],
    "order_products__prior": ["order_id", "product_id"],
    "order_products__train": ["order_id", "product_id"],
}


def read_json_hdfs(spark, fs, path):
    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(fs.open(spark._jvm.org.apache.hadoop.fs.Path(path)), "UTF-8")
    )
    try:
        lines = []
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
        return json.loads("\n".join(lines))
    finally:
        reader.close()


def write_json_hdfs(spark, fs, path, report):
    stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(path), False)
    try:
        stream.write(bytearray(json.dumps(report, indent=2).encode("utf-8")))
    finally:
        stream.close()


def any_of(expressions):
    return reduce(lambda a, b: a | b, expressions, F.lit(False))


def audit_tables(tables, report, progress):
    def record(name, value, expected=0):
        report["checks"].append({"name": name, "actual": int(value), "expected": expected, "pass": value == expected})
        progress(f"CHECK {'PASS' if value == expected else 'FAIL'} {name}: {value} (expected {expected})")

    for name, df in tables.items():
        required = [c for c in df.columns if c != "days_since_prior_order"]
        invalid = [F.col(c).isNull() for c in required]
        string_cols = [f.name for f in df.schema if f.dataType.simpleString() == "string"]
        invalid += [F.length(F.trim(F.col(c))) == 0 for c in string_cols]
        positive = [c for c in df.columns if c.endswith("_id") or c in ("order_number", "add_to_cart_order")]
        invalid += [F.col(c) <= 0 for c in positive]
        if name == "orders":
            invalid += [~F.col("eval_set").isin("prior", "train", "test"),
                        ~F.col("order_dow").between(0, 6), ~F.col("order_hour_of_day").between(0, 23)]
            days = F.col("days_since_prior_order")
            invalid += [(F.col("order_number") == 1) & days.isNotNull(),
                        (F.col("order_number") > 1) & days.isNull(),
                        days.isNotNull() & (~days.between(0, 30) | F.isnan(days))]
        if name.startswith("order_products"):
            invalid += [~F.col("reordered").isin(0, 1)]
        stats = df.agg(F.count("*").alias("rows"),
                       F.sum(F.when(any_of(invalid), 1).otherwise(0)).alias("invalid_rows")).first()
        rows = stats["rows"]
        report["tables"][name]["rows"] = rows
        record(f"{name}.empty_table", int(rows == 0))
        record(f"{name}.invalid_rows", stats["invalid_rows"] or 0)
        unique = df.select(*PRIMARY_KEYS[name]).distinct().count()
        record(f"{name}.duplicate_key_rows", rows - unique)

    products = tables["products"]
    for key, parent in [("aisle_id", "aisles"), ("department_id", "departments")]:
        missing = products.join(F.broadcast(tables[parent].select(key)), key, "left_anti").count()
        record(f"products.missing_{key}", missing)

    orders = tables["orders"]
    record("orders.duplicate_user_order_number_rows",
           report["tables"]["orders"]["rows"] - orders.select("user_id", "order_number").distinct().count())
    report["orders_by_eval_set"] = {r["eval_set"]: r["count"] for r in orders.groupBy("eval_set").count().collect()}
    report["distinct_users"] = orders.select("user_id").distinct().count()
    for split in ("prior", "train"):
        name = f"order_products__{split}"
        df = tables[name]
        record(f"{name}.missing_product_id",
               df.join(F.broadcast(products.select("product_id")), "product_id", "left_anti").count())
        linked = df.join(orders.select("order_id", "eval_set"), "order_id", "left")
        stats = linked.agg(
            F.sum(F.when(F.col("eval_set").isNull(), 1).otherwise(0)).alias("missing"),
            F.sum(F.when(F.col("eval_set").isNotNull() & (F.col("eval_set") != split), 1).otherwise(0)).alias("wrong"),
        ).first()
        record(f"{name}.missing_order_id", stats["missing"] or 0)
        record(f"{name}.wrong_eval_set", stats["wrong"] or 0)
        record(f"{name}.duplicate_cart_position_rows",
               report["tables"][name]["rows"] - df.select("order_id", "add_to_cart_order").distinct().count())
        # Positions must be the unique sequence 1..basket size within each labeled order.
        cart = df.groupBy("order_id").agg(F.count("*").alias("n"),
                                           F.min("add_to_cart_order").alias("lo"),
                                           F.max("add_to_cart_order").alias("hi"))
        record(f"{name}.noncontiguous_cart_orders", cart.filter((F.col("lo") != 1) | (F.col("hi") != F.col("n"))).count())
        expected_orders = orders.filter(F.col("eval_set") == split).select("order_id")
        record(f"{name}.orders_without_items", expected_orders.join(df.select("order_id").distinct(), "order_id", "left_anti").count())


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.snapshot):
        raise ValueError("Invalid snapshot identifier")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    root = f"hdfs://namenode:9000/instacart/bronze/{run_id}"
    raw = f"hdfs://namenode:9000/instacart/raw/{args.snapshot}"
    out = Path("/workspace/outputs/ingest")
    out.mkdir(parents=True, exist_ok=True)
    local_report = out / f"{run_id}.json"
    report = {"status": "RUNNING", "run_id": run_id, "source_snapshot": args.snapshot,
              "raw_path": raw, "bronze_path": root, "tables": {}, "checks": [],
              "started_at_utc": datetime.now(timezone.utc).isoformat()}
    started = time.monotonic()

    def checkpoint(message):
        local_report.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(message, flush=True)

    checkpoint(f"INGEST_STARTED run_id={run_id}")
    spark = None
    fs = None
    reserved = False
    try:
        spark = SparkSession.builder.appName("instacart-bronze-audit").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.shuffle.partitions", "64")
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        spark.conf.set("spark.sql.csv.parser.columnPruning.enabled", "false")
        spark.conf.set("spark.sql.caseSensitive", "true")
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Expected the project Spark Standalone cluster")
        report["spark_version"] = spark.version
        report["spark_master"] = spark.sparkContext.master
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(root), spark.sparkContext._jsc.hadoopConfiguration())
        manifest = read_json_hdfs(spark, fs, f"{raw}/_READY.json")
        if (manifest.get("status") != "READY" or manifest.get("snapshot") != args.snapshot
                or manifest.get("verification") != "local_sha256_equals_hdfs_readback_sha256"
                or {f["name"] for f in manifest["files"]} != {n + ".csv" for n in SCHEMAS}):
            raise RuntimeError("Raw snapshot manifest is not valid/READY")
        report["source_manifest"] = manifest
        path = spark._jvm.org.apache.hadoop.fs.Path(root)
        if fs.exists(path):
            raise RuntimeError("Bronze destination already exists")
        if not fs.mkdirs(path):
            raise RuntimeError("Cannot create Bronze destination")
        write_json_hdfs(spark, fs, f"{root}/_STARTED.json", {"run_id": run_id})
        reserved = True
        tables = {}
        for name, schema in SCHEMAS.items():
            destination = f"{root}/tables/{name}"
            checkpoint(f"INGEST_TABLE {name}")
            df = (spark.read.schema(schema).options(header="true", enforceSchema="false",
                  mode="FAILFAST", escape='"', encoding="UTF-8", multiLine="false").csv(f"{raw}/{name}.csv"))
            # Materialize every column: malformed values must fail before audit/count optimizations.
            df.write.mode("errorifexists").option("compression", "snappy").option("maxRecordsPerFile", 1000000).parquet(destination)
            tables[name] = spark.read.parquet(destination)
            report["tables"][name] = {"path": destination, "schema": schema}
        audit_tables(tables, report, checkpoint)
        failures = [c for c in report["checks"] if not c["pass"]]
        if failures:
            raise RuntimeError(f"Data audit failed: {len(failures)} check(s). See the run report.")
        report.update(status="PASS", finished_at_utc=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        write_json_hdfs(spark, fs, f"{root}/_AUDIT.json", report)
        checkpoint(f"AUDIT_ALL_CHECKS_PASS run_id={run_id}; publishing READY")
        write_json_hdfs(spark, fs, f"{root}/_READY.json", {"status": "READY", "run_id": run_id, "source_snapshot": args.snapshot})
        # READY is authoritative. A local convenience pointer failing after publish
        # must not relabel an already-published HDFS run as FAILED.
        try:
            (out / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as error:
            print(f"WARNING: could not update latest.json: {error}; use {local_report}", flush=True)
        print(f"BRONZE_INGEST_PASS run_id={run_id} report={local_report}", flush=True)
    except Exception as error:
        report.update(status="FAILED", error=str(error), finished_at_utc=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        checkpoint(f"BRONZE_INGEST_FAILED run_id={run_id} report={local_report}")
        if reserved:
            try:
                write_json_hdfs(spark, fs, f"{root}/_FAILED.json", report)
            except Exception:
                traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default="instacart-v1")
    main(parser.parse_args())
