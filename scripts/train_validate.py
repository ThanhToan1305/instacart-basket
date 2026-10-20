"""Fit Spark FP-Growth and select a configuration using validation only."""
import argparse
import csv
import json
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.ml.fpm import FPGrowth, FPGrowthModel
from pyspark.sql import SparkSession, functions as F
from ingest_bronze import read_json_hdfs, write_json_hdfs
from split_evaluation import protocol, validate_cases
from recommendation_core import Recommender, MODELS, METRICS, digest_json, evaluate, selection_key

MAX_RULES = 50000


def collect_rules(frame):
    count = frame.count()
    if count > MAX_RULES:
        raise RuntimeError(f"{count} rules exceed driver export limit {MAX_RULES}; no pruning was applied")
    return [{"antecedent": sorted(row["antecedent"]), "consequent": list(row["consequent"]),
             "confidence": row["confidence"], "lift": row["lift"], "support": row["support"]} for row in frame.collect()]


def fit_grid(spark, train, validation_cases, spec, root, report, progress):
    """No test-case parameter or test read exists in this function."""
    n_train = train.count()
    popularity_df = train.select(F.explode("items").alias("product_id")).groupBy("product_id").agg(F.count("*").alias("frequency"))
    popularity_df.write.mode("errorifexists").parquet(root + "/popularity")
    popularity = [row.asDict() for row in popularity_df.orderBy(F.desc("frequency"), F.asc("product_id")).collect()]
    report["train_baskets"] = n_train
    report["vocabulary_products"] = len(popularity)
    report["candidates"] = []
    report["fit_runs"] = []
    for support in spec["grid"]["minSupport"]:
        progress(f"FIT_FP_GROWTH minSupport={support}")
        key = str(support).replace(".", "p")
        path = root + "/candidates/support_" + key
        started = time.monotonic()
        model = FPGrowth(itemsCol="items", minSupport=support,
                         minConfidence=min(spec["grid"]["minConfidence"]), numPartitions=2).fit(train.select("items"))
        itemsets = model.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK)
        itemset_count = itemsets.count()
        fit_seconds = time.monotonic() - started
        rules_df = model.associationRules.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            rules = collect_rules(rules_df)
            model.write().save(path + "/spark_model")
            rules_df.write.mode("errorifexists").parquet(path + "/base_rules")
            report["fit_runs"].append({"minSupport": support, "frequent_itemsets": itemset_count,
                                       "fit_and_itemsets_seconds": fit_seconds, "base_rule_count": len(rules)})
            for confidence in spec["grid"]["minConfidence"]:
                chosen = [r for r in rules if r["confidence"] >= confidence]
                scores, _ = evaluate(validation_cases, Recommender(chosen, popularity))
                candidate = {"minSupport": support, "minConfidence": confidence, "rule_count": len(chosen),
                             "frequent_itemsets": itemset_count, "spark_model_path": path + "/spark_model",
                             "base_rules_path": path + "/base_rules", "validation_metrics": scores}
                report["candidates"].append(candidate)
                progress(f"VALIDATION_CONFIG support={support} confidence={confidence} rules={len(chosen)} hybrid_HR@5={scores['hybrid']['hit_rate_at_5']:.6f}")
        finally:
            rules_df.unpersist()
            itemsets.unpersist()
    best = min(report["candidates"], key=selection_key)
    selected_model = FPGrowthModel.load(best["spark_model_path"]).setMinConfidence(best["minConfidence"])
    selected_model.write().save(root + "/selected_model")
    selected_rules = collect_rules(selected_model.associationRules)
    if len(selected_rules) != best["rule_count"]:
        raise RuntimeError("Selected model rule count differs from the evaluated rules")
    scores, records = evaluate(validation_cases, Recommender(selected_rules, popularity))
    for name in MODELS:
        for metric in METRICS:
            if abs(scores[name][metric] - best["validation_metrics"][name][metric]) > 1e-12:
                raise RuntimeError("Selected Spark model cannot reproduce validation scores")
    report["selected"] = {"minSupport": best["minSupport"], "minConfidence": best["minConfidence"],
                          "rule_count": len(selected_rules), "frequent_itemsets": best["frequent_itemsets"],
                          "spark_model_path": root + "/selected_model"}
    report["validation_metrics"] = scores
    return popularity, selected_rules, records


def write_metrics_csv(path, metrics):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["model", "cases", *METRICS])
        writer.writeheader()
        for name in MODELS:
            writer.writerow({"model": name, **metrics[name]})


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", args.split_run):
        raise ValueError("Invalid split run identifier")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    root = f"hdfs://namenode:9000/instacart/models/{run_id}"
    split_root = f"hdfs://namenode:9000/instacart/evaluation/{args.split_run}"
    out = Path("/workspace/outputs/models") / run_id
    out.mkdir(parents=True, exist_ok=False)
    report = {"status": "RUNNING", "run_id": run_id, "source_split_run": args.split_run,
              "hdfs_path": root, "test_status": "NOT_EVALUATED", "checks": [],
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "execution_scope": "Spark fits FP-Growth; Python driver ranks bounded exported rules and evaluates the sample."}
    started = time.monotonic()

    def progress(message):
        (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(message, flush=True)

    def check(name, actual, expected):
        report["checks"].append({"name": name, "actual": actual, "expected": expected, "pass": actual == expected})
        if actual != expected:
            raise RuntimeError(f"Model input audit failed: {name}")

    progress(f"MODEL_VALIDATION_STARTED run_id={run_id}")
    spark = None
    fs = None
    reserved = False
    train = None
    try:
        spark = SparkSession.builder.appName("instacart-fpgrowth-validation").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Expected project Spark Standalone cluster")
        report["spark_version"] = spark.version
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(root), spark.sparkContext._jsc.hadoopConfiguration())
        ready = read_json_hdfs(spark, fs, split_root + "/_READY.json")
        source = read_json_hdfs(spark, fs, split_root + "/_REPORT.json")
        spec = read_json_hdfs(spark, fs, split_root + "/_PROTOCOL.json")
        if (ready.get("status") != "READY" or ready.get("run_id") != args.split_run
                or source.get("status") != "PASS" or source.get("run_id") != args.split_run
                or not source.get("checks") or not all(c["pass"] for c in source["checks"])
                or digest_json(spec) != ready.get("protocol_sha256")
                or digest_json(spec) != source.get("protocol_sha256") or spec != protocol(spec["seed"])):
            raise RuntimeError("Split source/protocol is not valid READY/PASS")
        report.update(protocol=spec, protocol_sha256=digest_json(spec), source_preparation_run=source["source_preparation_run"])
        path = spark._jvm.org.apache.hadoop.fs.Path(root)
        if fs.exists(path) or not fs.mkdirs(path):
            raise RuntimeError("Model destination already exists or cannot be created")
        write_json_hdfs(spark, fs, root + "/_STARTED.json", {"run_id": run_id})
        reserved = True
        train = spark.read.parquet(split_root + "/tables/train_baskets").persist(StorageLevel.MEMORY_AND_DISK)
        users = spark.read.parquet(split_root + "/tables/user_splits")
        validation = spark.read.parquet(split_root + "/tables/validation_cases")
        check("train.baskets", train.count(), source["train"]["baskets"])
        check("train.non_prior_rows", train.filter(F.col("eval_set") != "prior").count(), 0)
        check("train.users", train.select("user_id").distinct().count(), source["train"]["users"])
        check("train.outside_train_users", train.join(users.filter("split='train'").select("user_id"), "user_id", "left_anti").count(), 0)
        check("validation.cases", validation.count(), source["case_counts"]["validation"]["eligible_cases"])
        check("validation.invalid_cases", validate_cases(validation), 0)
        check("validation.train_user_overlap", validation.select("user_id").join(train.select("user_id").distinct(), "user_id").count(), 0)
        cases = [row.asDict(recursive=True) for row in validation.orderBy("case_id").collect()]
        popularity, rules, records = fit_grid(spark, train, cases, spec, root, report, progress)
        check("train.vocabulary_products", len(popularity), source["train"]["vocabulary_products"])
        check("validation.grid_size", len(report["candidates"]), len(spec["grid"]["minSupport"]) * len(spec["grid"]["minConfidence"]))
        serving = {"schema_version": 1, "model_run": run_id, "source_split_run": args.split_run,
                   "source_preparation_run": source["source_preparation_run"], "protocol_sha256": report["protocol_sha256"],
                   "k": 5, "selected": report["selected"], "train_baskets": report["train_baskets"],
                   "popularity": popularity, "rules": rules}
        report["serving_sha256"] = digest_json(serving)
        write_json_hdfs(spark, fs, root + "/_SERVING.json", serving)
        write_json_hdfs(spark, fs, root + "/_SELECTION.json", {"selected": report["selected"], "validation_metrics": report["validation_metrics"],
                                                               "serving_sha256": report["serving_sha256"], "test_status": "NOT_EVALUATED"})
        (out / "serving.json").write_text(json.dumps(serving, indent=2), encoding="utf-8")
        (out / "validation_predictions.json").write_text(json.dumps(records, indent=2), encoding="utf-8")
        write_metrics_csv(out / "validation_metrics.csv", report["validation_metrics"])
        (out / "grid.json").write_text(json.dumps(sorted(report["candidates"], key=selection_key), indent=2), encoding="utf-8")
        report.update(status="PASS", duration_seconds=round(time.monotonic() - started, 3), finished_at_utc=datetime.now(timezone.utc).isoformat())
        write_json_hdfs(spark, fs, root + "/_REPORT.json", report)
        progress(f"MODEL_VALIDATION_ALL_CHECKS_PASS run_id={run_id}; publishing READY")
        write_json_hdfs(spark, fs, root + "/_READY.json", {"status": "READY", "run_id": run_id,
                        "source_split_run": args.split_run, "protocol_sha256": report["protocol_sha256"], "serving_sha256": report["serving_sha256"]})
        try:
            (out.parent / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as error:
            print(f"WARNING: latest.json not updated: {error}; use {out / 'report.json'}", flush=True)
        print(f"MODEL_VALIDATION_PASS run_id={run_id} report={out / 'report.json'}", flush=True)
    except Exception as error:
        report.update(status="FAILED", error=str(error))
        progress(f"MODEL_VALIDATION_FAILED run_id={run_id} report={out / 'report.json'}")
        if reserved:
            try:
                write_json_hdfs(spark, fs, root + "/_FAILED.json", report)
            except Exception:
                traceback.print_exc()
        raise
    finally:
        if train is not None:
            train.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-run", required=True)
    main(parser.parse_args())
