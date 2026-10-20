"""Freeze a user-disjoint basket-completion dataset and protocol before model fitting."""
import argparse
import hashlib
import json
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, Window, functions as F
from ingest_bronze import read_json_hdfs, write_json_hdfs


def protocol(seed):
    return {
        "protocol_version": "basket-completion-user-disjoint-v1",
        "task": "complete_an_observed_basket_not_predict_a_future_order",
        "seed": seed, "k": 5,
        "user_split": {"method": "pmod(xxhash64('user-split-v1',seed_long,user_id_long),10000)",
                       "train": [0, 7000], "validation": [7000, 8500], "test": [8500, 10000]},
        "fit_data": "Only prior baskets from train users; never original train-labeled orders or held-out users.",
        "evaluation_basket": "Latest observed labeled basket by order_number DESC then order_id DESC per held-out user.",
        "eligibility": "Latest labeled basket must contain >=2 unique products. No fallback to an older basket.",
        "mask": {"method": "sort by xxhash64('basket-mask-v1',seed_long,user_id_long,order_id_long,product_id_long), tie product_id",
                 "context_size": "floor(basket_size/2)", "target": "all remaining products",
                 "retain_unseen_targets": True},
        "models": ["popularity", "fpgrowth_rule_only", "hybrid"],
        "grid": {"minSupport": [0.001, 0.002, 0.005], "minConfidence": [0.2, 0.4, 0.6]},
        "popularity_rank": ["training_basket_frequency DESC", "product_id ASC"],
        "rule_match": "antecedent subset of context; consequent not in context",
        "rule_rank": ["confidence DESC", "lift DESC", "support DESC", "product_id ASC"],
        "candidate_dedup": "keep highest-ranked applicable rule for each product",
        "hybrid": "rule-only prefix then popularity fills to k, exclude context and duplicate products",
        "selection": {"data": "validation only", "primary": "hybrid HitRate@5 DESC",
                      "tie_breaks": ["hybrid Recall@5 DESC", "rule_count ASC", "minSupport DESC", "minConfidence DESC"],
                      "shared_fp_config": "rule-only and hybrid report the same selected FP rules"},
        "metrics": {
            "HitRate@5": "mean(1 if any top-5 recommendation is in target else 0)",
            "Precision@5": "mean(number_of_hits / 5), even for lists shorter than 5",
            "Recall@5": "mean(number_of_hits / target_size)",
            "NDCG@5": "mean(sum(hit_at_rank/log2(rank+1)) / sum(1/log2(rank+1),rank=1..min(5,target_size)))",
            "Coverage@5": "fraction of all eligible cases with at least one recommendation",
            "FullListRate@5": "fraction of all eligible cases with five recommendations",
        },
        "metric_denominator": "all eligible cases, including cases with zero recommendations or unseen target products",
        "test_policy": "Evaluate once after config selection; do not use test scores to choose model parameters.",
        "limits": "One user sample, one split seed, one deterministic mask; no claim of future-order prediction or multi-host speedup.",
    }


def assign_users(users, seed):
    return (users.select("user_id").distinct()
        .withColumn("split_bucket", F.pmod(F.xxhash64(F.lit("user-split-v1"), F.lit(seed).cast("long"),
                                                    F.col("user_id").cast("long")), F.lit(10000)))
        .withColumn("split", F.when(F.col("split_bucket") < 7000, "train")
                    .when(F.col("split_bucket") < 8500, "validation").otherwise("test")))


def latest_heldout_baskets(baskets, splits):
    window = Window.partitionBy("user_id").orderBy(F.desc("order_number"), F.desc("order_id"))
    latest = baskets.withColumn("_row", F.row_number().over(window)).filter(F.col("_row") == 1).drop("_row")
    heldout = splits.filter(F.col("split") != "train").select("user_id", "split")
    return (heldout.join(latest, "user_id", "left")
        .withColumn("eligible", F.coalesce(F.col("basket_size") >= 2, F.lit(False)))
        .withColumn("exclusion_reason", F.when(F.col("order_id").isNull(), "no_observed_labeled_basket")
                    .when(F.col("basket_size") < 2, "single_item_latest_basket").otherwise(F.lit(None).cast("string"))))


def mask_cases(latest, seed):
    # Explode + window avoids capturing outer join attributes inside higher-order
    # lambdas, which can yield invalid bound references after Spark 3.5 projection pushdown.
    exploded = (latest.filter("eligible").withColumn("_product", F.explode("items"))
        .withColumn("_mask_hash", F.xxhash64(F.lit("basket-mask-v1"), F.lit(seed).cast("long"),
                     F.col("user_id").cast("long"), F.col("order_id").cast("long"), F.col("_product").cast("long"))))
    window = Window.partitionBy("user_id", "order_id").orderBy("_mask_hash", "_product")
    ranked = exploded.withColumn("_rank", F.row_number().over(window))
    context_size = F.floor(F.col("basket_size") / 2)
    cases = (ranked.groupBy("user_id", "split", "order_id", "order_number", "eval_set", "items", "basket_size")
        .agg(F.sort_array(F.collect_list(F.when(F.col("_rank") <= context_size, F.col("_product")))).alias("context"),
             F.sort_array(F.collect_list(F.when(F.col("_rank") > context_size, F.col("_product")))).alias("target"))
        .withColumn("case_id", F.concat_ws(":", F.lit("basket"), F.col("user_id"), F.col("order_id"))))
    return cases.select("case_id", "user_id", "split", "order_id", "order_number",
                        F.col("eval_set").alias("source_eval_set"), F.col("items").alias("basket_items"),
                        "basket_size", "context", "target")


def validate_cases(cases):
    invalid = ((F.size("context") < 1) | (F.size("target") < 1)
        | (F.size(F.array_intersect("context", "target")) != 0)
        | (F.sort_array(F.array_union("context", "target")) != F.sort_array("basket_items"))
        | (F.size("context") != F.floor(F.col("basket_size") / 2))
        | (F.size("context") + F.size("target") != F.col("basket_size"))
        | F.expr("exists(context, x -> x is null) or exists(target, x -> x is null)"))
    return cases.filter(invalid).count()


def build_split_dataset(spark, users, baskets, source, root, report, progress, seed):
    def check(name, actual, expected=0):
        passed = actual == expected
        report["checks"].append({"name": name, "actual": int(actual), "expected": int(expected), "pass": passed})
        progress(f"CHECK {'PASS' if passed else 'FAIL'} {name}: {actual} (expected {expected})")
        if not passed:
            raise RuntimeError(f"Split audit failed: {name}")

    def save(name, df):
        progress(f"SPLIT_TABLE {name}")
        path = f"{root}/tables/{name}"
        df.write.mode("errorifexists").option("compression", "snappy").parquet(path)
        report["tables"][name] = {"path": path}
        return spark.read.parquet(path)

    check("source.sample_users", users.count(), source["sample"]["users"])
    check("source.sample_baskets", baskets.count(), source["sample"]["baskets"])
    check("source.duplicate_users", users.count() - users.select("user_id").distinct().count())
    check("source.baskets_outside_sample", baskets.join(users.select("user_id"), "user_id", "left_anti").count())
    splits = save("user_splits", assign_users(users, seed))
    counts = {r["split"]: r["count"] for r in splits.groupBy("split").count().collect()}
    report["user_counts"] = counts
    check("split.assigned_user_total", sum(counts.values()), source["sample"]["users"])
    ids = {name: splits.filter(F.col("split") == name).select("user_id") for name in ("train", "validation", "test")}
    for name in ids:
        check(f"split.{name}_nonempty", int(counts.get(name, 0) > 0), 1)
    for a, b in (("train", "validation"), ("train", "test"), ("validation", "test")):
        check(f"split.{a}_{b}_user_overlap", ids[a].join(ids[b], "user_id").count())
    expected_train = baskets.filter(F.col("eval_set") == "prior").join(F.broadcast(ids["train"]), "user_id", "left_semi")
    train = save("train_baskets", expected_train)
    n_train = train.count()
    check("train.nonempty", int(n_train > 0), 1)
    check("train.unexpected_eval_set", train.filter(F.col("eval_set") != "prior").count())
    check("train.users_outside_train", train.join(ids["train"], "user_id", "left_anti").count())
    check("train.missing_baskets", expected_train.exceptAll(train).count())
    check("train.extra_baskets", train.exceptAll(expected_train).count())
    check("train.users_without_prior_baskets", ids["train"].join(train.select("user_id").distinct(), "user_id", "left_anti").count())
    vocab = save("train_vocabulary", train.select(F.explode("items").alias("product_id")).distinct())
    report["train"] = {"users": train.select("user_id").distinct().count(), "baskets": n_train,
                       "vocabulary_products": vocab.count(), "source_eval_set": "prior"}

    latest = latest_heldout_baskets(baskets, splits)
    eligibility = save("case_eligibility", latest.select("user_id", "split", "order_id", "order_number", "eval_set",
                                                        "basket_size", "eligible", "exclusion_reason"))
    check("cases.heldout_user_accounting", eligibility.count(), counts["validation"] + counts["test"])
    report["case_counts"] = {}
    cases = mask_cases(latest, seed)
    result = {"user_splits": splits, "train_baskets": train, "train_vocabulary": vocab, "case_eligibility": eligibility}
    for name in ("validation", "test"):
        frame = save(f"{name}_cases", cases.filter(F.col("split") == name))
        result[f"{name}_cases"] = frame
        count = frame.count()
        check(f"{name}.nonempty_cases", int(count > 0), 1)
        check(f"{name}.invalid_context_target", validate_cases(frame))
        check(f"{name}.duplicate_case_ids", count - frame.select("case_id").distinct().count())
        check(f"{name}.duplicate_case_users", count - frame.select("user_id").distinct().count())
        check(f"{name}.users_outside_split", frame.join(ids[name], "user_id", "left_anti").count())
        check(f"{name}.case_count", count, eligibility.filter((F.col("split") == name) & F.col("eligible")).count())
        check(f"{name}.train_order_overlap", frame.select("order_id").join(train.select("order_id"), "order_id").count())
        excluded = eligibility.filter((F.col("split") == name) & ~F.col("eligible"))
        n_excluded = excluded.count()
        check(f"{name}.assigned_users_accounted", count + n_excluded, counts[name])
        targets = frame.select("case_id", F.explode("target").alias("product_id"))
        unseen = targets.join(F.broadcast(vocab), "product_id", "left_anti")
        stats = frame.agg(F.avg(F.size("context")).alias("context_avg"), F.avg(F.size("target")).alias("target_avg")).first()
        report["case_counts"][name] = {
            "assigned_users": counts[name], "eligible_cases": count, "excluded_users": n_excluded,
            "exclusions_by_reason": {r["exclusion_reason"]: r["count"] for r in excluded.groupBy("exclusion_reason").count().collect()},
            "source_eval_sets": {r["source_eval_set"]: r["count"] for r in frame.groupBy("source_eval_set").count().collect()},
            "avg_context_size": stats["context_avg"], "avg_target_size": stats["target_avg"],
            "target_item_occurrences": targets.count(), "unseen_target_item_occurrences": unseen.count(),
            "cases_with_unseen_targets": unseen.select("case_id").distinct().count(),
            "unseen_targets_retained": True,
        }
    check("cases.validation_test_order_overlap", result["validation_cases"].select("order_id")
          .join(result["test_cases"].select("order_id"), "order_id").count())
    for name, frame in result.items():
        report["tables"][name]["rows"] = frame.count()
    return result


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", args.preparation_run) or not -(2**31) <= args.seed < 2**31:
        raise ValueError("Invalid preparation run or seed")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    parent = f"hdfs://namenode:9000/instacart/prepared/{args.preparation_run}"
    silver = f"hdfs://namenode:9000/instacart/silver/{args.preparation_run}"
    root = f"hdfs://namenode:9000/instacart/evaluation/{run_id}"
    out = Path("/workspace/outputs/splits") / run_id
    out.mkdir(parents=True, exist_ok=False)
    spec = protocol(args.seed)
    spec_digest = hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    (out / "protocol.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
    report = {"status": "RUNNING", "run_id": run_id, "source_preparation_run": args.preparation_run,
              "hdfs_path": root, "protocol": spec, "protocol_sha256": spec_digest,
              "checks": [], "tables": {}, "started_at_utc": datetime.now(timezone.utc).isoformat()}
    started = time.monotonic()

    def progress(message):
        (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(message, flush=True)

    progress(f"EVALUATION_SPLIT_STARTED run_id={run_id}")
    spark = None
    fs = None
    reserved = False
    try:
        spark = SparkSession.builder.appName("instacart-evaluation-split").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Expected project Spark Standalone cluster")
        report["spark_version"] = spark.version
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(root), spark.sparkContext._jsc.hadoopConfiguration())
        ready = read_json_hdfs(spark, fs, parent + "/_READY.json")
        source = read_json_hdfs(spark, fs, parent + "/_REPORT.json")
        if (ready.get("status") != "READY" or ready.get("run_id") != args.preparation_run
                or ready.get("silver_path") != silver or source.get("status") != "PASS"
                or source.get("run_id") != args.preparation_run or not source.get("checks")
                or not all(c["pass"] for c in source["checks"])):
            raise RuntimeError("Source preparation bundle is not READY/PASS")
        path = spark._jvm.org.apache.hadoop.fs.Path(root)
        if fs.exists(path) or not fs.mkdirs(path):
            raise RuntimeError("Evaluation destination already exists or cannot be created")
        write_json_hdfs(spark, fs, root + "/_STARTED.json", {"run_id": run_id})
        reserved = True
        write_json_hdfs(spark, fs, root + "/_PROTOCOL.json", spec)
        users = spark.read.parquet(silver + "/tables/sample_users")
        baskets = spark.read.parquet(silver + "/tables/sample_baskets")
        build_split_dataset(spark, users, baskets, source, root, report, progress, args.seed)
        report.update(status="PASS", finished_at_utc=datetime.now(timezone.utc).isoformat(),
                      duration_seconds=round(time.monotonic() - started, 3))
        write_json_hdfs(spark, fs, root + "/_REPORT.json", report)
        progress(f"EVALUATION_SPLIT_ALL_CHECKS_PASS run_id={run_id}; publishing READY")
        write_json_hdfs(spark, fs, root + "/_READY.json", {"status": "READY", "run_id": run_id,
                                                        "source_preparation_run": args.preparation_run,
                                                        "protocol_sha256": spec_digest})
        try:
            (out.parent / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as error:
            print(f"WARNING: latest.json not updated: {error}; use {out / 'report.json'}", flush=True)
        print(f"EVALUATION_SPLIT_PASS run_id={run_id} report={out / 'report.json'}", flush=True)
    except Exception as error:
        report.update(status="FAILED", error=str(error), duration_seconds=round(time.monotonic() - started, 3))
        progress(f"EVALUATION_SPLIT_FAILED run_id={run_id} report={out / 'report.json'}")
        if reserved:
            try:
                write_json_hdfs(spark, fs, root + "/_FAILED.json", report)
            except Exception:
                traceback.print_exc()
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation-run", required=True)
    parser.add_argument("--seed", type=int, default=42)
    main(parser.parse_args())
