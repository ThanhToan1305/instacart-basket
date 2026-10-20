"""Evaluate frozen serving artifacts on test; one binding per split run."""
import argparse
import json
import re
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from ingest_bronze import read_json_hdfs, write_json_hdfs
from recommendation_core import Recommender, digest_json, evaluate
from train_validate import write_metrics_csv


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", args.model_run):
        raise ValueError("Invalid model run identifier")
    spark = SparkSession.builder.appName("instacart-frozen-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    report = None
    fs = None
    root = None
    out = None
    reserved = False
    started = time.monotonic()
    try:
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Expected project Spark Standalone cluster")
        model_root = f"hdfs://namenode:9000/instacart/models/{args.model_run}"
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(model_root), spark.sparkContext._jsc.hadoopConfiguration())
        ready = read_json_hdfs(spark, fs, model_root + "/_READY.json")
        model = read_json_hdfs(spark, fs, model_root + "/_REPORT.json")
        serving = read_json_hdfs(spark, fs, model_root + "/_SERVING.json")
        selection = read_json_hdfs(spark, fs, model_root + "/_SELECTION.json")
        serving_hash = digest_json(serving)
        if (ready.get("status") != "READY" or ready.get("run_id") != args.model_run
                or model.get("status") != "PASS" or model.get("run_id") != args.model_run
                or not model.get("checks") or not all(c["pass"] for c in model["checks"])
                or serving.get("model_run") != args.model_run or serving.get("source_split_run") != model.get("source_split_run")
                or ready.get("source_split_run") != model.get("source_split_run")
                or serving.get("source_preparation_run") != model.get("source_preparation_run")
                or serving_hash != ready.get("serving_sha256") or serving_hash != model.get("serving_sha256")
                or serving_hash != selection.get("serving_sha256") or serving["selected"] != selection["selected"]
                or serving["selected"] != model.get("selected") or selection["validation_metrics"] != model["validation_metrics"]):
            raise RuntimeError("Selected model/serving artifacts are not a consistent READY bundle")
        split_run = model["source_split_run"]
        split_root = f"hdfs://namenode:9000/instacart/evaluation/{split_run}"
        split_ready = read_json_hdfs(spark, fs, split_root + "/_READY.json")
        split_report = read_json_hdfs(spark, fs, split_root + "/_REPORT.json")
        spec = read_json_hdfs(spark, fs, split_root + "/_PROTOCOL.json")
        if (split_ready.get("status") != "READY" or split_ready.get("run_id") != split_run
                or split_report.get("status") != "PASS" or digest_json(spec) != model["protocol_sha256"]
                or model["protocol_sha256"] != serving.get("protocol_sha256")
                or model["protocol_sha256"] != split_ready.get("protocol_sha256")):
            raise RuntimeError("Test split/protocol does not match the selected model")
        root = f"hdfs://namenode:9000/instacart/test-results/{split_run}"
        out = Path("/workspace/outputs/test") / split_run
        out.mkdir(parents=True, exist_ok=True)
        exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(root))
        if exists:
            # Re-running a completed test with the same model reuses its report.
            # An incomplete attempt is retained for diagnosis and is never silently reset.
            marker = root + "/_READY.json"
            if not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(marker)):
                raise RuntimeError("An incomplete test attempt already exists for this split. Preserve it and send logs before retrying.")
            previous_ready = read_json_hdfs(spark, fs, marker)
            if (previous_ready.get("status") != "READY" or previous_ready.get("source_split_run") != split_run
                    or previous_ready.get("source_model_run") != args.model_run or previous_ready.get("serving_sha256") != serving_hash):
                raise RuntimeError("This split is bound to another or inconsistent completed test result")
            previous = read_json_hdfs(spark, fs, root + "/_REPORT.json")
            if previous.get("source_model_run") != args.model_run or previous.get("serving_sha256") != serving_hash:
                raise RuntimeError("This split already has test results bound to another model. Do not select a new model using test scores.")
            if previous.get("status") != "PASS":
                raise RuntimeError("Existing test report is not PASS")
            (out / "report.json").write_text(json.dumps(previous, indent=2), encoding="utf-8")
            (out.parent / "latest.json").write_text(json.dumps(previous, indent=2), encoding="utf-8")
            write_metrics_csv(out / "test_metrics.csv", previous["test_metrics"])
            print(f"TEST_EVALUATION_PASS reused=true split_run={split_run} model_run={args.model_run}", flush=True)
            return
        if not fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(root)):
            raise RuntimeError("Cannot create test result directory")
        report = {"status": "RUNNING", "source_split_run": split_run, "source_model_run": args.model_run,
                  "source_preparation_run": model["source_preparation_run"], "serving_sha256": serving_hash,
                  "protocol_sha256": model["protocol_sha256"], "selected": model["selected"],
                  "validation_metrics": model["validation_metrics"], "checks": [],
                  "started_at_utc": datetime.now(timezone.utc).isoformat()}
        write_json_hdfs(spark, fs, root + "/_STARTED.json", report)
        reserved = True
        # First test-case read occurs only after frozen model validation and binding.
        print(f"TEST_EVALUATION_STARTED split_run={split_run} model_run={args.model_run}", flush=True)
        cases = [row.asDict(recursive=True) for row in spark.read.parquet(split_root + "/tables/test_cases").orderBy("case_id").collect()]
        expected = split_report["case_counts"]["test"]["eligible_cases"]
        if len(cases) != expected:
            raise RuntimeError("Test case count does not match the audited split")
        report["checks"].append({"name": "test.case_count", "actual": len(cases), "expected": expected, "pass": True})
        scores, records = evaluate(cases, Recommender(serving["rules"], serving["popularity"]))
        report["test_metrics"] = scores
        report["test_case_profile"] = split_report["case_counts"]["test"]
        report.update(status="PASS", duration_seconds=round(time.monotonic() - started, 3),
                      finished_at_utc=datetime.now(timezone.utc).isoformat())
        (out / "predictions.json").write_text(json.dumps(records, indent=2), encoding="utf-8")
        (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        write_metrics_csv(out / "test_metrics.csv", scores)
        # Preserve per-case evidence both on HDFS and on Windows.
        write_json_hdfs(spark, fs, root + "/_PREDICTIONS.json", records)
        write_json_hdfs(spark, fs, root + "/_REPORT.json", report)
        write_json_hdfs(spark, fs, root + "/_READY.json", {"status": "READY", "source_split_run": split_run,
                        "source_model_run": args.model_run, "serving_sha256": serving_hash})
        try:
            (out.parent / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        except OSError as error:
            print(f"WARNING: latest.json not updated: {error}; use {out / 'report.json'}", flush=True)
        print(f"TEST_EVALUATION_PASS split_run={split_run} model_run={args.model_run} report={out / 'report.json'}", flush=True)
    except Exception as error:
        if reserved:
            report.update(status="FAILED", error=str(error))
            try:
                (out / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
                write_json_hdfs(spark, fs, root + "/_FAILED.json", report)
            except Exception:
                traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-run", required=True)
    main(parser.parse_args())
