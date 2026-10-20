"""Export an evaluated model and matching Gold data; never refit or retune."""
import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from ingest_bronze import read_json_hdfs
from prepare_silver_gold import export_csv
from recommendation_core import MODELS, METRICS, digest_json, case_metrics
from dashboard_bundle import file_hash, load_bundle

GOLD = ("orders_by_dow", "orders_by_hour", "orders_by_eval_set", "basket_size_distribution",
        "product_popularity_eda", "department_activity_eda")


def audit_saved_predictions(records, expected):
    """Reconcile already-saved predictions/metrics; no new model prediction is run."""
    hits = {}
    for name in MODELS:
        group = [r for r in records if r["model"] == name]
        if len(group) != expected[name]["cases"] or len({r["case_id"] for r in group}) != len(group):
            raise ValueError("Stored prediction case count mismatch")
        sums = {key: 0.0 for key in METRICS}
        hits[name] = {}
        for row in group:
            if set(row["recommendations"]) & set(row["context"]):
                raise ValueError("Stored prediction contains context products")
            scores = case_metrics(row["recommendations"], row["target"])
            for key, value in scores.items():
                if abs(value - row[key]) > 1e-12:
                    raise ValueError("Stored per-case metrics mismatch")
                sums[key] += value
            hits[name][row["case_id"]] = scores["hit_rate_at_5"] > 0
        if any(abs(total / len(group) - expected[name][key]) > 1e-12 for key, total in sums.items()):
            raise ValueError("Stored aggregate metrics mismatch")
    p, h = hits["popularity"], hits["hybrid"]
    if p.keys() != h.keys() or p.keys() != hits["fpgrowth_rule_only"].keys():
        raise ValueError("Models have different saved test cases")
    return {"cases": len(p), "both_hit": sum(p[k] and h[k] for k in p),
            "hybrid_only_hit": sum(h[k] and not p[k] for k in p),
            "popularity_only_hit": sum(p[k] and not h[k] for k in p),
            "neither_hit": sum(not p[k] and not h[k] for k in p)}


def main(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", args.model_run):
        raise ValueError("Invalid model run")
    spark = SparkSession.builder.appName("instacart-dashboard-export").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        model_root = f"hdfs://namenode:9000/instacart/models/{args.model_run}"
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(model_root), spark.sparkContext._jsc.hadoopConfiguration())

        def read(path):
            return read_json_hdfs(spark, fs, path)

        model_ready = read(model_root + "/_READY.json")
        model = read(model_root + "/_REPORT.json")
        serving = read(model_root + "/_SERVING.json")
        if (model_ready.get("status") != "READY" or model.get("status") != "PASS"
                or not model.get("checks") or not all(c["pass"] for c in model["checks"])
                or model_ready["run_id"] != args.model_run or model["run_id"] != args.model_run
                or serving["model_run"] != args.model_run or digest_json(serving) != model_ready["serving_sha256"]
                or digest_json(serving) != model["serving_sha256"] or serving["selected"] != model["selected"]):
            raise ValueError("Invalid selected model export")
        split_run = model["source_split_run"]
        test_root = f"hdfs://namenode:9000/instacart/test-results/{split_run}"
        test_ready, test = read(test_root + "/_READY.json"), read(test_root + "/_REPORT.json")
        if (test_ready.get("status") != "READY" or test.get("status") != "PASS"
                or test_ready["source_model_run"] != args.model_run or test["source_model_run"] != args.model_run
                or test_ready["serving_sha256"] != digest_json(serving) or test["serving_sha256"] != digest_json(serving)
                or test["source_split_run"] != split_run or test["selected"] != model["selected"]
                or test["protocol_sha256"] != model["protocol_sha256"]):
            raise ValueError("Test results do not belong to this selected model")
        paired = audit_saved_predictions(read(test_root + "/_PREDICTIONS.json"), test["test_metrics"])
        preparation_run = model["source_preparation_run"]
        prep_root = f"hdfs://namenode:9000/instacart/prepared/{preparation_run}"
        prep_ready, preparation = read(prep_root + "/_READY.json"), read(prep_root + "/_REPORT.json")
        silver = f"hdfs://namenode:9000/instacart/silver/{preparation_run}"
        gold = f"hdfs://namenode:9000/instacart/gold/{preparation_run}"
        if (prep_ready.get("status") != "READY" or preparation.get("status") != "PASS"
                or not preparation.get("checks") or not all(c["pass"] for c in preparation["checks"])
                or prep_ready["run_id"] != preparation_run or prep_ready["silver_path"] != silver
                or prep_ready["gold_path"] != gold or serving["source_preparation_run"] != preparation_run):
            raise ValueError("EDA/Silver source is not the selected model's preparation run")

        parent = Path("/workspace/outputs/dashboard")
        parent.mkdir(parents=True, exist_ok=True)
        final = parent / args.model_run
        if final.exists():
            # Validate an existing immutable export before reusing it.
            pointer = {"bundle_dir": args.model_run, "model_run": args.model_run,
                       "manifest_sha256": file_hash(final / "manifest.json")}
            temporary = parent / ("pointer-" + uuid4().hex + ".json")
            try:
                temporary.write_text(json.dumps(pointer), encoding="utf-8")
                _, existing, _, _ = load_bundle(temporary)
                if existing["serving_sha256"] != digest_json(serving):
                    raise ValueError("Existing dashboard export has another model checksum")
            finally:
                temporary.unlink(missing_ok=True)
        else:
            stage = parent / ("building-" + uuid4().hex)
            (stage / "eda").mkdir(parents=True, exist_ok=False)
            catalog = spark.read.parquet(silver + "/tables/products")
            ids = {r["product_id"] for r in catalog.select("product_id").collect()}
            if any(row["product_id"] not in ids for row in serving["popularity"]):
                raise ValueError("Training vocabulary missing from product catalog")
            export_csv(catalog, stage / "catalog.csv")
            for name in GOLD:
                export_csv(spark.read.parquet(gold + "/tables/" + name), stage / "eda" / (name + ".csv"))
            bundle = {"schema_version": 1, "data_kind": "observed_project_exports", "model_run": args.model_run,
                      "source_split_run": split_run, "source_preparation_run": preparation_run,
                      "serving_sha256": digest_json(serving), "exported_at_utc": datetime.now(timezone.utc).isoformat(),
                      "model_report": model, "test_report": test, "kpis": preparation["kpis"], "sample": preparation["sample"],
                      "paired_test": paired, "protocol": model["protocol"]}
            (stage / "bundle.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")
            (stage / "serving.json").write_text(json.dumps(serving, indent=2), encoding="utf-8")
            manifest = {"schema_version": 1, "model_run": args.model_run,
                        "files": {str(p.relative_to(stage)).replace("\\", "/"): file_hash(p)
                                  for p in sorted(stage.rglob("*")) if p.is_file()}}
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            os.rename(stage, final)
            pointer = {"bundle_dir": args.model_run, "model_run": args.model_run,
                       "manifest_sha256": file_hash(final / "manifest.json")}
        temp_pointer = parent / ("latest-" + uuid4().hex + ".json")
        temp_pointer.write_text(json.dumps(pointer, indent=2), encoding="utf-8")
        load_bundle(temp_pointer)
        os.replace(temp_pointer, parent / "latest.json")
        print(f"DASHBOARD_EXPORT_PASS model_run={args.model_run} bundle={final}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-run", required=True)
    main(parser.parse_args())
