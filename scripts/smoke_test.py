"""Infrastructure check only; synthetic data is not an experiment result."""
import json
import socket
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F


def main():
    with urlopen("http://spark-master:8080/json/", timeout=15) as response:
        status = json.load(response)
    workers = [w for w in status["workers"] if w["state"] == "ALIVE"]
    if not workers:
        raise RuntimeError("No ALIVE Spark worker. Check docker compose logs spark-worker.")

    spark = SparkSession.builder.appName("instacart-infrastructure-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if spark.sparkContext.master != "spark://spark-master:7077":
            raise RuntimeError("Smoke test must run on Spark Standalone.")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        destination = f"hdfs://namenode:9000/instacart/checks/{run_id}"
        # Python tasks exercise the worker's Python runtime and driver connectivity.
        executor_hosts = sorted(set(spark.sparkContext.parallelize(range(4), 2)
                                    .map(lambda _: socket.gethostname()).collect()))
        if not executor_hosts or any(h != "spark-worker" for h in executor_hosts):
            raise RuntimeError(f"Unexpected execution hosts: {executor_hosts}")
        df = spark.range(1000, numPartitions=2).withColumn("bucket", F.col("id") % 10)
        df.write.mode("errorifexists").parquet(destination)
        loaded = spark.read.parquet(destination)
        result = loaded.agg(F.count("*").alias("rows"), F.sum("id").alias("sum_id")).first()
        buckets = {row["bucket"]: row["count"] for row in loaded.groupBy("bucket").count().collect()}
        if result["rows"] != 1000 or result["sum_id"] != 499500 or buckets != {i: 100 for i in range(10)}:
            raise RuntimeError(f"Readback mismatch: {result}; buckets={buckets}")
        report = {
            "status": "PASS", "scope": "infrastructure_only_synthetic_data",
            "run_id": run_id, "spark_version": spark.version,
            "master": spark.sparkContext.master, "alive_workers": len(workers),
            "executor_hosts": executor_hosts, "hdfs_path": destination,
            "rows": result["rows"], "sum_id": result["sum_id"],
            "bucket_counts": buckets,
        }
        out = Path("/workspace/outputs/checks")
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{run_id}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        (out / "latest.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("SMOKE_TEST_PASS " + json.dumps(report, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
