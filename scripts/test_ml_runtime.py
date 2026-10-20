"""Check the ML import on driver/executors and fit a tiny synthetic FP-Growth model."""
import json
import numpy
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession


def inspect_executor(_):
    import socket
    import numpy
    from pyspark.ml.fpm import FPGrowth
    return (socket.gethostname(), numpy.__version__, FPGrowth.__name__)


def main():
    spark = SparkSession.builder.appName("instacart-ml-runtime-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if numpy.__version__ != "1.26.4":
            raise RuntimeError(f"Unexpected driver NumPy version: {numpy.__version__}")
        workers = sorted(set(spark.sparkContext.parallelize([0, 1], 2).map(inspect_executor).collect()))
        if not workers or any(row[1] != "1.26.4" for row in workers):
            raise RuntimeError(f"Executor dependency mismatch: {workers}")
        data = spark.createDataFrame([([1, 2],), ([1, 2],), ([1],), ([3],)], "items ARRAY<INT>")
        model = FPGrowth(itemsCol="items", minSupport=.25, minConfidence=.5, numPartitions=2).fit(data)
        rules = model.associationRules.collect()
        rule = next(r for r in rules if r["antecedent"] == [1] and r["consequent"] == [2])
        if abs(rule["confidence"] - 2 / 3) > 1e-12 or model.freqItemsets.count() != 4:
            raise RuntimeError("Synthetic FP-Growth readback mismatch")
        print("ML_RUNTIME_PASS " + json.dumps({"numpy": numpy.__version__, "spark": spark.version,
              "executors": workers, "scope": "synthetic_dependency_and_fit_check"}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
