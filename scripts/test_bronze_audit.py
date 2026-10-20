"""Small synthetic audit tests. Not an Instacart experiment or HDFS integration test."""
import tempfile

from pyspark.sql import SparkSession
from ingest_bronze import SCHEMAS, audit_tables


def run_checks(spark, data):
    tables = {name: spark.createDataFrame(rows, SCHEMAS[name]) for name, rows in data.items()}
    report = {"checks": [], "tables": {name: {} for name in data}}
    audit_tables(tables, report, lambda _: None)
    return {check["name"]: check for check in report["checks"]}


def main():
    spark = SparkSession.builder.appName("test-bronze-audit-synthetic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        data = {
            "aisles": [(1, "Aisle")], "departments": [(1, "Department")],
            "products": [(1, 'Milk, "Fresh"', 1, 1), (2, "Bread", 1, 1)],
            "orders": [(101, 1, "prior", 1, 0, 12, None), (102, 1, "train", 2, 1, 13, 7.0),
                       (103, 2, "test", 1, 2, 14, None)],
            "order_products__prior": [(101, 1, 1, 0), (101, 2, 2, 0)],
            "order_products__train": [(102, 2, 1, 1)],
        }
        good = run_checks(spark, data)
        assert all(c["pass"] for c in good.values()), good
        # Multiple independent corruptions verify key, domain, FK and split checks.
        bad = dict(data)
        bad["order_products__train"] = [(101, 999, 2, 2), (101, 999, 2, 2)]
        bad_checks = run_checks(spark, bad)
        for suffix in ("invalid_rows", "duplicate_key_rows", "missing_product_id", "wrong_eval_set",
                       "duplicate_cart_position_rows", "noncontiguous_cart_orders", "orders_without_items"):
            assert not bad_checks[f"order_products__train.{suffix}"]["pass"], suffix

        # The CSV reader must preserve commas and doubled quotes in product names.
        # Use Spark's own filesystem rather than a driver-local temp path so this
        # test also works when submitted to the project's Standalone worker.
        base = spark.sparkContext._jsc.hadoopConfiguration().get("fs.defaultFS", "file:///")
        if base.startswith("hdfs:"):
            from uuid import uuid4
            folder = f"{base.rstrip('/')}/instacart/checks/audit-test-{uuid4().hex}"
        else:
            folder = "file://" + tempfile.mkdtemp(prefix="bronze-audit-test-")
        product = spark.createDataFrame(data["products"], SCHEMAS["products"])
        product.coalesce(1).write.option("header", True).option("escape", '"').csv(folder + "/csv")
        parsed = (spark.read.schema(SCHEMAS["products"]).options(header="true", enforceSchema="false",
                  mode="FAILFAST", escape='"').csv(folder + "/csv"))
        assert {r["product_name"] for r in parsed.collect()} == {'Milk, "Fresh"', "Bread"}
        # This uses the same HDFS JSON helpers as the publishing path.
        from ingest_bronze import read_json_hdfs, write_json_hdfs
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(folder), spark.sparkContext._jsc.hadoopConfiguration())
        write_json_hdfs(spark, fs, folder + "/roundtrip.json", {"text": "kiểm tra", "checks": len(good)})
        assert read_json_hdfs(spark, fs, folder + "/roundtrip.json") == {"text": "kiểm tra", "checks": len(good)}
        stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(folder + "/malformed.csv"), False)
        try:
            stream.write(bytearray(b'product_id,product_name,aisle_id,department_id\nnot-an-int,Milk,1,1\n'))
        finally:
            stream.close()
        spark.sparkContext.setLogLevel("OFF")
        try:
            malformed = (spark.read.schema(SCHEMAS["products"]).options(header="true", enforceSchema="false",
                         mode="FAILFAST", escape='"').csv(folder + "/malformed.csv"))
            malformed.write.parquet(folder + "/must-fail")
        except Exception as error:
            assert "MALFORMED_RECORD_IN_PARSING" in str(error) or "NumberFormatException" in str(error), str(error)
        else:
            raise AssertionError("Invalid numeric CSV data was silently accepted")
        finally:
            spark.sparkContext.setLogLevel("ERROR")
        print(f"BRONZE_AUDIT_TEST_PASS checks={len(good)} scope=synthetic_audit_rules_csv_and_json_helpers", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
