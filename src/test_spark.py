"""Kiểm thử Spark bằng dữ liệu tổng hợp."""
from common.logger import get_logger
from common.spark_session import create_spark_session


def main() -> None:
    logger = get_logger("test_spark")
    spark = create_spark_session("InstacartPrototypeTest")
    try:
        df = spark.range(1, 1_000_001)
        row_count = df.count()
        partitions = df.rdd.getNumPartitions()
        print("Spark version:", spark.version)
        print("Số dòng:", row_count)
        print("Số partition:", partitions)
        assert row_count == 1_000_000, f"Sai số dòng: {row_count}"
        assert partitions > 0
        assert spark.sparkContext.master == "local[*]"
        assert spark.sparkContext.getConf().get("spark.driver.memory") == "2g"
        assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
        assert spark.conf.get("spark.sql.shuffle.partitions") == "32"
        logger.info("Kiểm thử thành công: Spark %s, %s dòng, %s partition", spark.version, row_count, partitions)
    finally:
        spark.stop()
        logger.info("Đã dừng SparkSession")


if __name__ == "__main__":
    main()
