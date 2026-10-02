"""Khởi tạo Spark từ cấu hình project."""
from pathlib import Path
import yaml
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def create_spark_session(app_name: str | None = None, settings_path: str | Path | None = None,
                         extra_configs: dict | None = None) -> SparkSession:
    path = Path(settings_path) if settings_path else PROJECT_ROOT / "config/settings.yaml"
    with path.open(encoding="utf-8") as config_file:
        settings = yaml.safe_load(config_file)["spark"]
    builder = (
        SparkSession.builder
        .appName(app_name or settings["app_name"])
        .master(settings["master"])
        .config("spark.driver.memory", settings["driver_memory"])
        .config("spark.sql.adaptive.enabled", str(settings["adaptive_enabled"]).lower())
        .config("spark.sql.shuffle.partitions", str(settings["shuffle_partitions"]))
    )
    for key, value in (extra_configs or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings["log_level"])
    return spark
