from __future__ import annotations

import os
from typing import Iterable

from pyspark.sql import SparkSession


DELTA_PACKAGE = os.getenv("LOCALBRICKS_DELTA_PACKAGE", "io.delta:delta-spark_2.13:4.2.0")
UC_SPARK_PACKAGE = os.getenv(
    "LOCALBRICKS_UC_SPARK_PACKAGE", "io.unitycatalog:unitycatalog-spark_2.13:0.4.1"
)
DEFAULT_APP_NAME = os.getenv("LOCALBRICKS_SPARK_APP_NAME", "localbricks")
DEFAULT_CATALOG = os.getenv("LOCALBRICKS_DEFAULT_CATALOG", "unity")
WAREHOUSE_DIR = os.getenv("LOCALBRICKS_WAREHOUSE_DIR", "/workspace/warehouse")


def _truthy(value: str | None) -> bool:
    return value in {"1", "true", "TRUE", "True", "yes", "YES", "Yes", "on", "ON", "On"}


def spark_packages() -> str:
    packages = [DELTA_PACKAGE, UC_SPARK_PACKAGE]
    extra = os.getenv("LOCALBRICKS_SPARK_PACKAGES", "")
    if extra:
        packages.extend(item.strip() for item in extra.split(",") if item.strip())
    return ",".join(dict.fromkeys(packages))


def _apply_configs(builder: SparkSession.Builder, configs: Iterable[tuple[str, str]]) -> None:
    for key, value in configs:
        builder.config(key, value)


def create_spark(app_name: str | None = None) -> SparkSession:
    uc_uri = os.getenv("UC_URI", "http://uc-server:8080")
    default_catalog = os.getenv("LOCALBRICKS_DEFAULT_CATALOG", DEFAULT_CATALOG)

    builder = SparkSession.builder.appName(app_name or DEFAULT_APP_NAME).master(
        os.getenv("LOCALBRICKS_SPARK_MASTER", "local[*]")
    )

    _apply_configs(
        builder,
        (
            ("spark.jars.packages", spark_packages()),
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "io.unitycatalog.spark.UCSingleCatalog"),
            ("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog"),
            ("spark.sql.catalog.unity.uri", uc_uri),
            ("spark.sql.catalog.unity.token", os.getenv("UC_TOKEN", "")),
            ("spark.sql.defaultCatalog", default_catalog),
            ("spark.sql.warehouse.dir", os.getenv("LOCALBRICKS_WAREHOUSE_DIR", WAREHOUSE_DIR)),
            ("spark.databricks.delta.schema.autoMerge.enabled", "true"),
        ),
    )

    if _truthy(os.getenv("LOCALBRICKS_SPARK_UI_DISABLED", "true")):
        builder.config("spark.ui.enabled", "false")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("LOCALBRICKS_SPARK_LOG_LEVEL", "WARN"))
    return spark

