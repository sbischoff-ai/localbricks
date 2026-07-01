from __future__ import annotations

import argparse
import importlib.util
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Iterator

from pyspark.pipelines.flow import Flow
from pyspark.pipelines.graph_element_registry import (
    GraphElementRegistry,
    graph_element_registration_context,
)
from pyspark.pipelines.output import MaterializedView, Output, Sink, Table, TemporaryView
from pyspark.sql import DataFrame, SparkSession

from localbricks.spark import WAREHOUSE_DIR, create_spark


@dataclass
class LocalPipelineRegistry(GraphElementRegistry):
    outputs: dict[str, Output] = field(default_factory=dict)
    flows: list[Flow] = field(default_factory=list)
    sql_files: list[tuple[str, Path]] = field(default_factory=list)

    def register_output(self, output: Output) -> None:
        if output.name in self.outputs:
            raise ValueError(f"Duplicate pipeline output: {output.name}")
        self.outputs[output.name] = output

    def register_flow(self, flow: Flow) -> None:
        self.flows.append(flow)

    def register_sql(self, sql_text: str, file_path: Path) -> None:
        self.sql_files.append((sql_text, file_path))


@contextmanager
def _temporary_spark_conf(spark: SparkSession, conf: dict[str, str]) -> Iterator[None]:
    previous: dict[str, str | None] = {}
    for key, value in conf.items():
        try:
            previous[key] = spark.conf.get(key)
        except Exception:
            previous[key] = None
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def _load_pipeline_module(path: Path, registry: LocalPipelineRegistry, spark: SparkSession) -> ModuleType:
    module_name = f"localbricks_pipeline_{path.stem}_{abs(hash(path.resolve()))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot load pipeline file: {path}")

    module = importlib.util.module_from_spec(spec)
    module.spark = spark
    sys.modules[module_name] = module
    with graph_element_registration_context(registry):
        spec.loader.exec_module(module)
    return module


def _qualified_name(name: str, catalog: str, schema: str) -> str:
    parts = name.split(".")
    if len(parts) == 1:
        return f"{catalog}.{schema}.{name}"
    if len(parts) == 2:
        return f"{catalog}.{name}"
    return name


def _storage_path(name: str, schema: str) -> Path:
    parts = name.split(".")
    table_schema = schema if len(parts) == 1 else parts[-2]
    table_name = parts[-1]
    return Path(WAREHOUSE_DIR) / table_schema / table_name


def _create_schema(spark: SparkSession, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _register_delta_table(spark: SparkSession, target: str, path: Path, mode: str) -> None:
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {target}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING delta LOCATION '{path.as_posix()}'")


def _write_table(
    spark: SparkSession,
    df: DataFrame,
    target: str,
    path: Path,
    output: Table,
    mode: str,
) -> None:
    writer = df.write.format(output.format or "delta").mode(mode)
    if output.partition_cols:
        writer = writer.partitionBy(*output.partition_cols)
    writer.save(path.as_posix())
    _register_delta_table(spark, target, path, mode)


def _write_streaming_table(
    df: DataFrame,
    spark: SparkSession,
    target: str,
    path: Path,
    output: Table,
    mode: str,
    checkpoint_root: Path,
) -> None:
    checkpoint = checkpoint_root / target.replace(".", "_")
    writer = (
        df.writeStream.format(output.format or "delta")
        .option("checkpointLocation", checkpoint.as_posix())
        .outputMode("append")
        .trigger(availableNow=True)
    )
    if output.partition_cols:
        writer = writer.partitionBy(*output.partition_cols)
    writer.start(path.as_posix()).awaitTermination()
    _register_delta_table(spark, target, path, mode)


def run_pipeline(
    pipeline_files: list[Path],
    *,
    catalog: str,
    schema: str,
    checkpoint_root: Path,
    spark: SparkSession | None = None,
) -> LocalPipelineRegistry:
    spark = spark or create_spark("localbricks-pipeline")
    registry = LocalPipelineRegistry()

    _create_schema(spark, catalog, schema)
    for path in pipeline_files:
        _load_pipeline_module(path, registry, spark)

    for sql_text, file_path in registry.sql_files:
        raise NotImplementedError(f"SQL pipeline files are not supported yet: {file_path}")

    for flow in registry.flows:
        output = registry.outputs.get(flow.target)
        if output is None:
            raise ValueError(f"Flow {flow.name} targets unknown output {flow.target}")

        with _temporary_spark_conf(spark, flow.spark_conf):
            df = flow.func()

        if not isinstance(df, DataFrame):
            raise TypeError(f"Flow {flow.name} must return a Spark DataFrame")

        target = _qualified_name(flow.target, catalog, schema)
        if isinstance(output, TemporaryView):
            df.createOrReplaceTempView(output.name)
        elif isinstance(output, Sink):
            raise NotImplementedError(f"External sinks are not supported locally: {output.name}")
        elif isinstance(output, MaterializedView):
            _write_table(spark, df, target, _storage_path(flow.target, schema), output, "overwrite")
        elif output.__class__.__name__ == "StreamingTable":
            path = _storage_path(flow.target, schema)
            if df.isStreaming:
                _write_streaming_table(df, spark, target, path, output, "append", checkpoint_root)
            else:
                mode = "append" if flow.name != flow.target else "overwrite"
                _write_table(spark, df, target, path, output, mode)
        else:
            raise TypeError(f"Unsupported pipeline output type for {output.name}: {type(output).__name__}")

    return registry


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark Declarative Pipeline files locally.")
    parser.add_argument("pipeline_files", nargs="+", type=Path)
    parser.add_argument("--catalog", default="unity")
    parser.add_argument("--schema", default="demo")
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=Path("/workspace/warehouse/_localbricks_checkpoints"),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    files = [path.resolve() for path in args.pipeline_files]
    registry = run_pipeline(
        files,
        catalog=args.catalog,
        schema=args.schema,
        checkpoint_root=args.checkpoint_root,
    )
    print(f"Registered {len(registry.outputs)} outputs and executed {len(registry.flows)} flows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
