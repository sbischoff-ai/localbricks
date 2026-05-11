# Spark And Delta Training Reference

Use this handout next to `01_localbricks_databricks_basics.ipynb`.

Assumption: you already ran the notebook cells that load environment variables and start `spark`.

Main local paths:

- PDFs: `/workspace/data/raw/pdfs`
- Optional JSON files: `/workspace/data/raw/json`
- Delta table files: `/workspace/warehouse`
- Training schema: `demo`
- Existing tables: `demo.training_events`, `demo.pdf_chunks`

## Quick Start Checks

```python
spark.version
```

```python
spark.sql("SHOW SCHEMAS").show(truncate=False)
spark.sql("SHOW TABLES IN demo").show(truncate=False)
```

```python
events_df = spark.table("demo.training_events")
events_df.show(truncate=False)
events_df.printSchema()
```

```python
chunks_df = spark.table("demo.pdf_chunks")
chunks_df.select("source", "chunk_id").show(10, truncate=False)
```

## DataFrame Basics

Create a DataFrame from Python dictionaries:

```python
rows = [
    {"id": 1, "topic": "spark", "status": "started"},
    {"id": 2, "topic": "delta", "status": "started"},
    {"id": 3, "topic": "json", "status": "planned"},
]

df = spark.createDataFrame(rows)
df.show(truncate=False)
```

Inspect shape and schema:

```python
df.count()
df.columns
df.printSchema()
```

Notebook-friendly preview for small results:

```python
display(df.limit(10).toPandas())
```

Pick columns:

```python
df.select("id", "topic").show(truncate=False)
```

Filter rows:

```python
df.filter(df.status == "started").show(truncate=False)
df.where("topic = 'spark'").show(truncate=False)
```

Sort rows:

```python
df.orderBy("topic").show(truncate=False)
df.orderBy(df.id.desc()).show(truncate=False)
```

Add or transform columns:

```python
from pyspark.sql import functions as F

df_with_columns = (
    df
    .withColumn("topic_upper", F.upper("topic"))
    .withColumn("loaded_at", F.current_timestamp())
)

df_with_columns.show(truncate=False)
```

Group and aggregate:

```python
(
    df
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
    .orderBy("status")
    .show(truncate=False)
)
```

Join two DataFrames:

```python
owners = spark.createDataFrame([
    {"topic": "spark", "owner": "data engineering"},
    {"topic": "delta", "owner": "platform"},
    {"topic": "json", "owner": "analytics"},
])

(
    df
    .join(owners, on="topic", how="left")
    .select("id", "topic", "owner", "status")
    .show(truncate=False)
)
```

Use SQL against a temporary view:

```python
df.createOrReplaceTempView("training_events_temp")

spark.sql("""
SELECT status, count(*) AS row_count
FROM training_events_temp
GROUP BY status
ORDER BY status
""").show(truncate=False)
```

## Delta Table Workflows

Create a schema:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS demo")
```

Write a DataFrame as a managed-style table in the local warehouse:

```python
from pathlib import Path

table_path = Path("/workspace/warehouse/demo/example_events")

(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .save(table_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.example_events")
spark.sql(f"""
CREATE TABLE demo.example_events
USING delta
LOCATION '{table_path.as_posix()}'
""")
```

Read a Delta table by table name:

```python
spark.table("demo.example_events").show(truncate=False)
```

Read a Delta table by path:

```python
spark.read.format("delta").load("/workspace/warehouse/demo/example_events").show(truncate=False)
```

Append rows:

```python
new_rows = spark.createDataFrame([
    {"id": 4, "topic": "pdf", "status": "started"},
    {"id": 5, "topic": "quality", "status": "planned"},
])

(
    new_rows
    .write
    .format("delta")
    .mode("append")
    .save("/workspace/warehouse/demo/example_events")
)

spark.table("demo.example_events").orderBy("id").show(truncate=False)
```

Overwrite intentionally:

```python
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .save("/workspace/warehouse/demo/example_events")
)
```

Inspect Delta history:

```python
spark.sql("DESCRIBE HISTORY demo.example_events").show(truncate=False)
```

Run SQL queries:

```python
spark.sql("""
SELECT topic, status
FROM demo.example_events
WHERE status = 'started'
ORDER BY topic
""").show(truncate=False)
```

## PDF Chunk Table Exercises

The example notebook reads PDFs from `/workspace/data/raw/pdfs`, chunks the text, and writes `demo.pdf_chunks`.

Preview chunks:

```python
spark.sql("""
SELECT source, chunk_id, substring(text, 1, 120) AS preview
FROM demo.pdf_chunks
ORDER BY source, chunk_id
""").show(truncate=False)
```

Count chunks per PDF:

```python
spark.sql("""
SELECT source, count(*) AS chunk_count
FROM demo.pdf_chunks
GROUP BY source
ORDER BY source
""").show(truncate=False)
```

Find longer chunks:

```python
from pyspark.sql import functions as F

(
    spark.table("demo.pdf_chunks")
    .withColumn("text_length", F.length("text"))
    .select("source", "chunk_id", "text_length")
    .orderBy(F.desc("text_length"))
    .show(10, truncate=False)
)
```

Search chunk text:

```python
(
    spark.table("demo.pdf_chunks")
    .filter(F.lower("text").contains("spark"))
    .select("source", "chunk_id", F.substring("text", 1, 160).alias("preview"))
    .show(truncate=False)
)
```

## JSON ETL Workflow

Generate sample JSON data for the session:

```python
from pathlib import Path
import json

json_dir = Path("/workspace/data/raw/json")
json_dir.mkdir(parents=True, exist_ok=True)

events = [
    {
        "event_id": 1001,
        "user": {"id": "u-001", "department": "finance"},
        "action": "uploaded_pdf",
        "tags": ["pdf", "raw"],
        "duration_seconds": 12,
    },
    {
        "event_id": 1002,
        "user": {"id": "u-002", "department": "analytics"},
        "action": "created_table",
        "tags": ["delta", "table"],
        "duration_seconds": 30,
    },
    {
        "event_id": 1003,
        "user": {"id": "u-001", "department": "finance"},
        "action": "queried_table",
        "tags": ["spark", "sql"],
        "duration_seconds": 8,
    },
]

with (json_dir / "events.json").open("w", encoding="utf-8") as file:
    for event in events:
        file.write(json.dumps(event) + "\n")
```

Read JSON lines with Spark:

```python
from pyspark.sql import functions as F

raw_json_df = spark.read.json("/workspace/data/raw/json/events.json")

raw_json_df.show(truncate=False)
raw_json_df.printSchema()
```

Flatten nested fields:

```python
flattened_events_df = raw_json_df.select(
    "event_id",
    F.col("user.id").alias("user_id"),
    F.col("user.department").alias("department"),
    "action",
    "duration_seconds",
    "tags",
)

flattened_events_df.show(truncate=False)
```

Explode arrays into one row per tag:

```python
event_tags_df = (
    flattened_events_df
    .select("event_id", "action", F.explode("tags").alias("tag"))
    .orderBy("event_id", "tag")
)

event_tags_df.show(truncate=False)
```

Aggregate JSON events:

```python
(
    flattened_events_df
    .groupBy("department")
    .agg(
        F.count("*").alias("event_count"),
        F.sum("duration_seconds").alias("total_duration_seconds"),
    )
    .orderBy("department")
    .show(truncate=False)
)
```

Write parsed JSON events to Delta:

```python
json_table_path = Path("/workspace/warehouse/demo/training_json_events")

(
    flattened_events_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(json_table_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.training_json_events")
spark.sql(f"""
CREATE TABLE demo.training_json_events
USING delta
LOCATION '{json_table_path.as_posix()}'
""")

spark.table("demo.training_json_events").show(truncate=False)
```

Parse a JSON string column with an explicit schema:

```python
from pyspark.sql import types as T

messages_df = spark.createDataFrame([
    {"message_id": 1, "payload": '{"source":"pdf","pages":4,"ok":true}'},
    {"message_id": 2, "payload": '{"source":"json","pages":0,"ok":true}'},
])

payload_schema = T.StructType([
    T.StructField("source", T.StringType(), True),
    T.StructField("pages", T.IntegerType(), True),
    T.StructField("ok", T.BooleanType(), True),
])

parsed_messages_df = (
    messages_df
    .withColumn("parsed", F.from_json("payload", payload_schema))
    .select(
        "message_id",
        F.col("parsed.source").alias("source"),
        F.col("parsed.pages").alias("pages"),
        F.col("parsed.ok").alias("ok"),
    )
)

parsed_messages_df.show(truncate=False)
```

## Common Patterns

Use a clear ETL shape:

```python
raw_df = spark.read.json("/workspace/data/raw/json/events.json")

clean_df = raw_df.select(
    F.col("event_id").cast("int"),
    F.col("user.id").alias("user_id"),
    F.col("user.department").alias("department"),
    F.col("action"),
    F.col("duration_seconds").cast("int"),
)

result_df = (
    clean_df
    .filter(F.col("duration_seconds") > 0)
    .withColumn("loaded_at", F.current_timestamp())
)

result_df.write.format("delta").mode("overwrite").save("/workspace/warehouse/demo/clean_events")
```

Check row counts before and after:

```python
print(f"raw rows: {raw_df.count()}")
print(f"clean rows: {clean_df.count()}")
print(f"result rows: {result_df.count()}")
```

Check for missing values:

```python
(
    result_df
    .select([
        F.count(F.when(F.col(column).isNull(), column)).alias(column)
        for column in result_df.columns
    ])
    .show(truncate=False)
)
```

Drop duplicate rows:

```python
deduplicated_df = result_df.dropDuplicates(["event_id"])
```

Rename columns:

```python
renamed_df = result_df.withColumnRenamed("duration_seconds", "duration_s")
```

Convert Spark results to a small local Python list:

```python
small_rows = result_df.limit(5).collect()
small_rows
```

Use `collect()` only for small results. Spark tables can be much larger than local memory.

## Host Guide: Introductory Training Cases

### Case 1: What Is A DataFrame?

Goal: make DataFrames feel like tables that can be transformed with Python.

1. Run `events_df = spark.table("demo.training_events")`.
2. Show `events_df.show()`, `events_df.printSchema()`, and `events_df.count()`.
3. Ask learners to select only `topic` and `status`.
4. Ask learners to filter to rows where `status == "started"`.
5. Show the same query in SQL and compare readability.

Expected discussion:

- A DataFrame is lazy: transformations describe work; actions like `show()` and `count()` run it.
- Schema matters because Spark needs to know column names and types.
- DataFrame code and SQL can solve the same problem.

### Case 2: Turn Raw Rows Into A Useful Table

Goal: practice the ETL shape: read, transform, validate, write.

1. Create a small DataFrame from dictionaries.
2. Add `topic_upper` and `loaded_at`.
3. Group by `status`.
4. Write the result to `/workspace/warehouse/demo/example_events`.
5. Register it as `demo.example_events`.
6. Query the table with `spark.table()` and `spark.sql()`.

Expected discussion:

- ETL scripts are usually a sequence of small, named DataFrames.
- Good intermediate names make beginner code easier to debug.
- Delta table storage and table registration are related but separate ideas.

### Case 3: PDFs Become Rows

Goal: connect the session PDF workflow to normal table operations.

1. Copy PDFs into `data/raw/pdfs` on the host.
2. Run the notebook cells that parse PDFs and create `demo.pdf_chunks`.
3. Count chunks by source.
4. Search chunks for a word from one of the PDFs.
5. Find the longest chunks.

Expected discussion:

- Unstructured files become useful when converted into rows and columns.
- `source`, `chunk_id`, and `text` are enough to trace each row back to its file.
- Chunking is an ETL step, even without AI.

### Case 4: JSON ETL

Goal: parse semi-structured data into a clean Delta table.

1. Generate `events.json` with the sample JSON code.
2. Read it with `spark.read.json`.
3. Inspect the inferred schema.
4. Flatten `user.id` and `user.department`.
5. Explode `tags`.
6. Write the flattened result to `demo.training_json_events`.

Expected discussion:

- JSON can contain nested structs and arrays.
- Flattening makes nested JSON easier to query in tables.
- Exploding arrays changes the number of rows.

### Case 5: Delta Table Operations

Goal: show why Delta is useful for repeatable ETL.

1. Read `demo.example_events`.
2. Append two rows.
3. Query the new row count.
4. Run `DESCRIBE HISTORY demo.example_events`.
5. Overwrite intentionally and inspect history again.

Expected discussion:

- Append and overwrite are different write modes.
- Delta keeps transaction history.
- Repeatable scripts should be explicit about write mode.

## Troubleshooting

If `SHOW TABLES IN demo` fails:

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS demo")
```

If `demo.pdf_chunks` does not exist, rerun the notebook cells under `Parse PDFs And Chunk Text`.

If a table exists but the data looks stale, rerun the write cell that creates it.

If Spark cannot resolve Delta packages, the first Spark startup may still be downloading dependencies. Restart the kernel after the first successful startup if needed.

If you want a clean local table:

```python
spark.sql("DROP TABLE IF EXISTS demo.example_events")
```

For file cleanup, remove only the matching directory under `/workspace/warehouse/demo/` when you are sure the data is not needed.

## Official Links

- PySpark DataFrame quickstart: https://spark.apache.org/docs/3.5.3/api/python/getting_started/quickstart_df.html
- PySpark DataFrame API: https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
- PySpark functions API: https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/functions.html
- Spark JSON files guide: https://spark.apache.org/docs/3.5.3/sql-data-sources-json.html
- Spark data sources guide: https://spark.apache.org/docs/3.5.3/sql-data-sources.html
- Delta Lake quick start: https://docs.delta.io/quick-start/
- Databricks DataFrame tutorial: https://docs.databricks.com/gcp/en/getting-started/dataframes
- Databricks Delta tutorial: https://docs.databricks.com/aws/en/delta/tutorial
