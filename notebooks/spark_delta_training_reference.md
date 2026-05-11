# Spark And Delta Training Guide

Use this guide next to `01_localbricks_databricks_basics.ipynb`.

This is not meant to be a second notebook that everyone runs line by line. It is a teaching guide and exercise sheet. The example notebook creates the Spark session and writes the first tables. Your colleagues can then open a new notebook and work through the challenges here.

Assumption: the setup and Spark cells from the example notebook have already run.

Main paths and tables:

- PDFs are copied into `/workspace/data/raw/pdfs`
- JSON examples use `/workspace/data/raw/json`
- Delta table files live under `/workspace/warehouse`
- Training schema: `demo`
- Existing tables after the example notebook runs: `demo.training_events`, `demo.pdf_chunks`

## What We Are Practicing

The session is about the shape of a data engineering task:

1. Read raw data.
2. Inspect what arrived.
3. Transform it into useful columns.
4. Validate row counts and important fields.
5. Write a Delta table.
6. Query that table again.

The PDF workflow is useful because it turns messy documents into table rows. That is also the first step of many RAG systems: before any AI is involved, documents need to become clean, traceable chunks with source information.

For this session, keep the RAG idea simple:

- A PDF is a source document.
- A chunk is a smaller piece of text from that document.
- A chunk table lets us filter, count, search, and validate those pieces.
- A future RAG system would need this table to be clean before it could retrieve useful context.

## How Spark Thinks About Data

Spark code often looks like normal Python, but the mental model is different.

A DataFrame is a table-like description of data. When you write:

```python
chunks_df = spark.table("demo.pdf_chunks")
```

Spark does not load every row into local Python memory. It creates a DataFrame object that knows how to read the table when an action is requested.

Common transformations describe new DataFrames:

```python
filtered_df = chunks_df.filter("source IS NOT NULL")
selected_df = filtered_df.select("source", "chunk_id", "text")
```

Common actions actually run work:

```python
selected_df.show(truncate=False)
selected_df.count()
```

Teaching point: beginners often expect every line to run immediately. In Spark, transformations build a plan; actions execute the plan.

## First Checks In A New Notebook

Start a learner notebook with small checks. The goal is to confirm that the Spark session and training tables are available.

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
chunks_df.show(5, truncate=False)
chunks_df.printSchema()
```

What to ask learners:

- What columns are in each table?
- Which table came from handwritten example rows?
- Which table came from document processing?
- Which columns would help us trace a chunk back to its original PDF?

## Core DataFrame Skills

Use `demo.training_events` for the first few examples because it is tiny.

Load the table:

```python
events_df = spark.table("demo.training_events")
```

Look at rows:

```python
events_df.show(truncate=False)
```

Look at column names and types:

```python
events_df.printSchema()
```

Select only some columns:

```python
events_df.select("topic", "status").show(truncate=False)
```

Filter rows:

```python
events_df.filter(events_df.status == "started").show(truncate=False)
events_df.where("topic = 'spark'").show(truncate=False)
```

Sort rows:

```python
events_df.orderBy("topic").show(truncate=False)
```

Add a new column:

```python
from pyspark.sql import functions as F

events_with_metadata_df = (
    events_df
    .withColumn("topic_upper", F.upper("topic"))
    .withColumn("loaded_at", F.current_timestamp())
)

events_with_metadata_df.show(truncate=False)
```

Group rows:

```python
(
    events_df
    .groupBy("status")
    .agg(F.count("*").alias("row_count"))
    .orderBy("status")
    .show(truncate=False)
)
```

Same idea in SQL:

```python
spark.sql("""
SELECT status, count(*) AS row_count
FROM demo.training_events
GROUP BY status
ORDER BY status
""").show(truncate=False)
```

Teaching point: DataFrame code and SQL are both valid. DataFrame code is convenient when building scripts; SQL is often convenient for exploration and reviews.

## The ETL Pattern

A beginner-friendly Spark script usually becomes easier to read when each stage has a clear name.

```python
from pathlib import Path
from pyspark.sql import functions as F

raw_df = spark.table("demo.pdf_chunks")

clean_df = (
    raw_df
    .filter(F.col("text").isNotNull())
    .filter(F.length("text") > 0)
)

result_df = (
    clean_df
    .withColumn("text_length", F.length("text"))
    .withColumn("search_text", F.lower("text"))
)

output_path = Path("/workspace/warehouse/demo/example_clean_chunks")

(
    result_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.example_clean_chunks")
spark.sql(f"""
CREATE TABLE demo.example_clean_chunks
USING delta
LOCATION '{output_path.as_posix()}'
""")
```

What this teaches:

- `raw_df` is what arrived.
- `clean_df` removes rows we do not want.
- `result_df` adds useful columns.
- The final write makes the work reusable.

Common mistake: writing a table before inspecting it. Encourage learners to run `show`, `printSchema`, and `count` before every write.

## Delta Tables In Plain Language

A Delta table is a folder of data files plus a transaction log. In this local stack, the data files live under `/workspace/warehouse`.

You can read a Delta table by table name:

```python
spark.table("demo.pdf_chunks").show(5, truncate=False)
```

You can also read by path:

```python
spark.read.format("delta").load("/workspace/warehouse/demo/pdf_chunks").show(5, truncate=False)
```

Use table names for normal work. Use paths when you are debugging storage or creating a table from files.

Inspect table history:

```python
spark.sql("DESCRIBE HISTORY demo.pdf_chunks").show(truncate=False)
```

Write modes matter:

```python
df.write.format("delta").mode("append").save(path)
df.write.format("delta").mode("overwrite").save(path)
```

Teaching point: `append` adds rows. `overwrite` replaces the table contents at that path. In training, overwrite is convenient. In production, it should be deliberate.

## Document Chunks As Data

The example notebook turns PDFs into rows like this:

- `source`: the file name
- `chunk_id`: the position of the chunk within that file
- `text`: the chunk content

That table is already useful before AI exists. We can answer questions such as:

- How many chunks came from each PDF?
- Which chunks are empty or very short?
- Which chunks contain a keyword?
- Which source document should we inspect manually?
- Are chunk IDs unique within each source?

These are real ETL questions. A future RAG system would depend on this basic quality work.

## Challenge 1: Open A Table And Ask Basic Questions

Goal: practice reading a table and inspecting it.

Starter code:

```python
events_df = spark.table("demo.training_events")
```

Tasks:

1. Show all rows.
2. Print the schema.
3. Count the rows.
4. Select only `topic` and `status`.
5. Filter to rows where `status` is `started`.

Hint:

Use `show`, `printSchema`, `count`, `select`, and `filter`.

Solution:

```python
events_df.show(truncate=False)
events_df.printSchema()
events_df.count()

events_df.select("topic", "status").show(truncate=False)

(
    events_df
    .filter(events_df.status == "started")
    .show(truncate=False)
)
```

What you learned:

- How to open a table as a DataFrame.
- How to inspect data before changing it.
- How to keep only the columns and rows you need.

## Challenge 2: Understand The PDF Chunk Table

Goal: treat document chunks as normal table rows.

Starter code:

```python
chunks_df = spark.table("demo.pdf_chunks")
```

Tasks:

1. Show the first 10 chunks.
2. Print the schema.
3. Count all chunks.
4. Count chunks per `source`.
5. Show a short preview of the chunk text instead of the full text.

Hint:

Use `groupBy`, `count`, `substring`, and `orderBy`.

Solution:

```python
from pyspark.sql import functions as F

chunks_df.show(10, truncate=False)
chunks_df.printSchema()
chunks_df.count()

(
    chunks_df
    .groupBy("source")
    .agg(F.count("*").alias("chunk_count"))
    .orderBy("source")
    .show(truncate=False)
)

(
    chunks_df
    .select(
        "source",
        "chunk_id",
        F.substring("text", 1, 120).alias("preview"),
    )
    .orderBy("source", "chunk_id")
    .show(20, truncate=False)
)
```

What you learned:

- A document processing result can be explored like any other table.
- `source` and `chunk_id` are basic lineage columns.
- Text columns are easier to inspect with previews.

## Challenge 3: Add Useful Chunk Metadata

Goal: create columns that make chunks easier to validate and search.

Starter code:

```python
from pyspark.sql import functions as F

chunks_df = spark.table("demo.pdf_chunks")
```

Tasks:

1. Add a `text_length` column.
2. Add a `search_text` column with lowercase text.
3. Add a `contains_spark` column that is true when the text contains `spark`.
4. Show the longest chunks first.

Hint:

Use `F.length`, `F.lower`, `contains`, and `F.desc`.

Solution:

```python
chunk_metadata_df = (
    chunks_df
    .withColumn("text_length", F.length("text"))
    .withColumn("search_text", F.lower("text"))
    .withColumn("contains_spark", F.col("search_text").contains("spark"))
)

(
    chunk_metadata_df
    .select("source", "chunk_id", "text_length", "contains_spark")
    .orderBy(F.desc("text_length"))
    .show(20, truncate=False)
)
```

What you learned:

- Metadata columns make raw text easier to work with.
- Lowercase search columns make simple keyword filters more reliable.
- Sorting by length helps find unusual chunks.

## Challenge 4: Build A Clean Chunk Table

Goal: write a reusable Delta table from transformed chunks.

Starter code:

```python
from pathlib import Path
from pyspark.sql import functions as F

chunks_df = spark.table("demo.pdf_chunks")
```

Tasks:

1. Keep only chunks where `text` is not null.
2. Keep only chunks where `text` length is greater than zero.
3. Add `text_length`, `search_text`, and `loaded_at`.
4. Write the result to `/workspace/warehouse/demo/cleaned_pdf_chunks`.
5. Register the table as `demo.cleaned_pdf_chunks`.
6. Query the new table.

Hint:

Build this in stages: `raw_df`, `clean_df`, `result_df`, then write.

Solution:

```python
raw_df = spark.table("demo.pdf_chunks")

clean_df = (
    raw_df
    .filter(F.col("text").isNotNull())
    .filter(F.length("text") > 0)
)

result_df = (
    clean_df
    .withColumn("text_length", F.length("text"))
    .withColumn("search_text", F.lower("text"))
    .withColumn("loaded_at", F.current_timestamp())
)

output_path = Path("/workspace/warehouse/demo/cleaned_pdf_chunks")

(
    result_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.cleaned_pdf_chunks")
spark.sql(f"""
CREATE TABLE demo.cleaned_pdf_chunks
USING delta
LOCATION '{output_path.as_posix()}'
""")

spark.table("demo.cleaned_pdf_chunks").show(10, truncate=False)
```

What you learned:

- Cleaning and enrichment are normal ETL steps.
- A Delta write turns notebook work into a reusable table.
- Registering a table gives other code a stable name to query.

## Challenge 5: Prepare Chunks For A Future RAG System

Goal: create a clean chunk table with the minimum fields a later retrieval system would need.

No AI is used in this task. The point is to prepare good data.

Starter code:

```python
from pathlib import Path
from pyspark.sql import functions as F

chunks_df = spark.table("demo.pdf_chunks")
```

Tasks:

1. Build a DataFrame with `source`, `chunk_id`, `text`, `text_length`, and `search_text`.
2. Add a `chunk_key` column that combines source and chunk ID.
3. Filter out chunks shorter than 20 characters.
4. Write the result to `demo.rag_ready_chunks`.
5. Search the table for one keyword from your PDFs.

Hint:

Use `F.concat_ws` for the key and `F.lower` for simple search.

Solution:

```python
rag_ready_df = (
    chunks_df
    .filter(F.col("text").isNotNull())
    .withColumn("text_length", F.length("text"))
    .filter(F.col("text_length") >= 20)
    .withColumn("search_text", F.lower("text"))
    .withColumn("chunk_key", F.concat_ws(":", F.col("source"), F.col("chunk_id")))
    .select("chunk_key", "source", "chunk_id", "text", "text_length", "search_text")
)

output_path = Path("/workspace/warehouse/demo/rag_ready_chunks")

(
    rag_ready_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.rag_ready_chunks")
spark.sql(f"""
CREATE TABLE demo.rag_ready_chunks
USING delta
LOCATION '{output_path.as_posix()}'
""")

(
    spark.table("demo.rag_ready_chunks")
    .filter(F.col("search_text").contains("spark"))
    .select("chunk_key", "source", "chunk_id", F.substring("text", 1, 160).alias("preview"))
    .show(20, truncate=False)
)
```

What you learned:

- A future retrieval system needs stable chunk identity.
- Chunk text should keep source metadata attached.
- Simple keyword search is not full RAG, but it teaches why clean chunks matter.

## Challenge 6: JSON ETL

Goal: turn nested JSON into a clean table.

Generate sample JSON:

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

Tasks:

1. Read `/workspace/data/raw/json/events.json`.
2. Print the schema.
3. Flatten `user.id` into `user_id`.
4. Flatten `user.department` into `department`.
5. Keep `event_id`, `user_id`, `department`, `action`, and `duration_seconds`.
6. Write the result to `demo.training_json_events`.

Hint:

Nested fields can be selected with `F.col("user.id")`.

Solution:

```python
from pathlib import Path
from pyspark.sql import functions as F

raw_json_df = spark.read.json("/workspace/data/raw/json/events.json")

raw_json_df.show(truncate=False)
raw_json_df.printSchema()

clean_json_df = raw_json_df.select(
    F.col("event_id").cast("int"),
    F.col("user.id").alias("user_id"),
    F.col("user.department").alias("department"),
    F.col("action"),
    F.col("duration_seconds").cast("int"),
)

output_path = Path("/workspace/warehouse/demo/training_json_events")

(
    clean_json_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path.as_posix())
)

spark.sql("DROP TABLE IF EXISTS demo.training_json_events")
spark.sql(f"""
CREATE TABLE demo.training_json_events
USING delta
LOCATION '{output_path.as_posix()}'
""")

spark.table("demo.training_json_events").show(truncate=False)
```

Optional extension:

```python
event_tags_df = (
    raw_json_df
    .select("event_id", "action", F.explode("tags").alias("tag"))
    .orderBy("event_id", "tag")
)

event_tags_df.show(truncate=False)
```

What you learned:

- Spark can infer a JSON schema.
- Nested JSON can be flattened into normal table columns.
- Arrays can become multiple rows with `explode`.

## Challenge 7: Validate An ETL Job

Goal: check whether a transformation did what you expected.

This challenge assumes Challenge 4 has already created `demo.cleaned_pdf_chunks`.

Starter code:

```python
from pyspark.sql import functions as F

raw_df = spark.table("demo.pdf_chunks")
clean_df = spark.table("demo.cleaned_pdf_chunks")
```

Tasks:

1. Count raw rows.
2. Count clean rows.
3. Count null values in important columns.
4. Find duplicate `(source, chunk_id)` pairs.
5. Inspect Delta history for `demo.cleaned_pdf_chunks`.

Hint:

Use `count`, `where`, `isNull`, `groupBy`, and `DESCRIBE HISTORY`.

Solution:

```python
print(f"raw chunks: {raw_df.count()}")
print(f"clean chunks: {clean_df.count()}")

(
    clean_df
    .select(
        F.count(F.when(F.col("source").isNull(), "source")).alias("missing_source"),
        F.count(F.when(F.col("chunk_id").isNull(), "chunk_id")).alias("missing_chunk_id"),
        F.count(F.when(F.col("text").isNull(), "text")).alias("missing_text"),
    )
    .show(truncate=False)
)

(
    clean_df
    .groupBy("source", "chunk_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") > 1)
    .show(truncate=False)
)

spark.sql("DESCRIBE HISTORY demo.cleaned_pdf_chunks").show(truncate=False)
```

What you learned:

- ETL work includes validation, not just transformation.
- Row counts help catch accidental filters or duplicates.
- Delta history shows when and how a table was written.

## Host Guide

Use the first 10 to 15 minutes for a shared walkthrough, then let colleagues work in their own notebooks.

Suggested flow:

1. Run the example notebook through `demo.pdf_chunks`.
2. Explain the ETL pattern using `raw_df`, `clean_df`, and `result_df`.
3. Let everyone complete Challenges 1 and 2.
4. Pair people for Challenges 3 and 4.
5. Use Challenge 5 to connect chunk tables to future RAG work.
6. Use Challenge 6 only if the group is ready for JSON.
7. Finish with Challenge 7 to reinforce validation.

Questions to ask while teaching:

- What is the grain of this table: one row per what?
- Which columns identify the original source?
- Which transformations changed row count?
- Which transformations only added or changed columns?
- What would break if `source` or `chunk_id` were missing?
- What table would another notebook want to read tomorrow?

Expected beginner misunderstandings:

- Confusing a DataFrame variable with the persisted table.
- Forgetting that transformations are lazy until an action runs.
- Writing output before checking `show`, `printSchema`, and `count`.
- Using `collect()` for data that should stay in Spark.
- Treating `overwrite` as harmless after a table becomes important.

Quick extension tasks for faster learners:

- Change the keyword in Challenge 5.
- Add `word_count` using `F.size(F.split("text", "\\s+"))`.
- Find the shortest non-empty chunks.
- Create a table with one row per source PDF and summary counts.
- Compare the DataFrame version of a query with a SQL version.

## Compact Reference

Most snippets in this section use Spark SQL functions:

```python
from pyspark.sql import functions as F
```

Open a table:

```python
df = spark.table("demo.pdf_chunks")
```

Inspect:

```python
df.show(10, truncate=False)
df.printSchema()
df.count()
df.columns
```

Select and filter:

```python
df.select("source", "chunk_id").show(truncate=False)
df.filter(F.col("source").isNotNull()).show(truncate=False)
df.where("chunk_id = 0").show(truncate=False)
```

Add columns:

```python
df2 = (
    df
    .withColumn("text_length", F.length("text"))
    .withColumn("search_text", F.lower("text"))
)
```

Aggregate:

```python
(
    df
    .groupBy("source")
    .agg(F.count("*").alias("chunk_count"))
    .orderBy("source")
    .show(truncate=False)
)
```

Write Delta:

```python
path = "/workspace/warehouse/demo/my_table"
df.write.format("delta").mode("overwrite").save(path)
```

Register a table:

```python
spark.sql("DROP TABLE IF EXISTS demo.my_table")
spark.sql(f"""
CREATE TABLE demo.my_table
USING delta
LOCATION '{path}'
""")
```

Read JSON:

```python
json_df = spark.read.json("/workspace/data/raw/json/events.json")
json_df.printSchema()
```

Flatten JSON:

```python
flat_df = json_df.select(
    F.col("event_id"),
    F.col("user.id").alias("user_id"),
    F.col("user.department").alias("department"),
)
```

Inspect Delta history:

```python
spark.sql("DESCRIBE HISTORY demo.my_table").show(truncate=False)
```

## Official Links

- PySpark DataFrame quickstart: https://spark.apache.org/docs/3.5.3/api/python/getting_started/quickstart_df.html
- PySpark DataFrame API: https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
- PySpark functions API: https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/functions.html
- Spark JSON files guide: https://spark.apache.org/docs/3.5.3/sql-data-sources-json.html
- Spark data sources guide: https://spark.apache.org/docs/3.5.3/sql-data-sources.html
- Delta Lake quick start: https://docs.delta.io/quick-start/
- Databricks DataFrame tutorial: https://docs.databricks.com/gcp/en/getting-started/dataframes
- Databricks Delta tutorial: https://docs.databricks.com/aws/en/delta/tutorial
