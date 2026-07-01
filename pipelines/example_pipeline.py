from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="pipeline_training_events")
def pipeline_training_events():
    return spark.createDataFrame(
        [
            (1, "spark", "ready"),
            (2, "delta", "ready"),
            (3, "lakeflow", "local"),
        ],
        "id INT, topic STRING, status STRING",
    )


@dp.table(name="pipeline_topic_counts")
def pipeline_topic_counts():
    return (
        spark.table("unity.demo.pipeline_training_events")
        .groupBy("status")
        .agg(F.count("*").alias("row_count"))
        .orderBy("status")
    )
