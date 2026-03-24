#!/usr/bin/env python3
import os
import re
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, regexp_replace, trim

import unicodedata


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_PATH = f"file://{os.path.join(BASE_DIR, 'data_source', 'a.parquet')}"
DOCS_DIR = os.path.join(BASE_DIR, "data")
PREPARED_DIR = os.path.join(BASE_DIR, "prepared_input")

N_DOCS = 100



def safe_title(title: str) -> str:
    title = unicodedata.normalize("NFKD", title)
    title = title.encode("ascii", "ignore").decode("ascii")
    title = title.strip().replace(" ", "_")
    title = re.sub(r"[^A-Za-z0-9._-]+", "_", title)
    title = re.sub(r"_+", "_", title).strip("_")
    return title[:200] if title else "untitled"


def reset_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


spark = (
    SparkSession.builder
    .appName("prepare-wikipedia-docs")
    .master("local[*]")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.columnarReaderBatchSize", "64")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read.parquet(PARQUET_PATH)
    .select("id", "title", "text")
    .filter(col("id").isNotNull())
    .filter(col("title").isNotNull())
    .filter(col("text").isNotNull())
    .withColumn("title", trim(col("title")))
    .withColumn("text", trim(col("text")))
    .filter(length(col("title")) > 0)
    .filter(length(col("text")) > 0)
    .withColumn("text", regexp_replace(col("text"), r"[\r\n\t]+", " "))
    .limit(N_DOCS)
)

reset_dir(DOCS_DIR)

docs_rdd = df.rdd.map(
    lambda row: (
        str(row["id"]).strip(),
        str(row["title"]).strip(),
        str(row["text"]).strip(),
    )
).filter(
    lambda x: x[0] != "" and x[1] != "" and x[2] != ""
)

prepared_rdd = docs_rdd.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}").coalesce(1)

if os.path.exists(PREPARED_DIR):
    shutil.rmtree(PREPARED_DIR)

prepared_rdd.saveAsTextFile(f"file://{PREPARED_DIR}")


def write_partition(partition):
    for doc_id, doc_title, doc_text in partition:
        filename = f"{doc_id}_{safe_title(doc_title)}.txt"
        filepath = os.path.join(DOCS_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(doc_text)
    return iter(())


docs_rdd.foreachPartition(write_partition)

print(f"Saved documents to {DOCS_DIR}")
print(f"Prepared one-partition input in {PREPARED_DIR}")

spark.stop()