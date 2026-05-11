from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import time
import os

spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("url", StringType(), True),
    StructField("ts", TimestampType(), True)
])

stream = (
    spark.readStream
    .schema(schema)
    .json("data/incoming")
)

by_source = (
    stream.groupBy("source")
    .count()
)

def write_to_json(df, epoch_id, filename):
    path = os.path.join("data/processed", filename)
    df.toPandas().to_json(path, orient="records")

q_source = (
    by_source.writeStream
    .outputMode("complete")
    .foreachBatch(lambda df, epoch: write_to_json(df, epoch, "by_source.json"))
    .start()
)

by_window = (
    stream.withWatermark("ts", "2 hours")
    .groupBy(F.window("ts", "1 hour"))
    .count()
)

q_window = (
    by_window.writeStream
    .outputMode("complete")
    .foreachBatch(lambda df, epoch: write_to_json(df, epoch, "by_window.json"))
    .start()
)

stop_words = ["the", "and", "for", "with", "that", "this", "from", "are", "was", "you", "your", "but", "not", "have", "has", "his", "her", "its", "our", "their", "into", "about", "after"]

words = (
    stream.select(F.explode(F.split(F.lower(F.col("title")), " ")).alias("word"))
    .withColumn("word", F.regexp_replace(F.col("word"), "[^a-z]", ""))
    .filter(F.length("word") > 3)
    .filter(~F.col("word").isin(stop_words))
)

top_words = (
    words.groupBy("word")
    .count()
)

q_words = (
    top_words.writeStream
    .outputMode("complete")
    .foreachBatch(lambda df, epoch: write_to_json(df, epoch, "top_words.json"))
    .start()
)

print("Streaming job started.")
print("Saving latest state to: data/processed/*.json")
print("\nWaiting for data to arrive from ingester...")
print("This may take a few seconds to process the first batch...\n")

spark.streams.awaitAnyTermination()