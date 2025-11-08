# Read the main documentation here https://spark.apache.org/docs/latest/streaming/index.html
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, count, current_timestamp

spark = SparkSession.builder \
    .appName("StructuredStreamingWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read text stream from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9009) \
    .load()

# Add timestamp column
lines = lines.withColumn("timestamp", current_timestamp())

# Split lines into words and keep timestamp
words = lines.select(
    explode(split(col("value"), " ")).alias("word"),
    col("timestamp")  # <-- keep the timestamp of the value.
)

# Windowed word count with watermark
windowed_counts = words.withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "4 minutes", "5 seconds"),
        col("word")
    ).count()

# Output to console
# We can have different output mode like "completed"
query = windowed_counts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()