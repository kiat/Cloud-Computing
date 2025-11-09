# Read the main documentation here https://spark.apache.org/docs/latest/streaming/index.html
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, count, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("StructuredStreamingWordCount") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



flightSchema = StructType() \
    .add("YEAR", IntegerType(), True) \
    .add("MONTH", IntegerType(), True) \
    .add("DAY", IntegerType(), True) \
    .add("DAY_OF_WEEK", IntegerType(), True) \
    .add("AIRLINE", StringType(), True) \
    .add("FLIGHT_NUMBER", IntegerType(), True) \
    .add("TAIL_NUMBER", StringType(), True) \
    .add("ORIGIN_AIRPORT", StringType(), True) \
    .add("DESTINATION_AIRPORT", StringType(), True) \
    .add("SCHEDULED_DEPARTURE", IntegerType(), True) \
    .add("DEPARTURE_TIME", IntegerType(), True) \
    .add("DEPARTURE_DELAY", IntegerType(), True) \
    .add("TAXI_OUT", IntegerType(), True) \
    .add("WHEELS_OFF", IntegerType(), True) \
    .add("SCHEDULED_TIME", IntegerType(), True) \
    .add("ELAPSED_TIME", IntegerType(), True) \
    .add("AIR_TIME", IntegerType(), True) \
    .add("DISTANCE", IntegerType(), True) \
    .add("WHEELS_ON", IntegerType(), True) \
    .add("TAXI_IN", IntegerType(), True) \
    .add("SCHEDULED_ARRIVAL", IntegerType(), True) \
    .add("ARRIVAL_TIME", IntegerType(), True) \
    .add("ARRIVAL_DELAY", IntegerType(), True) \
    .add("DIVERTED", IntegerType(), True) \
    .add("CANCELLED", IntegerType(), True) \
    .add("CANCELLATION_REASON", StringType(), True) \
    .add("AIR_SYSTEM_DELAY", IntegerType(), True) \
    .add("SECURITY_DELAY", IntegerType(), True) \
    .add("AIRLINE_DELAY", IntegerType(), True) \
    .add("LATE_AIRCRAFT_DELAY", IntegerType(), True) \
    .add("WEATHER_DELAY", IntegerType(), True)


# Download the file from here 
# https://www.cs.utexas.edu/~kiat/datasets/flights.csv.bz2
# mkdir flights 
# move the file into that folder
# unzip it. 
# bunzip2 flights.csv.bz2
# you should have inside flights folder a file with name flights.csv 
# 565M 

flights = spark \
    .readStream \
    .option("sep", ",") \
    .schema(flightSchema) \
    .csv("flights")

flights.printSchema()


# Add timestamp column
lines = flights.withColumn("timestamp", current_timestamp())

# Select only AIRLINE
# 
windowed_counts = lines \
    .select("timestamp", "AIRLINE")\
    .groupBy(
        window(col("timestamp"), "10 seconds", "2 seconds"),
        col("AIRLINE")
    ).count()

# Output to console
# We can have different output mode like "completed"
query = windowed_counts.writeStream \
    .trigger(processingTime='2 seconds')\
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()


