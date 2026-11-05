# Read the main documentation here https://spark.apache.org/docs/latest/streaming/index.html
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, col, count, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import max, max_by


from pyspark.sql.functions import (
    col,
    from_csv,
    current_timestamp,
    window,
    count,
    desc
)

from collections import deque


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("FlightStreamAnalysis")
    .getOrCreate()
)

# spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setLogLevel("FATAL")


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

# flights = spark \
#     .readStream \
#     .option("sep", ",") \
#     .schema(flightSchema) \
#     .csv("flights")

# flights.printSchema()



# --------------------------------------------------
# 2. Read Flight Data from TCP Socket
# --------------------------------------------------

flight_stream = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9009)
    .load()
)


# --------------------------------------------------
# 3. Parse Text Stream Using flightSchema
# --------------------------------------------------

parsed_flights = (
    flight_stream
    .select(
        from_csv(
            col("value"),
              flightSchema.toDDL(),
            {"sep": ",", "mode": "PERMISSIVE"}
        ).alias("flight")
    )
    .select("flight.*")
)


# --------------------------------------------------
# 4. Add Processing Timestamp
# --------------------------------------------------

flights_with_time = (
    parsed_flights
    .withColumn(
        "processing_time",
        current_timestamp()
    )
)


# --------------------------------------------------
# 5. Create a Sliding Window
#    Window duration = 1 minute
#    Slide interval   = 5 seconds
# --------------------------------------------------

windowed_flights = (
    flights_with_time
    .withWatermark("processing_time", "1 minute")
    .groupBy(
        window(
            col("processing_time"),
            "1 minute",
            "5 seconds"
        ),
        col("AIRLINE")
    )
    .agg(
        count("*").alias("flight_count")
    )
)

# --------------------------------------------------
# 6. Find the Airline with the Most Flights
# --------------------------------------------------

from collections import deque

last_10_results = deque(maxlen=10)


def print_top_airline(batch_df, batch_id):

    # Ignore empty micro-batches
    if batch_df.isEmpty():
        return

    # Find the most recent window in this micro-batch
    latest_window = (
        batch_df
        .agg(max(col("window.end")).alias("latest"))
        .first()["latest"]
    )

    if latest_window is None:
        return

    # Select only the results for the latest window
    latest_counts = (
        batch_df
        .filter(col("window.end") == latest_window)
    )

    # Find the airline with the maximum flight count
    top_airline = (
        latest_counts
        .orderBy(
            desc("flight_count"),
            col("AIRLINE").asc()
        )
        .first()
    )

    if top_airline is None:
        return

    result = (
        batch_id,
        latest_window,
        top_airline["AIRLINE"],
        top_airline["flight_count"]
    )

    last_10_results.append(result)

    print("\n" + "=" * 80)
    print("TOP AIRLINE IN THE LATEST WINDOW")
    print("=" * 80)

    print(
        f"Batch ID: {batch_id}\n"
        f"Window End: {latest_window}\n"
        f"Airline: {top_airline['AIRLINE']}\n"
        f"Flight Count: {top_airline['flight_count']}"
    )

    print("\nLast 10 Results")
    print("-" * 80)

    for item in last_10_results:

        print(
            f"Batch: {item[0]:<5} | "
            f"Window End: {item[1]} | "
            f"Airline: {item[2]:<5} | "
            f"Flights: {item[3]}"
        )


# --------------------------------------------------
# 7. Start Structured Streaming Query
# --------------------------------------------------

query = (
    windowed_flights
    .writeStream
    .outputMode("update")
    .foreachBatch(print_top_airline)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()