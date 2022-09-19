
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add

# def aggregate_count(new_values, total_sum):
#     return sum(new_values) + (total_sum or 0)


# create spark configuration
conf = SparkConf()
conf.setAppName("Flight Data Stream")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_FlightDataApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)
# dataStream = ssc.textFileStream("./data")

# dataStream.pprint()

# Flight data is in the following format.
# Download the file from here

# Large data
# https://storage.googleapis.com/cs378/flights.csv.bz2
# Google Storage s3://cs378/flights.csv.bz2

# Small Data here on github
# https://github.com/kiat/Cloud-Computing/tree/main/Spark-Example-FlightsData
# wget https://raw.githubusercontent.com/kiat/Cloud-Computing/main/Spark-Example-FlightsData/flights_data_small.csv

# YEAR,MONTH,DAY,DAY_OF_WEEK,AIRLINE,FLIGHT_NUMBER,TAIL_NUMBER,ORIGIN_AIRPORT,DESTINATION_AIRPORT,SCHEDULED_DEPARTURE,DEPARTURE_TIME,DEPARTURE_DELAY,TAXI_OUT,WHEELS_OFF,SCHE

# Question-1: What are the top-10 airlines who have flights in the last 60 seconds?
#results = dataStream.map(lambda x: x.split(",")).map(lambda x: x[7]).window(60, 2).map(lambda x: (x, 1))\
# .reduceByKey(add)

# results.foreachRDD(lambda rdd: print(rdd.top(5, lambda x: x[1])) )




# Question-2: To how many distincs cities do we have flights in the last 60 seconds?

# results = dataStream.map(lambda x: x.split(",")).map(lambda x: x[8])\
# .transform(lambda rdd: rdd.distinct()).countByWindow(60, 2)

# .map(lambda x: (x, 1))
# .reduceByKey(add)


# results.pprint()



# Question-3: From each ORIGIN_AIRPORT to which destinations do we have flights in the last 10 seconds?
# def merge(x,y):
#     return x.add(y)


results = dataStream.map(lambda x: x.split(",")).map(lambda x: (x[7], x[8])).window(60, 2)\
.reduceByKey(lambda x, y: str(x)+","+str(y))

results.pprint()

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()
