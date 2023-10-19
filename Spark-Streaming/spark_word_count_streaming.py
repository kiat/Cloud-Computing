import sys
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# create spark configuration
conf = SparkConf()
conf.setAppName("WordCount")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_WordCount_App")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
words = dataStream.window(300, 2).flatMap(lambda line: line.split())

word_count = words.map(lambda x: (x.lower(), 1))\
     .reduceByKey(add).transform(lambda rdd: rdd.filter(lambda x: x[1]> 2) )


# adding the count 
result = word_count.updateStateByKey(aggregate_count)\
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# printing 
result.pprint(2)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()


