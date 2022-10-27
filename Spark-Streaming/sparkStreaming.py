import sys
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
words = dataStream.window(300, 2).flatMap(lambda line: line.split())

hashtags = words.filter(lambda w: "#" in str(w))\
                .filter(lambda w: not (('http' in w) or ('\\u' in w) or (len(w)==1) ))\
                .map(lambda x: (x.lower(), 1))\
                .reduceByKey(add)

# word_count = words.filter(lambda w: "#" in str(w)).map(lambda x: (x.lower(), 1))\
#     .reduceByKey(add).transform(lambda rdd: rdd.filter(lambda x: x[1]> 2) )


# hashtags.pprint(20)

# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_count)\
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

# # Debuging code 
tags_totals.pprint(2)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()


