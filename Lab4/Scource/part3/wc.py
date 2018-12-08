from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple
import pyspark as ps
import warnings
from pyspark.sql import SQLContext

try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    sc = ps.SparkContext('local[4]')
    sqlContext = SQLContext(sc)
    print("Just created a SparkContext")
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
sc.master
streamingc = StreamingContext(sc, 3)
Tweet = namedtuple("Data", ("tag", "count"))
# Split each line into words and use map reduce to count occurance of token then print word count
streamingc.socketTextStream("localhost", 8000).flatMap(lambda line: line.split(" ")).map(lambda word: (word.lower(), 1)).reduceByKey(lambda x, y: x + y).map(lambda rec: Tweet(rec[0], rec[1])).pprint()

# Start spark streaming 
streamingc.start()
streamingc.awaitTermination()