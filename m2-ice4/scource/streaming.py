from pyspark import SparkContext
from pyspark.streaming import StreamingContext


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
# Change log level to error
logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
lines = ssc.textFileStream('F:\spark\m2-ice4\scource\log')  #'log/ mean directory name
counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
counts.pprint()
ssc.start()
ssc.awaitTermination()
