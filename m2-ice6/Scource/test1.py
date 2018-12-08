from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit
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
trip_data_df = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).load("trip_data.csv")
station_data_df = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).load("station_data.csv")
# Create vertex and edge dataframes
v = station_data_df.select(col("name").alias("id"), "lat", "long")
e = trip_data_df.select(col("Start Station").alias("src"), col("End Station").alias("dst"), col("Subscriber Type").alias("relationship"))
# Create graph
g = GraphFrame(v, e)
# Triangle count
results = g.triangleCount()
results.select("id", "count").show(10, False)
# Find shortest path
results = g.shortestPaths(landmarks=["2nd at Folsom", "California Ave Caltrain Station"])
results.select("id", "distances").show(10, False)
# page rank
results = g.pageRank(resetProbability=0.15, tol=0.01)
results.vertices.select("id", "pagerank").show(5, False)
results.edges.select("src", "dst", "weight").distinct().show(5, False)
g.vertices.write.parquet("v")
g.edges.write.parquet("e")

# Bonus
communities = g.labelPropagation(maxIter=5)
communities.persist().show(5, False)

# Bonus
paths = g.bfs("id='Ryland Park'", "lat=37.342725")
paths.show(10, False)
