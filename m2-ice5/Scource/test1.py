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
#  Concatenate columns
station_data_df.select(concat(col("lat"), lit(" "), col("long")).alias("loc")).show(10, False)
# distinct
station_data_df.select("dockcount").distinct().show()
#  dataframe
station_data_df.write.parquet("data.parquet")
# Show vertics
g.vertices.show(10, False)
# Show edges
g.edges.show(10, False)
# Vertex in-degree
g.inDegrees.show(10, False)
# Vertex out-degree
g.outDegrees.show(10, False)
# Bonus
g.degrees.show(10, False)
# Bonus
g.find("(a)-[e]->(b); (b)-[e2]->(a)").distinct().show(10, False)
# Bonus
g.vertices.write.parquet("v")
g.edges.write.parquet("e")
