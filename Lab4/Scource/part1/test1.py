from graphframes import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
# Load vertics and edges
sc.master
v = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).load("meta-members.csv")\
    .select(col("member_id").alias("id"), col("name"))
e = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).load("member-edges.csv")\
    .select(col("member1").alias("src"), col("member2").alias("dst"), col("weight").alias("relationship"))
# Create graph
g = GraphFrame(v, e)
# Run PageRank
results = g.pageRank(resetProbability=0.20, tol=0.02)
# Display resulting pageranks and final edge weights
results.vertices.select("id", "pagerank").show(10, False)
results.edges.select("src", "dst", "weight").show(10, False)
