from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import udf
from pyspark.sql import Row
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


#part 1
#.Import the dataset and create data framesdirectly on import
print("Import the dataset:")
data_df = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).load("ConsumerComplaints.csv")

# Save data to file.
print("Save data to file.:")
data_df.write.save("F:\spark\m2-ice3\scource\output", format="csv", header="true")

# Count the number of repeated record in the dataset.
print("Count the number of repeated record in the dataset.:")
print("no. of duplicate records: %d" % (data_df.count() - data_df.dropDuplicates().count()))

# Apply Union operation on the dataset and order the output by Company Name alphabetically.
print("Apply Union operation on the dataset and order the output by Company Name alphabetically.:")
data_df.union(data_df).orderBy(data_df["Company"].desc()).select("Company").show(10, False)

# Use Groupby Query based on Zip Codes
print("Use Groupby Query based on Zip Codes:")
data_df.groupBy("Zip Code").count().show()


# part 2
#
print("Apply join:")
print(data_df.crossJoin(data_df).columns)

# Apply aggregate
print("Apply aggregate:")
data_df.where("Company = 'Bank of America'").count()

# Show 13 rows
data_df.show(13, False)

# bonus Parse line with comma-delimited row
def splitFunc(line):
    return line.split(",")

# Create UDF
splitUDF = udf(splitFunc)
# Create dummy data
df = sc.parallelize([Row(input="a,b,c,d")]).toDF()
# Apply UDF
new_df = df.select(splitUDF("input"))
# Show data
new_df.show()
