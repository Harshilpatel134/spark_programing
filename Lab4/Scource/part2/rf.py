from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
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


data = sqlContext.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ";").load("Absenteeism_at_work.csv")
data = data.withColumnRenamed("Social drinker", "label").select("label", "Distance from Residence to Work", "Son", "Pet")
data = data.select(data.label.cast("double"), "Distance from Residence to Work", "Son", "Pet")


assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)


training, test = data.select("label", "features").randomSplit([0.75, 0.25])


rf = RandomForestClassifier()
model = rf.fit(training)


predictions = model.transform(test)


evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)


print("Accuracy:", accuracy)

predictionAndLabels = predictions.select("label", "prediction").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())
