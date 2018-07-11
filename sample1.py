from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext =SQLContext(sc)
df = sqlContext.read.json("companies.json")
df.printSchema()
df.show(1)
df.take(1)
df.select()
