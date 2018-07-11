from pyspark import SparkContext
from pyspark.sql import SQLContext
from IPython import embed

sc = SparkContext()
sqlContext =SQLContext(sc)
df = sqlContext.read.json("sampledata/sample")
#df = sqlContext.read.json("sampledata/companies.json")
df.printSchema()
df.show(1)
df.take(1)
df.select()

#df.withColumn('id2', df.id).take(1)

a = [1,2,3]
b = sc.parallelize(a, 2)
#b.collect()
#b.map(lambda x: x + 1).collect()

embed()
