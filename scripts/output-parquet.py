from pyspark import SparkContext
from pyspark.sql import SQLContext
from IPython import embed

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

sc = SparkContext()
sqlContext =SQLContext(sc)
df = sqlContext.read.json("sampledata/sample")

df.printSchema()

def key_a(params):
    a = list(filter(lambda x: x.key == "a", params))
    return a[0].value

my_udf = udf(key_a, StringType())

df2 = df.withColumn("a", my_udf(df.params))

df2.printSchema()

df2.write.parquet("output/parquet")

embed()

# https://changhsinlee.com/pyspark-udf/
