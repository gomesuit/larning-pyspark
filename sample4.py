from pyspark import SparkContext
from pyspark.sql import SQLContext
from IPython import embed

from pyspark.sql.types import StringType
from pyspark.sql.functions import UserDefinedFunction

sc = SparkContext()
sqlContext =SQLContext(sc)
df = sqlContext.read.json("sampledata/sample")

df.printSchema()

def aaa(params):
    a = list(filter(lambda x: x.key == "a", params))
    return a[0].value

my_udf3 = UserDefinedFunction(aaa, StringType())

df2 = df.withColumn("a", my_udf3(df.params))

df2.printSchema()

embed()
