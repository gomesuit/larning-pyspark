from pyspark import SparkContext
from pyspark.sql import SQLContext
from IPython import embed

from pyspark.sql.types import StringType
from pyspark.sql.functions import UserDefinedFunction

sc = SparkContext()
sqlContext =SQLContext(sc)
df = sqlContext.read.json("sampledata/sample")

df.printSchema()
df.show(1)
df.take(1)
df.select()

def test_function(date):
    return date + "a"

# my_udf2 = UserDefinedFunction(lambda x: x + 'a', StringType())
my_udf2 = UserDefinedFunction(test_function, StringType())

df.withColumn("id2", my_udf2(df.date)).show(1)

# my_udf2 = UserDefinedFunction(lambda x, y: x + y, StringType())
# df.withColumn('id2', my_udf2(df.date, df.date)).show(1)

embed()
