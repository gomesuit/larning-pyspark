from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf

sc = SparkContext()
sqlContext =SQLContext(sc)

maturity_udf = udf(lambda age: "adult" if age >=18 else "child", StringType())

df = sqlContext.createDataFrame([{'name': 'Alice', 'age': 1}])
df2 = df.withColumn("maturity", maturity_udf(df.age))

df2.printSchema
df2.show()
