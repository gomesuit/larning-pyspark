from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import pandas as pd

from IPython import embed

sc = SparkContext()
sqlContext =SQLContext(sc)

maturity_udf = udf(lambda age: "adult" if age >=18 else "child", StringType())

df = sqlContext.createDataFrame([{'name': 'Alice', 'age': 1}])
df2 = df.withColumn("maturity", maturity_udf(df.age))

df2.printSchema
df2.show()



# https://changhsinlee.com/pyspark-udf/

# example data
df_pd = pd.DataFrame(
    data={'integers': [1, 2, 3],
     'floats': [-1.0, 0.5, 2.7],
     'integer_arrays': [[1, 2], [3, 4, 5], [6, 7, 8, 9]]}
)
df = sqlContext.createDataFrame(df_pd)
df.printSchema()

embed()
