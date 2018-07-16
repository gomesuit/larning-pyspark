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

df.show()

def square(x):
    return x**2







# Registering UDF with integer type output
square_udf_int = udf(lambda z: square(z), IntegerType())
square_udf_float = udf(lambda z: square(z), FloatType())
df.select('integers',
          'floats',
          square_udf_int('integers').alias('int_squared'),
          square_udf_int('floats').alias('float_squared')).show()

df.select('integers',
          'floats',
          square_udf_float('integers').alias('int_squared'),
          square_udf_float('floats').alias('float_squared')).show()

def square_float(x):
    return float(x**2)
square_udf_float2 = udf(lambda z: square_float(z), FloatType())

df.select('integers',
          'floats',
          square_udf_float2('integers').alias('int_squared'),
          square_udf_float2('floats').alias('float_squared')).show()

def square_list(x):
    return [float(val)**2 for val in x]
square_list_udf = udf(lambda y: square_list(y), ArrayType(FloatType()))

df.select('integer_arrays', square_list_udf('integer_arrays')).show()






# Composite type outputs
import string

def convert_ascii(number):
    return [number, string.ascii_letters[number]]

convert_ascii(1)

array_schema = StructType([
    StructField('number', IntegerType(), nullable=False),
    StructField('letters', StringType(), nullable=False)
])

spark_convert_ascii = udf(lambda z: convert_ascii(z), array_schema)

df_ascii = df.select('integers', spark_convert_ascii('integers').alias('ascii_map'))
df_ascii.show()


df_ascii.printSchema()



embed()
