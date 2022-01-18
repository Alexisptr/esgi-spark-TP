from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("topic", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("device", StringType(), False),
    StructField("value", IntegerType(), False),
])

spark = SparkSession \
    .builder \
    .appName("TP6") \
    .master("local[*]") \
    .getOrCreate()

