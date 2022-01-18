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

# Initialisation
stream = spark \
    .readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load() \

query = stream.writeStream.format("console")

# query.start().awaitTermination()

jsonStream = stream.select(from_json(col("value"), schema).alias("json")).select("json.*")

# 1
simpleQuery = jsonStream \
    .writeStream \
    .format("console")

# simpleQuery.start().awaitTermination()

# 2
sumQuery = jsonStream \
    .groupBy("device") \
    .avg("value") \
    .writeStream \
    .outputMode("complete") \
    .format("console")

# sumQuery.start().awaitTermination()

# 3
windowedQuery = jsonStream \
    .groupBy(window("timestamp", "10 seconds", "5 seconds"), "device") \
    .avg("value") \
    .writeStream \
    .outputMode("complete") \
    .format("console")

windowedQuery.start().awaitTermination()
