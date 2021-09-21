from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("TP2") \
    .master("local[*]") \
    .getOrCreate()

text = spark.sparkContext.textFile("data/spark-wikipedia.txt")

print(text.count())
