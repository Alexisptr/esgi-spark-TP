from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from time import sleep

spark = SparkSession \
    .builder \
    .appName("TP5") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", 6) \
    .getOrCreate()

mnm_file = "data/mnm.csv"

# 1
mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file) \
    .repartition(10)

# 2
mnm_df = mnm_df.select("State", "Count") \
    .groupBy("State") \
    .agg(sum("Count").alias("Total")) \
    .orderBy("Total", ascending=False) \

mnm_df.show(n=10, truncate=False)

# 5
mnm_df.cache()

for i in range(10):
    print(mnm_df.count())

sleep(1000)
