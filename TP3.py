from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col

spark = SparkSession \
    .builder \
    .appName("TP3") \
    .master("local[*]") \
    .getOrCreate()

mnm_file = "data/mnm.csv"

mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

# 1
mnm_df.select("State", "Count") \
    .groupBy("State") \
    .agg(sum("Count").alias("Total")) \
    .orderBy("Total", ascending=False) \
    .show(n=10, truncate=False)

# 2
mnm_df.select("State", "Color", "Count") \
    .where(col("State") == "CA") \
    .groupBy("Color") \
    .agg(sum("Count").alias("Total")) \
    .orderBy("Total", ascending=False) \
    .show(n=10, truncate=False)