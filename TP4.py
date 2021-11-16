from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("TP4") \
    .master("local[*]") \
    .getOrCreate()

mnm_file = "data/mnm.csv"

mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

mnm_df.createOrReplaceTempView("mnm_table")
