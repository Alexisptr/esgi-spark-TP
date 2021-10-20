from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("TP3-bis") \
    .master("local[*]") \
    .getOrCreate()

delay_flights_file = "data/delay_flights.csv"

delay_flights_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(delay_flights_file)

