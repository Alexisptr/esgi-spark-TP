from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder \
    .appName("TP4-bis") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

delay_flights_file = "data/delay_flights.csv"

delay_flights_df = spark.read.format("csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(delay_flights_file)

delay_flights_df.createOrReplaceTempView("delay_flights_table")
