from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, udf, when, desc, sum, dense_rank

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

# 1
delay_flights_df.filter(col("distance") > 1000) \
    .show()

# 2
delay_flights_df.select("date", "delay", "origin", "destination") \
    .filter(col("delay") > 120) \
    .filter(col("origin") == "SFO") \
    .filter(col("destination") == "ORD") \
    .orderBy("delay", ascending=False) \
    .show()

# 3
delay_flights_df.select("delay", "origin", "destination") \
    .withColumn("delay_type",
                when(col("delay") >= 360, "Très long retard")
                .when((col("delay") >= 120) & (col("delay") < 360), "Long retard")
                .when((col("delay") >= 60) & (col("delay") < 120), "Retard modéré")
                .when((col("delay") > 0) & (col("delay") < 60), "Retard tolérable")
                .when(col("delay") == 0, "Pas de retard")
                .otherwise("En avance")) \
    .orderBy("origin", desc("delay")) \
    .show()

# 4
df = delay_flights_df.select("origin", "destination", "delay") \
    .where(col("origin").isin("SEA", "SFO", "JFK")) \
    .where(col("destination").isin("SEA", "SFO", "JFK", "DEN", "ORD", "LAX", "ATL")) \
    .groupBy("origin", "destination") \
    .agg(sum("delay").alias("TotalDelays"))

df.show()

# 5
windowSpec = Window.partitionBy("origin").orderBy(desc("TotalDelays"))

df.withColumn("rank", dense_rank().over(windowSpec)) \
    .where(col("rank") <= 3) \
    .show()


# 6
def route(origin, destination):
    return origin + "-" + destination


route_col = udf(route, StringType())

delay_flights_df.withColumn('route', route_col('origin', 'destination')).show()
