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

# 1
spark.sql("""SELECT *
FROM delay_flights_table
WHERE distance > 1000""").show()

# 2
spark.sql("""SELECT date, delay, origin, destination
FROM delay_flights_table
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show()

# 3
spark.sql("""SELECT delay, origin, destination, 
CASE 
WHEN delay >= 360 THEN 'Très long retard' 
WHEN delay >= 120 AND delay < 360 THEN 'Long retard' 
WHEN delay >= 60 AND delay < 120 THEN 'Retard modéré' 
WHEN delay > 0 and delay < 60 THEN 'Retard tolérable' 
WHEN delay = 0 THEN 'Pas de retard' 
ELSE 'En avance' 
END AS delay_type 
FROM delay_flights_table 
ORDER BY origin, delay DESC""").show()

# 4
spark.sql("""DROP TABLE IF EXISTS departureDelays""")

spark.sql("""CREATE TABLE departureDelays AS 
SELECT origin, destination, SUM(delay) AS TotalDelays 
FROM delay_flights_table 
WHERE origin IN ('SEA', 'SFO', 'JFK') 
AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
GROUP BY origin, destination""")

spark.sql("""SELECT * FROM departureDelays""").show()

# 5
spark.sql("""
SELECT origin, destination, TotalDelays, rank 
FROM ( 
    SELECT origin, destination, TotalDelays, dense_rank() 
    OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
    FROM departureDelays 
) t 
WHERE rank <= 3""").show()

# 6
def route(origin, destination):
    return origin + "-" + destination

spark.udf.register("route", route, StringType())

spark.sql("SELECT *, route(origin, destination) AS route FROM delay_flights_table").show()
