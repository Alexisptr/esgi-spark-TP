from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("TP4") \
    .master("local[*]") \
    .enableHiveSupport() \
    .getOrCreate()

mnm_file = "data/mnm.csv"

mnm_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(mnm_file)

mnm_df.createOrReplaceTempView("mnm_table")

# 1
spark.sql("""SELECT State, SUM(Count) AS Total
FROM mnm_table
GROUP BY State
ORDER BY Total DESC""").show(n=10, truncate=False)

# 2
spark.sql("""SELECT State, Color, SUM(Count) As Total
FROM mnm_table
WHERE State == "CA"
GROUP BY State, Color
ORDER BY Total DESC""").show(n=10, truncate=False)