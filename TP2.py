from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("TP2") \
    .master("local[*]") \
    .getOrCreate()

text = spark.sparkContext.textFile("data/spark-wikipedia.txt")

textRdd = text.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda a: a[1], ascending=False) \
    .coalesce(1)

textRdd.saveAsTextFile("results/wordcount.csv")

print(textRdd.getNumPartitions())

accum = spark.sparkContext.accumulator(0)

text.filter(lambda l: "Spark" in l).foreach(lambda x: accum.add(1))

print(accum.value)
