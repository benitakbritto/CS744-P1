from pyspark.sql import SparkSession

# Create Spark context
spark = SparkSession.builder.appName("SortingApp").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load DF into Spark
df = spark.read.format("csv").option("header", "true").load("hdfs://10.10.1.1:9000/part1/export.csv")

print(f"Data read from HDFS: {df.count()} rows")

# Sort the dataframe first on country code and then timestamp
df = df.sort("cca2", "timestamp")

df.write.option("header", True).csv("hdfs://10.10.1.1:9000/part1/sorted.csv")

print(f"Data sorted and stored in HDFS: {df.count()} rows")

spark.stop()
