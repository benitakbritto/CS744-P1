from pyspark.sql import SparkSession
import argparse

# Default parameters set
APP_NAME = "SortingApp"

# Parsing command line args
parser = argparse.ArgumentParser()
parser.add_argument("--input", type=str, help="The path of the input file.")
parser.add_argument("--output", type=str, help="The path of the output file.")
args = parser.parse_args()

# Create Spark context
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load DF into Spark
df = spark.read.format("csv").option("header", "true").load(args.input)
print(f"Data read from HDFS: {df.count()} rows")

# Sort the dataframe first on country code and then timestamp
df = df.sort("cca2", "timestamp")
df.write.\
   mode("overwrite").\
   option("header", True).\
   csv(args.output)
   
print(f"Data sorted and stored in HDFS: {df.count()} rows")

spark.stop()
