from pyspark.sql import SparkSession

NUMBER_NODES = 685230

# Create Spark context
spark = SparkSession.builder.appName("PageRank").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load RDD into Spark
rdd = spark.sparkContext.textFile("hdfs://10.10.1.1:9000/part2/datasets/web-BerkStan.txt")

# Cleaning up the commented lines
rdd = rdd.filter(lambda line: '#' not in line)

# Splitting tab separated lines into two columns
rdd = rdd.map(lambda line: line.split('\t'))

# Peek the RDD
rdd.toDF(["From", "To"]).show()

neighbours = rdd.groupByKey().mapValues(list)

neighbours.toDF(["Node", "Neighbours"]).show()

spark.stop()
