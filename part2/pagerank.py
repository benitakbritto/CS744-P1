from pyspark.sql import SparkSession
from pyspark import StorageLevel
from operator import add

# Create Spark context
spark = SparkSession.builder.appName("PageRank").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load RDD into Spark
#rdd = spark.sparkContext.textFile("hdfs://10.10.1.1:9000/part2/datasets/web-BerkStan.txt")

rdd = spark.sparkContext.textFile("hdfs://10.10.1.1:9000/part2/datasets/enwiki-pages-articles/link-enwiki-*")

# Cleaning up the data
rdd = rdd.\
    filter(lambda line: '#' not in line).\
    filter(lambda line: 'Category' not in line).\
    filter(lambda line: 'Image' not in line).\
    filter(lambda line: 'User' not in line)

# Splitting tab separated lines into two columns
rdd = rdd.map(lambda line: line.split('\t', 1))

# Peek the RDD
#rdd.toDF(["From", "To"]).show()

neighbours = rdd.groupByKey().mapValues(list)

# Peek the neighbour list
#neighbours.toDF(["Node", "Neighbours"]).show()

# Create the rank map
ranks = neighbours.mapValues(lambda x: 1.0)
#ranks.toDF(["Node", "Rank"]).show()

for _ in range(10):
    ranks = ranks.\
    join(neighbours).\
    flatMap(lambda row: [(node, row[1][0]/len(row[1][1])) for node in row[1][1]]).\
    reduceByKey(add).\
    map(lambda row: (row[0], 0.15 + 0.85*row[1]))

ranks.toDF(["Node", "Rank"]).show(50)

spark.stop()
