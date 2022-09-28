'''
@file: pagerank_small.py

This file includes the PageRank algorithm for
the small dataset. 

@creator: Benita, Devansh, Hemal
'''
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from operator import add

app_name = "PageRank-BerkStanDataset"

# Create Spark context
spark = SparkSession.builder.appName(app_name).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

num_iterations = 10

# Load RDD into Spark
rdd = spark.\
      sparkContext.\
      textFile("hdfs://10.10.1.1:9000/part3/web-BerkStan.txt")

# Cleaning up the data, removing commented rows
rdd = rdd.\
    filter(lambda line: '#' not in line)

# Splitting tab separated lines into two columns
rdd = rdd.map(lambda line: line.split('\t', 1))

# Create neighbours RDD -> (key, list[neighbours])
neighbours = rdd.groupByKey().mapValues(list)

# Create the rank RDD -> (key, rank)
ranks = neighbours.mapValues(lambda x: 1.0)

# Execute 'num_iterations' iterations of the PageRank algorithm
for _ in range(num_iterations):
    # Join the ranks, neighbours 
    # RDD schema: (sourceNode, (rank, list[destinationNodes]))
    joinedRDD = neighbours.join(ranks)
    
    # Project the contributions that a node will make towards destination nodes
    # RDD schema: (destinationNode, contributionFromSource)
    flatMapRDD = joinedRDD.flatMap(lambda row: [(node, row[1][1]/len(row[1][0])) for node in row[1][0]])

    # Summing up all the contributions towards each node
    # RDD schema: (destinationNode, sumOfContributions)
    contributionsRDD = flatMapRDD.reduceByKey(add)

    # Weight the contributions towards each node and assign new ranks
    # RDD schema: (destinationNode, newRank)
    ranks = contributionsRDD.mapValues(lambda x: 0.15 + 0.85*x)

# Write back RDD to HDFS
ranks.\
    toDF(["Node", "Rank"]).\
    write.\
    mode("overwrite").\
    option("header", True).\
    csv(f"hdfs://10.10.1.1:9000/output/{app_name}")

spark.stop()
