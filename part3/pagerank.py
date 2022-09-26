from pyspark.sql import SparkSession
from pyspark import StorageLevel
from operator import add
import argparse

# Default parameters set
APP_NAME = "PageRank-WikiDataset"


parser = argparse.ArgumentParser()

parser.add_argument("--iterations", type=int, default=10, help="The number of iterations to run PageRank algorithm.")
parser.add_argument("--partitions", type=int, default=None, help="The number of partitions for the PageRank shuffling.")
parser.add_argument("--persist", type=str, choices=["Memory_Only", "Disk_Only", "Memory_And_Disk"], 
       help="The persistence mode for PageRank algorithm."\
       " Options are: Memory_Only, Disk_Only, Memory_And_Disk")

args = parser.parse_args()

APP_NAME = f"PageRank-WikiDataset-{args.iterations}-{args.partitions}-{args.persist}"

# Create Spark context
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load RDD into Spark
rdd = spark.\
    sparkContext.\
    textFile("hdfs://10.10.1.1:9000/part2/datasets/enwiki-pages-articles/link-enwiki-*")


def get_statistics(input_rdd):
    """
    Returns the statistics about each partition in 'input_rdd'.
    """
    partitions = input_rdd.glom().map(len).collect() 
    print(f"Characteristics about data partitioned by the default hash partitioner of Spark:")
    print(f"Number of partitions: {len(partitions)}")
    print(f"Maximum number of elements in one partition: {max(partitions)}")
    print(f"Minimum number of elements in one partition: {min(partitions)}")
    print(f"Mean number of elements in one partition: {sum(partitions)/len(partitions)}")
    print(f"Median number of elements in one partition: {sorted(partitions)[len(partitions)//2]}")

# Cleaning up the data
# As per README, ignoring all 'User:', 'Image:' except Category
# Then, converting the lines to lower case
rdd = rdd.\
    filter(lambda line: ':' not in line or 'Category' in line).\
    map(lambda line: line.lower())

# Splitting tab separated lines into two columns
rdd = rdd.map(lambda line: line.split('\t', 1))

# Create neighbours RDD -> (key, list[neighbours])
neighbours = rdd.groupByKey().mapValues(list)

# If the flag is set, partition into user provided number.
if args.partitions:
    neighbours = neighbours.partitionBy(args.partitions)

print(f"Number of partitions in neighbours RDD: {neighbours.getNumPartitions()}")

# If the flag is set, persist the neighbour RDD into memory or disk.
if args.persist:
    if args.persist == "Disk_Only":
        neighbours = neighbours.persist(StorageLevel.DISK_ONLY)
    elif args.persist == "Memory_Only":
        neighbours = neighbours.persist(StorageLevel.MEMORY_ONLY)
    else:
        # Default to memory and disk
        neighbours = neighbours.persist(StorageLevel.MEMORY_AND_DISK)

# Create the rank RDD -> (key, rank)
ranks = neighbours.mapValues(lambda x: 1.0)

get_statistics(neighbours)

get_statistics(ranks)

# # Execute 'num_iterations' iterations of the PageRank algorithm
# for _ in range(NUM_ITERATIONS):
#     # Join the ranks, neighbours 
#     # RDD schema: (sourceNode, (rank, list[destinationNodes]))
#     # Note: Constrainning the output number of joins slights increases performace
#     joinedRDD = neighbours.join(ranks, neighbours.getNumPartitions())
    
#     # Project the contributions that a node will make towards destination nodes
#     # RDD schema: (destinationNode, contributionFromSource)
#     flatMapRDD = joinedRDD.flatMap(lambda row: [(node, row[1][1]/len(row[1][0])) for node in row[1][0]])

#     # Summing up all the contributions towards each node
#     # RDD schema: (destinationNode, sumOfContributions)
#     contributionsRDD = flatMapRDD.reduceByKey(add)

#     # Weight the contributions towards each node and assign new ranks
#     # RDD schema: (destinationNode, newRank)
#     ranks = contributionsRDD.map(lambda row: (row[0], 0.15 + 0.85*row[1]))

# # Write back RDD to HDFS
# ranks.\
#     toDF(["Node", "Rank"]).\
#     write.\
#     mode("overwrite").\
#     option("header", True).\
#     csv(f"hdfs://10.10.1.1:9000/output/{APP_NAME}")

spark.stop()
