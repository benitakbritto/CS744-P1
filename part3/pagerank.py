from pyspark.sql import SparkSession
from pyspark import StorageLevel
from multiprocessing import Process
from operator import add
import argparse
import psutil
import csv
import os


parser = argparse.ArgumentParser()
parser.add_argument(
    "--iterations", 
    type=int, 
    default=10, 
    help="The number of iterations to run PageRank algorithm."
)
parser.add_argument(
    "--partitions", 
    type=int, 
    default=None, 
    help="The number of partitions for the PageRank shuffling."
)
parser.add_argument(
    "--out_partitions",
    type=int, 
    default=None, 
    help="The number of partitions for the PageRank shuffling."
)
parser.add_argument(
    "--persist", 
    type=str, 
    choices=["Memory_Only", "Disk_Only", "Memory_And_Disk"], 
    help="The persistence mode for PageRank algorithm."\
    " Options are: Memory_Only, Disk_Only, Memory_And_Disk"
)

args = parser.parse_args()

APP_NAME = f"PageRank-WikiDataset-{args.iterations}-{args.partitions}-{args.out_partitions}-{args.persist}"

stop_monitoring = False

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

def monitor(pid):
    global stop_monitoring

    process = psutil.Process(pid)

    print("Starting Monitoring Process for Spark Job")

    with open(f"/mnt/data/{APP_NAME}.csv" , 'w') as f:
        writer = csv.writer(f)
        title = ["CPU Util %", "Mem Usage"]
        writer.writerow(title)

        while True:
            if stop_monitoring:
                break

            data = [process.cpu_percent(), process.virtual_memory().used]
            writer.writerow(data)

            print("Logging data to file...")

            time.sleep(30)

def run_spark():
    # Create Spark context
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    # Load RDD into Spark
    rdd = spark.\
        sparkContext.\
        textFile("hdfs://10.10.1.1:9000/part3/enwiki-pages-articles/link-enwiki-*")
        
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
        print(f"Partitioning neighbours RDD into {args.partitions} partitions")
        neighbours = neighbours.partitionBy(args.partitions)

    print(f"Number of partitions in neighbours RDD: {neighbours.getNumPartitions()}")

    # If the flag is set, persist the neighbour RDD into memory or disk.
    if args.persist:
        print(f"Persisting neighbours RDD with {args.persist} mode")
        if args.persist == "Disk_Only":
            neighbours = neighbours.cache(storageLevel=StorageLevel.DISK_ONLY)
        elif args.persist == "Memory_Only":
            neighbours = neighbours.persist(storageLevel=StorageLevel.MEMORY_ONLY)
        else:
            # Default to memory and disk
            neighbours = neighbours.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

    # Create the rank RDD -> (key, rank)
    ranks = neighbours.mapValues(lambda x: 1.0)

    # Execute 'num_iterations' iterations of the PageRank algorithm
    for _ in range(args.iterations):
        # Join the ranks, neighbours 
        # RDD schema: (sourceNode, (rank, list[destinationNodes]))
        # Note: Constrainning the output number of joins slights increases performace
        joinedRDD = neighbours.join(ranks, neighbours.getNumPartitions())
        
        # Project the contributions that a node will make towards destination nodes
        # RDD schema: (destinationNode, contributionFromSource)
        flatMapRDD = joinedRDD.flatMap(lambda row: [(node, row[1][1]/len(row[1][0])) for node in row[1][0]])

        # Summing up all the contributions towards each node
        # RDD schema: (destinationNode, sumOfContributions)
        contributionsRDD = flatMapRDD.reduceByKey(add)

        # Weight the contributions towards each node and assign new ranks
        # RDD schema: (destinationNode, newRank)
        ranks = contributionsRDD.mapValues(lambda x: 0.15 + 0.85*x)


    # If output partitions are configured, partition again
    if args.out_partitions:
        ranks = ranks.coalesce(args.out_partitions)

    print(f"Number of partitions being written to disk: {ranks.getNumPartitions()}")

    # Write back RDD to HDFS
    ranks.\
        toDF(["Node", "Rank"]).\
        write.\
        mode("overwrite").\
        option("header", True).\
        csv(f"hdfs://10.10.1.1:9000/output/{APP_NAME}")
    
    spark.stop()

if(__name__) == '__main__':
    # Start thread to record CPU and Mem usage
    monitoring_process = Process(target=monitor, args=(os.getpid(),))
    monitoring_process.start()

    # Actually invoke the Spark computation
    run_spark()

    # Stop the monitoring thread
    stop_monitoring = True
    monitoring_process.join()


