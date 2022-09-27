#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

# Test: Number of partitions while writing
echo "${BLUE} Running Job for Partitions = 200, Out Parititons = 1, Persistence Mode = Memory Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --out_partitions 1 --persist Memory_Only
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200, Out Parititons = 50, Persistence Mode = Memory Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --out_partitions 50 --persist Memory_Only
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200, Out Parititons = 100, Persistence Mode = Memory Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --out_partitions 100 --persist Memory_Only
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200, Out Parititons = 150, Persistence Mode = Memory Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --out_partitions 150 --persist Memory_Only
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*