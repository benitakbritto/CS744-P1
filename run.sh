#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

echo "${BLUE} Running script run.sh ${NOCOLOR}"

# Part2: Sorting
echo "${BLUE} Running Part2: Sorting ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 part2/sort.py --input hdfs://10.10.1.1:9000/part1/export.csv --output hdfs://10.10.1.1:9000/part1/sorted.csv
echo "${BLUE} Completed Part2: Sorting ${NOCOLOR}" 
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*
echo "${BLUE} Completed removing output directory ${NOCOLOR}"

# Part3: PageRank on Small File
echo "${BLUE} Running Part3: PageRank on Small File ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 part3/pagerank_small.py
echo "${BLUE} Completed Part3: PageRank on Small File ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*
echo "${BLUE} Completed removing output directory ${NOCOLOR}"

# Part3: PageRank on Large File
echo "${BLUE} Part3: PageRank on Large File ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 part3/pagerank.py --partitions 200 --persist "Memory_Only" 
echo "${BLUE} Part3: PageRank on Large File ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*
echo "${BLUE} Completed removing output directory ${NOCOLOR}"

echo "${BLUE} Completed script run.sh ${NOCOLOR}"