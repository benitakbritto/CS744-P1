#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

# Test: Number of partitions
echo "${BLUE} Running Job for Partitions = 50 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 50
echo "${BLUE} Job for Partitions = 50 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 100 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 100
echo "${BLUE} Job for Partitions = 100 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 150 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 150
echo "${BLUE} Job for Partitions = 150 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200
echo "${BLUE} Job for Partitions = 200 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 250 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 250
echo "${BLUE} Job for Partitions = 250 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 300 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 300
echo "${BLUE} Job for Partitions = 300 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 350 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 350
echo "${BLUE} Job for Partitions = 350 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 400 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 400
echo "${BLUE} Job for Partitions = 400 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 450 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 450
echo "${BLUE} Job for Partitions = 450 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 500 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 500
echo "${BLUE} Job for Partitions = 500 finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*