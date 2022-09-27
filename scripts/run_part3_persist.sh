#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

# Test: Persistence Mode
echo "${BLUE} Running Job for Partitions = 200, Persistence Mode = Memory Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --persist Memory_Only
echo "${BLUE} Job for Partitions = 200,Persistence Mode = Memory Only finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200, Persistence Mode = Disk Only ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --persist Disk_Only
echo "${BLUE} Job for Partitions = 200,Persistence Mode = Disk Only finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*

echo "${BLUE} Running Job for Partitions = 200, Persistence Mode = Memory_And_Disk ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part3/pagerank.py --partitions 200 --persist Memory_And_Disk
echo "${BLUE} Job for Partitions = 200,Persistence Mode = Memory_And_Disk finished ${NOCOLOR}"
echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*
