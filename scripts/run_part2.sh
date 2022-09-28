#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

echo "${BLUE} Running sorting app ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 ../part2/sort.py --input hdfs://10.10.1.1:9000/part2/export.csv --output hdfs://10.10.1.1:9000/output/sorted.csv