#!/bin/sh
export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

echo "${BLUE} Removing output directory ${NOCOLOR}"
/users/hrpatel5/hadoop-3.3.4/bin/hdfs dfs -rm -r -f /output/*
echo "${BLUE} Completed ${NOCOLOR}"