export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

# Test: Number of partitions
echo "${BLUE} Running Job for Partitions = 50 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 50

echo "${BLUE} Running Job for Partitions = 100 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 100

echo "${BLUE} Running Job for Partitions = 150 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 150

echo "${BLUE} Running Job for Partitions = 200 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 200

echo "${BLUE} Running Job for Partitions = 250 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 250

echo "${BLUE} Running Job for Partitions = 300 ${NOCOLOR}"
/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py --partitions 300