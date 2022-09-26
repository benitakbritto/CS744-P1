export HDFS_NAMENODE_USER=root;
export HDFS_DATANODE_USER=root;
export HDFS_SECONDARYNAMENODE_USER=root;

/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py;

echo "Run 2"

/users/hrpatel5/spark-3.3.0-bin-hadoop3/bin/spark-submit --master spark://10.10.1.1:7077 pagerank.py;