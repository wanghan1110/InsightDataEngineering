peg fetch spark-cluster
peg start spark-cluster
peg service spark-cluster hadoop start
peg service spark-cluster spark start
peg service spark-cluster cassandra start

peg ssh spark-cluster 1
$SPARK_HOME/bin/pyspark --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 --master spark://ip-10-0-0-11:7077