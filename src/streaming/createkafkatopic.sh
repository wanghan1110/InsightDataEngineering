peg ssh spark-cluster 1

# create Kafka topic
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic drone_data_ --partitions 4 --replication-factor 2

# check if the topic exist
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# describe the topic
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181