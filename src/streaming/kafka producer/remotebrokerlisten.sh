# monitor the session from individual node
# node 1
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data_demo --partition 0
# node 2
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data --partition 1
# node 3
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data --partition 2
# node 4
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data --partition 3

# check if the topic exists from any node
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# describe the topic and see which node takes care of which partitio
/usr/local/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181
