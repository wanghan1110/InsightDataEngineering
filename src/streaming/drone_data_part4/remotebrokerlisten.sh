# node 1
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data_part4 --partition 0
# node 2
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data_part4 --partition 1
# node 3
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data_part4 --partition 2
# node 4
/usr/local/kafka/bin/kafka-simple-consumer-shell.sh --broker-list localhost:9092 --topic drone_data_part4 --partition 3