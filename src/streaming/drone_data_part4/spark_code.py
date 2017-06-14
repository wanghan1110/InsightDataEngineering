import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka_RM_01")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)
topic = 'drone_data_part4'
brokers = 'ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9092'
kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
parsed = kafkaStream.map(lambda v: 1)
parsed.count().map(lambda x:'Record in this batch: %s' % x).pprint()


ssc.start()
# stop after 3 minutes
ssc.awaitTermination(timeout=180)

