import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# import redis

def saveCoord(rdd):
    filedir = 'hdfs://ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9000/streamscript/data/train_data_8'
    # rdd.foreach(lambda rec: open(filedir, "w+").write("{"+rec.split(" ")[0]+":"+rec.split(" ")[1]+","+rec.split(" ")[2]+":"+rec.split(" ")[3]+"},\n"))
    rdd.saveAsTextFile(filedir)

if __name__ == '__main__':
    sc = SparkContext(appName="Streaming-K-Means")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 5)
    topic = 'drone_data_part4'
    brokers = 'ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9092'
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    
    lines = kafkaStream.map(lambda x: x[1])
    coords = lines.map(lambda line: line)
    coords.pprint()
    coords.foreachRDD(saveCoord)

    # parsed = kafkaStream.map(lambda v: 1)
    # parsed.count().map(lambda x:'Record in this batch: %s' % x).pprint()


    # POOL = redis.ConnectionPool(host='10.0.0.1', port=6379, db=0)
    ssc.start()
    # stop after 3 minutes
    ssc.awaitTermination(timeout=180)

