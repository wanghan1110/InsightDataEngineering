"""calculate cluster centers based on streaming data"""
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.clustering import StreamingKMeansModel
from pyspark.sql import SparkSession, SQLContext
from pyspark.mllib.linalg import Vectors
import redis

redis_server = "localhost"
redis_db = redis.StrictRedis(redis_server, port=6379, db=0)

def get_center(rdd):
    """get cluster centers"""
    for i in range(len(stkm.centers)):
        key = "key-" + str(i)
        center = str(stkm.centers[i])
        center = center.replace('[', '')
        center = center.replace(']', '')
        center = center.split()
        write_redis(key, center)
    collected_rdd = rdd.collect()
    cnt = 1
    for i in collected_rdd:
        if cnt <= 120:
            write_redis(cnt, i)
            cnt += 1
        else:
            cnt = 1
            write_redis(cnt, i)
    return rdd

def update_center(rdd):
    """update cluster centers"""
    stkm.update(get_center(rdd), decay_factor, u"batches")

def parse(lp):
    """parse coordinates from streaming data"""
    coord = lp[1].encode("utf8").split(",")
    vec = Vectors.dense([float(coord[0]), float(coord[1])])
    return vec

def write_redis(k, val):
    """write into redis"""
    redis_db.set(k, val)

if __name__ == '__main__':
    sc = SparkContext(appName="Streaming-KMeans")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    ssc = StreamingContext(sc, 2)

    topic = 'drone_data_demo'
    brokers = 'ec2-34-211-247-230.us-west-2.compute.amazonaws.com:9092'

    kafka_stream = KafkaUtils.\
    createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    init_centers = [[111.08575106060509, 13.134825358711282],\
    [111.08120771714472, 13.187724105098596],\
    [111.17965587636363, 13.1889099103317],\
    [111.07777050101427, 13.071335633938101],\
    [111.02132026192659, 13.163110522505603]]

    init_weights = [1.0, 1.0, 1.0, 1.0, 1.0]

    stkm = StreamingKMeansModel(init_centers, init_weights)

    decay_factor = 0.0

    test_stream = kafka_stream.map(parse)

    test_stream.foreachRDD(update_center)

    ssc.start()
    ssc.awaitTermination()

    """running command:
    spark-submit --packages org.apache.spark:
    spark-streaming-kafka-0-8_2.11:2.0.2
    --master spark://ip-10-0-0-10:7077 ~/streamscript/try_redis.py"""
