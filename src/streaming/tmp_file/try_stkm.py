from __future__ import print_function
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.mllib.clustering import StreamingKMeansModel
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.mllib.linalg import Vectors, DenseVector
import redis

def sendPartition(iter):
    redis_table = redis.StrictRedis('localhost', port=6379, db=0)
    for record in iter:
        redis_table.set(record, record)

def logRDD(rdd):
    print(rdd.count())
    return rdd

def get_center(rdd):
    print(stkm.centers)
    return rdd

def update_center(x):
    stkm.update(get_center(x),decayFactor, u"batches")

def parse(lp):
    coord = lp[1].encode("utf8").split(",")
    vec = Vectors.dense([float(coord[0]), float(coord[1])])
    return vec

if __name__ == '__main__':
    sc = SparkContext(appName="Streaming-KMeans")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
    # define batch interval of 5s
    ssc = StreamingContext(sc, 5)

    # define topic and brokers
    topic = 'drone_data_new'
    brokers = 'ec2-34-211-247-230.us-west-2.compute.amazonaws.com:9092'
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    
    # get initial centers from batch K-Means
    initCenters = [[111.08575106060509, 13.134825358711282],\
    [111.08120771714472, 13.187724105098596],\
    [111.17965587636363, 13.1889099103317],\
    [111.07777050101427, 13.071335633938101],\
    [111.02132026192659, 13.163110522505603]]
    
    # define initial weights of clusters
    initWeights = [1.0,1.0,1.0,1.0,1.0]

    # create a streaming K-Means model with initial center 
    stkm = StreamingKMeansModel(initCenters, initWeights)

    # decayFactor = 0.0: only the most recent data will be used
    decayFactor = 0.0

    # get test stream
    test_stream = kafkaStream.map(parse)

    # update center
    test_stream.foreachRDD(update_center)

    ssc.start()

    # stop after 3 minutes
    ssc.awaitTermination(timeout=180)

    # running command: 
 #    spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --master spark://ip-10-0-0-10:7077 ~/streamscript/try_stkm.py

