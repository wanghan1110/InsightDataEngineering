import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.mllib.linalg import Vectors
# import redis

def parse(lp):
    label = float(lp[lp.find('(') + 1: lp.find(')')])
    vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
    return LabeledPoint(label, vec)


if __name__ == '__main__':
    sc = SparkContext(appName="Streaming-KMeans")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
    # every 5 seconds
    ssc = StreamingContext(sc, 5)

    # define topic and brokers
    topic = 'drone_data_part4'
    brokers = 'ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9092'

    # get train data (rdd obj)
    # dataLink = 's3a://cellulartest/train-data.csv'
    rawDataRDD = sc.textFile\
    ('hdfs://ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9000/streamscript/data/train_data.txt')
    trainingData = rawDataRDD\
    .map(lambda line:Vectors.dense([float(x) for x in line.strip().split(',')]))
    trainingStream = ssc.queueStream([trainingData])

    # get test data (rdd obj)
    # kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # testingStream = ssc.queueStream(kafkaStream)
    testData = sc.textFile\
    ('hdfs://ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9000/streamscript/data/test_data.txt')\
    .map(parse)
    testingStream = ssc.queueStream([testData])
    
    # create a model with (random for now )cluter center  
    # specify the number of clusters to find, k = 5
    model = StreamingKMeans(k = 2, decayFactor = 1.0).\
    setRandomCenters(2, 1.0, 0)


    # register the streams for training and testing
    model.trainOn(trainingStream)
    result = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
    print("Final centers: " + str(model.latestModel().centers))

    # print predicted cluster assignments on new data points
    result.pprint()
        
    # Redis connection
    # POOL = redis.ConnectionPool(host='10.0.0.1', port=6379, db=0)

    # print(model.clusterCenters)
    
    
    # topic = 'drone_data_part4'
    # brokers = 'ec2-52-10-138-212.us-west-2.compute.amazonaws.com:9092'
    # kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # parsed = kafkaStream.map(lambda v: 1)
    # parsed.count().map(lambda x:'Record in this batch: %s' % x).pprint()

    ssc.start()
    # stop after 3 minutes
    ssc.awaitTermination(timeout=180)
    # print("Final centers: " + str(model.latestModel().centers))