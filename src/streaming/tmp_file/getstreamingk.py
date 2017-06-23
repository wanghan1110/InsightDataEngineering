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
    print(lp)
    coord = lp[1].encode("utf8").split(",")
    vec = Vectors.dense([float(coord[0]), float(coord[1])])
    print("vec type: {}, val: {}".format(type(vec), vec))
    return LabeledPoint(lp[0], vec)


if __name__ == '__main__':
    sc = SparkContext(appName="Streaming-KMeans")
    sc.setLogLevel("WARN")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
    
    # every 5 seconds
    ssc = StreamingContext(sc, 5)

    # define topic and brokers
    topic = 'drone_data_new'
    brokers = 'ec2-34-211-247-230.us-west-2.compute.amazonaws.com:9092'
    kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # get train data (rdd obj)
    # dataLink = 's3a://cellulartest/train-data.csv'
    rawDataRDD = sc.textFile\
    ('hdfs://ec2-34-211-247-230.us-west-2.compute.amazonaws.com:9000/streamscript/data/train_data_5.txt')
    trainingData = rawDataRDD\
    .map(lambda line:Vectors.dense([float(x) for x in line.strip().split(',')]))
    trainingStream = ssc.queueStream([trainingData])
    
    # create a model with (random for now )cluter center  
    # specify the number of clusters to find, k = 5
    model = StreamingKMeans(k = 5, decayFactor = 1.0).\
    setRandomCenters(2, 1.0, 123)


    # register the streams for training and testing
    model.trainOn(trainingStream)

    # get test stream
    test_stream = kafkaStream.map(parse)

    result = model.predictOnValues(test_stream.map(lambda lp: (lp.label, lp.features)))

    # print("Final centers: " + str(model.latestModel().centers))
    # model.latestModel().centers.foreach(print)

    # print predicted cluster assignments on new data points
    result.pprint()

    ssc.start()
    # stop after 3 minutes
    ssc.awaitTermination(timeout=180)
    # print("Final centers: " + str(model.latestModel().centers))