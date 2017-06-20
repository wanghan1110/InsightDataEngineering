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

def write_redis():
    # redis connection
    redis_server = "localhost"
    redis_db = redis.StrictRedis(redis_server, port=6379, db=0)
    redis_db.set('Test',100)

if __name__ == '__main__':
    # writing to Redis
    write_redis()