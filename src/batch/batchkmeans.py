"""batch process K-Means centers"""
from __future__ import print_function
import time
import uuid
from math import sqrt
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.linalg import Vectors
from pyspark.ml.feature import VectorAssembler, StandardScaler

def get_dataframe(rdd_obj):
    """onvert RDD obj to DataFrame"""
    rdd = rdd_obj.map(lambda x: x.split(','))
    header = rdd.first()
    df = rdd.filter(lambda row: row != header).toDF(header)
    return df

def convert_num(df):
    """convert df column to numbers"""
    df_num = df.select(df.lon.cast("float"),\
        df.lat.cast("float"),\
        df.time_hour.cast("integer"))
    return df_num

def get_GMT(epoch):
    """convert epoch time to GMT-time. return GMT hour"""
    hour = time.gmtime(epoch).tm_hour
    return float(hour)

def assign_ID():
    """generate unique ID. return string format"""
    return str(uuid.uuid4())

def write_cassandra(df, table_name, keyspace_name):
    """write to cassandra"""
    df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(table=table_name, keyspace=keyspace_name)\
    .save()

def get_RDD(df, feat_cols):
    """convert dataframe to rdd"""
    assembler = VectorAssembler(inputCols=feat_cols, outputCol="features")
    df1 = assembler.transform(df).select('features')
    df3 = df1.rdd.map(lambda row: Vectors.dense([x for x in row['features']]))
    return df3

def get_schema():
    """create schema for empty dataframe"""
    field = [StructField("hour", FloatType(), True),\
    StructField('center_id', IntegerType(), True),\
    StructField('lon', FloatType(), True),\
    StructField('lat', FloatType(), True)]
    schema = StructType(field)
    return schema

if __name__ == '__main__':

    # set spark context
    sc = SparkContext(appName="K-Means-Batch-Process")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    # read data from s3
    out_link = 's3a://cellulartest/output.csv'
    out_RDD = sc.textFile(out_link)

    # convert RDD objects to dataframe
    outDF = get_dataframe(out_RDD)

    # inner join two tables
    out_num = convert_num(outDF)

    # define get_GMT as UDF
    udf_gmt = udf(get_GMT, FloatType())
    out_num = out_num.withColumn("hour", udf_gmt(out_num['time_hour']))

    # define assign_ID as UDF
    id_gmt = udf(assign_ID, StringType())
    out_num = out_num.withColumn("id", id_gmt())

    kmeans_list = []
    # feature columns that are relevant to k-means clustering
    feat_cols = ['lon', 'lat']
    for i in range(0, 24):
        out_num_byhour = out_num.where(col('hour').isin([i+0.0]))
        if out_num_byhour.count() != 0:
            # convert dataframe to RDD
            out_num_rdd_byhour = get_RDD(out_num_byhour, feat_cols)
            # k-means clustering, k = 5
            model = KMeans.train(out_num_rdd_byhour, 5)
            center_id = 0
            for x in model.clusterCenters:
                center_id += 1
                row = [i+0.0, center_id, float(x[0]), float(x[1])]
                kmeans_list.append(row)
    kmeans_list = sc.parallelize(kmeans_list)
    kmeans_data = kmeans_list.map(lambda x: (x[0], x[1], x[2], x[3]))
    schema = get_schema()
    kmeans_df = sqlContext.createDataFrame(kmeans_data, schema)
    write_cassandra(kmeans_df, "kmeans_batch", "batchdata")

    sc.stop()

# running command:
# spark-submit --packages datastax:spark-cassandra-connector:2.0.1-s_2.11
# --master spark://ip-10-0-0-11:7077 batchkmeans.py
