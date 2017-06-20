from __future__ import print_function
import time
import uuid
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.mllib.linalg import Vectors
from pyspark.sql import SQLContext
import numpy as np
from math import sqrt

# convert RDD obj to DataFrame
def getDataFrame(rddObj):
    rdd = rddObj.map(lambda x : x.split(','))
    header = rdd.first()
    df = rdd.filter(lambda row : row != header).toDF(header)
    return df

# convert df column to numbers
def convertNum(df):
    df_num = df.select(df.lon.cast("float"),\
        df.lat.cast("float")\
        ,df.time_hour.cast("integer"))
    return df_num

# convert epoch time to GMT-time. return GMT hour
def getGMT(epoch):
    hour = time.gmtime(epoch).tm_hour
    return float(hour)

# generate unique ID. return string format
def assignID():
    return str(uuid.uuid4())

# write to cassandra
def write_cassandra(df,table_name,keyspace_name):
    df.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(table=table_name, keyspace=keyspace_name)\
    .save()

# convert dataframe to rdd
def getRDD(df,feat_cols):
    # create 'feature' vector
    assembler = VectorAssembler(inputCols = feat_cols, outputCol = "features")
    df1 = assembler.transform(df).select('features')
    # convert df to a dense vector RDD
    df3 = df1.rdd.map(lambda row: Vectors.dense([x for x in row['features']]))
    return df3

# get schema
def getSchema():
    field = [StructField("hour", FloatType(), True),\
    StructField('center_id', IntegerType(), True),\
    StructField('lon',FloatType(),True),\
    StructField('lat',FloatType(),True)]
    schema = StructType(field)
    return schema

if __name__ == '__main__':

    # set spark context
    sc = SparkContext(appName = "K-Means-Batch-Process")
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    # read data from s3
    outLink = 's3a://cellulartest/output.csv'
    outRDD = sc.textFile(outLink)

    # convert RDD objects to dataframe
    outDF = getDataFrame(outRDD)

    # inner join two tables
    out_num = convertNum(outDF)

    # define getGMT as UDF
    udf_gmt = udf(getGMT,FloatType())
    out_num = out_num.withColumn("hour",udf_gmt(out_num['time_hour']))

    # define assignID as UDF
    id_gmt = udf(assignID,StringType())
    out_num = out_num.withColumn("id",id_gmt())

    kmeans_list = []
    # feature columns that are relevant to k-means clustering
    feat_cols = ['lon','lat']
    for i in range(0,24):
        out_num_byhour = out_num.where(col('hour').isin([i+0.0]))
        if out_num_byhour.count() != 0:
            # convert dataframe to RDD
            out_num_rdd_byhour = getRDD(out_num_byhour,feat_cols)
            # k-means clustering, k = 5
            model = KMeans.train(out_num_rdd_byhour,5)
            center_id = 0
            for x in model.clusterCenters: 
                center_id += 1
                row = [i+0.0,center_id,float(x[0]),float(x[1])]
                kmeans_list.append(row)
    kmeans_list = sc.parallelize(kmeans_list)
    kmeans_data = kmeans_list.map(lambda x:(x[0],x[1],x[2],x[3]))
    schema = getSchema()
    kmeans_df = sqlContext.createDataFrame(kmeans_data,schema)
    write_cassandra(kmeans_df,"kmeans_batch","batchdata")

    sc.stop()

# running command:
# spark-submit --packages datastax:spark-cassandra-connector:2.0.1-s_2.11 --master spark://ip-10-0-0-11:7077 batchkmeans.py
