import time
import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import udf

# convert RDD obj to DataFrame
def getDataFrame(rddObj):
    rdd = rddObj.map(lambda x : x.split(','))
    header = rdd.first()
    df = rdd.filter(lambda row : row != header).toDF(header)
    return df

# join dataframes
def joinTable(df1,df2):
    joinDF = df1.join(df2,df1.bs==df2.bs,'inner').\
    select(df1.bs,df1.users,df2.lon,df2.lat,df1.time_hour)
    return joinDF

# convert df column to numbers
def convertNum(df):
    df_num = df.select(df.users.cast("integer"),\
        df.lon.cast("float"),df.lat.cast("float"),\
        df.time_hour.cast("integer"))
    return df_num

# convert epoch time to GMT-time. return GMT hour
def getGMT(epoch):
    hour = time.gmtime(epoch).tm_hour
    return float(hour)

# generate unique ID. return string format
def assignID():
    return str(uuid.uuid4())

if __name__ == '__main__':
    cellLink = 's3a://cellulartest/cellular_traffic.csv'
    topoLink = 's3a://cellulartest/topology.csv'
    cellRDD = sc.textFile(cellLink)
    topoRDD = sc.textFile(topoLink)

    cellDF = getDataFrame(cellRDD)
    topoDF = getDataFrame(topoRDD)

    joinDF = joinTable(cellDF,topoDF)
    joinDF_num = convertNum(joinDF)

    # define getGMT as UDF
    udf_gmt = udf(getGMT,FloatType())
    joinDF_num = joinDF_num.withColumn("hour",udf_gmt(joinDF_num['time_hour']))

    # define assignID as UDF
    id_gmt = udf(assignID,StringType())
    joinDF_num = joinDF_num.withColumn("id",id_gmt())

# cassandra version: (old: 3.10) 3.0.13
# spark version: 2.1.1 
# connector version: (old: 1.6.6-s_2.10) 2.0.1-s_2.11

