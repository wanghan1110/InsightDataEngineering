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

joinDF_num.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('overwrite')\
    .options(table="kmeans_source", keyspace="batchdata")\
    .save()



# cassandra version: 3.10(now: 3.0.13)
# spark version: 2.1.1 
# connector version: 1.6.6-s_2.10 (now 2.0.1-s_2.11)