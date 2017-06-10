//Create keyspace
CREATE KEYSPACE batchdata WITH replication = {'class': 'SimpleStrategy','replication_factor':3};

USE batchdata;

//Create Table
DROP TABLE kmeans_source
CREATE TABLE kmeans_source (users int, lon float, lat float, time_hour int, hour float, id text PRIMARY KEY);