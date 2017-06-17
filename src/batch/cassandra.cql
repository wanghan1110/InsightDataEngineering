//Create keyspace
CREATE KEYSPACE batchdata WITH replication = {'class': 'SimpleStrategy','replication_factor':3};

USE batchdata;

//Create Table kmeans_batch
DROP TABLE kmeans_batch
CREATE TABLE kmeans_batch (hour float, center_id int, lon float, lat float, PRIMARY KEY (hour,center_id));