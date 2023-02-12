-- create external table
CREATE OR REPLACE EXTERNAL TABLE `dte-de-375705.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://gcs-bq-nytaxi/fhv/fhv_tripdata_2019-*.parquet']
);

DROP table IF EXISTS `dte-de-375705.nytaxi.fhv_tripdata`;

select * from `dte-de-375705.nytaxi.fhv_tripdata` limit 1;

-- create non partition table
CREATE OR REPLACE TABLE `dte-de-375705.nytaxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `dte-de-375705.nytaxi.fhv_tripdata`;

-- create partition and cluster table 
create or replace table `dte-de-375705.nytaxi.fhv_tripdata_partition_cluster`
partition by date(pickup_datetime)
cluster by Affiliated_base_number as 
select * from `dte-de-375705.nytaxi.fhv_tripdata`

-- q1
select count(1) from `dte-de-375705.nytaxi.fhv_tripdata`;

-- q2 
-- non partitioned table 317.94 MB
select distinct count(Affiliated_base_number) from `dte-de-375705.nytaxi.fhv_nonpartitioned_tripdata`;
-- external table
select distinct count(Affiliated_base_number) from `dte-de-375705.nytaxi.fhv_tripdata`;

-- q3
select COUNT(*) 
from `dte-de-375705.nytaxi.fhv_tripdata` 
where PUlocationID is NULL and DOlocationID is NULL

-- q5
-- non partitioned table 647.87 MB
select distinct(affiliated_base_number)
from `dte-de-375705.nytaxi.fhv_nonpartitioned_tripdata`
where pickup_datetime between '2019-03-01' and '2019-03-31'
-- partition table 23.05 MB
select distinct(affiliated_base_number)
from `dte-de-375705.nytaxi.fhv_tripdata_partition_cluster`
where pickup_datetime between '2019-03-01' and '2019-03-31'