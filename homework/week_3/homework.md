## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696 ✅
- 22,978,333
- 13,942,414

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table ✅


## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748 ✅
- 1,215,687
- 5
- 20,332

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number ✅
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table ✅
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket ✅
- Container Registry
- Big Table


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False ✅


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 

<details open>
<summary>SQL</summary>
```
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
```
</details>