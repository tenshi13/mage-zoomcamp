## Big Query sqls used in zoomcamp homework 3
### Questions with 3 dash('-'), answers in 2 dash

### Setup
```sql
--- Data was preloaded to gcs bucket - ethanhobl_zoomcamp_week3
--- Create an external table using the Green Taxi Trip Records Data for 2022.
CREATE OR REPLACE EXTERNAL TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata`
OPTIONS (
  format ="PARQUET",
  uris = ['gs://ethanhobl_zoomcamp_week3/green_tripdata_2022-*.parquet']
);

--- created another external table to fix lpep_pickup_datetime, lpep_dropoff_datetime int64 to datetime data type issue
CREATE OR REPLACE TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
AS 
SELECT
  *,
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime
FROM `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata`;

ALTER TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
DROP COLUMN lpep_pickup_datetime;
ALTER TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
DROP COLUMN lpep_dropoff_datetime;

ALTER TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
RENAME COLUMN cleaned_pickup_datetime to lpep_pickup_datetime;

ALTER TABLE `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
RENAME COLUMN cleaned_dropoff_datetime to lpep_dropoff_datetime;

--- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE `vaulted-bit-411213.ny_taxi.nonpartitioned_green_2022_tripdata`
AS SELECT * FROM `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`;
```

### Question 1
```sql
--- What is count of records for the 2022 Green Taxi Data??
SELECT count(*) FROM `vaulted-bit-411213.ny_taxi.nonpartitioned_green_2022_tripdata`
-- 840402
```

### Question 2
```sql
--- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
--- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT distinct(pu_location_id) 
FROM `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
-- This query will process 0 B when run.

SELECT distinct(pu_location_id) 
FROM `vaulted-bit-411213.ny_taxi.nonpartitioned_green_2022_tripdata`
-- This query will process 6.41 MB when run
-- 0 MB for the External Table and 6.41MB for the Materialized Table
```

### Question 3
```sql
--- How many records have a fare_amount of 0?
select count(*) from
`vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`
where fare_amount = 0
-- 1622
```

### Question 4
```sql
--- What is the best strategy to make an optimized table in Big Query if your query will always 
--- order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE
OR REPLACE TABLE `vaulted-bit-411213.ny_taxi.partitioned_green_2022_tripdata` 
PARTITION BY DATE(cleaned_pickup_datetime) CLUSTER BY pu_location_id AS
SELECT
  *
FROM
  `vaulted-bit-411213.ny_taxi.external_green_2022_tripdata2`;
```

### Question 5
```sql
--- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
--- Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from 
--- clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?
--- Choose the answer which most closely matches.
select
  distinct(pu_location_id)
from
  `vaulted-bit-411213.ny_taxi.nonpartitioned_green_2022_tripdata` 
where
  lpep_pickup_datetime >= "2022-06-01"
  and lpep_pickup_datetime <= '2022-06-30'
-- This query will process 12.82 MB when run

-- partitioned
select
  distinct(pu_location_id)
from
  `vaulted-bit-411213.ny_taxi.partitioned_green_2022_tripdata` 
where
  lpep_pickup_datetime >= "2022-06-01"
  and lpep_pickup_datetime <= '2022-06-30'
-- This query will process 1.12 MB when run.
-- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
```

### Question 6
```sql
---  Where is the data stored in the External Table you created?
-- GCP Bucket
```

### Question 7
```sql
---  It is best practice in Big Query to always cluster your data:
-- True, and ideally couple with partitioning
```

### Question 8
```sql
--- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT count(*)
from `vaulted-bit-411213.ny_taxi.partitioned_green_2022_tripdata` 
-- This query will process 0 B when run
-- partitioned tables store meta data, and bigquery will scan using the meta data that's why there is not cost
```