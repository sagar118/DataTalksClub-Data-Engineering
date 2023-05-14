-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `teak-alloy-385319.dw_hw_data.fhv_external_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_teak-alloy-385319/dw_hw/fhv_tripdata_2019-*.csv.gz']
);

-- Create Table
CREATE OR REPLACE TABLE `teak-alloy-385319.dw_hw_data.materialized_table` 
AS (
  SELECT * FROM `teak-alloy-385319.dw_hw_data.fhv_external_data`
);

-- Q1
SELECT COUNT(*) FROM `teak-alloy-385319.dw_hw_data.materialized_table`;

-- Q2
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `teak-alloy-385319.dw_hw_data.fhv_external_data`;
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `teak-alloy-385319.dw_hw_data.materialized_table`;

-- Q3
SELECT COUNT(*) FROM `teak-alloy-385319.dw_hw_data.materialized_table`
WHERE PULocationID is NULL and DOLocationID is NULL;

-- Q5
CREATE OR REPLACE TABLE `teak-alloy-385319.dw_hw_data.fhv_partitioned_table`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_num AS (
  SELECT * FROM `teak-alloy-385319.dw_hw_data.materialized_table`
);

SELECT DISTINCT(affiliated_base_num) FROM `teak-alloy-385319.dw_hw_data.fhv_partitioned_table` 
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT(affiliated_base_num) FROM `teak-alloy-385319.dw_hw_data.materialized_table` 
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'; 