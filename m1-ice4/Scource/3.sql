-- Create partitioned table
create table petrol2 (
    distributer_id STRING,
    distributer_name STRING,
    amt_IN STRING,
    amy_OUT STRING,
    vol_IN INT,
    vol_OUT INT)  partitioned by (year INT)
row format delimited fields terminated by ',' stored as textfile;

-- Change settings for allow partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Load data
INSERT OVERWRITE TABLE petrol2 PARTITION (year)
SELECT * FROM petrol
where year = 2018;

-- Join with two tables
select
    distributer_name,
    total
from
    petrol2
join
    olympic on petrol2.year = olympic.year
where
    petrol2.year = 2018;