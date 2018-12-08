-- Create table 
create table petrol (
    distributer_id STRING,
    distributer_name STRING,
    amt_IN STRING,
    amy_OUT STRING,
    vol_IN INT,
    vol_OUT INT,
    year INT)
row format delimited fields terminated by ',' stored as textfile;

-- Load data
load data local inpath '/home/cloudera/Desktop/HW/ICP/4/petrol.txt' into table petrol;

-- 1) In real life what is the total amount of petrol in volume sold by every distributor?
SELECT
    distributer_name,
    SUM(vol_OUT)
FROM
    petrol
GROUP BY
    distributer_name;

-- Which are the top 10 distributors ID’s for selling petrol and also display the amount of petrol sold in volume by them individually?
SELECT 
    distributer_id,
    vol_OUT
FROM
    petrol
order by
    vol_OUT desc
limit 10;

-- Find real life 10 distributor name who sold petrol in the least amount.
SELECT
    distributer_id,
    vol_OUT
FROM
    petrol
order by
    vol_OUT
limit 10;

-- List all distributors who have this difference, along with the year and the difference which they have in that year.
SELECT
    distributer_name,
    year,
    (vol_in - vol_out) diff
FROM
    petrol 
WHERE
    (vol_in - vol_out) > 400;
