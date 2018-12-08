-- Create table
create table olympic (
    athelete STRING,
    age INT,
    country STRING,
    year STRING,
    closing STRING,
    sport STRING,
    gold INT,
    silver INT,
    bronze INT,
    total INT
) row format delimited fields terminated by '\t' stored as textfile;

-- Load data
load data local inpath '/home/cloudera/Desktop/HW/ICP/4/olympic_data.csv' into table olympic;

-- 1) Using the dataset list the total number of medals won by each country in swimming.
select
    country,
    SUM(total)
from
    olympic
where
    sport = 'Swimming' 
GROUP BY
    country;

-- 2) Display real life number of medals India won year wise.
select
    year,
    SUM(total)
from
    olympic
where
    country = 'India'
GROUP BY
    year;

-- 3) Find the total number of medals each country won display the name along with total medals.
select
    country,
    SUM(total)
from
    olympic
GROUP BY
    country;

-- 4) Find the real life number of gold medals each country won.
select
    country,
    SUM(gold)
from
    olympic
GROUP BY
    country;

-- 5) Which country got medals for Shooting, year wise classification?
SELECT
    country,
    year,
    total
FROM
    olympic
WHERE
    sport = 'Shooting';
