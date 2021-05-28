/* READ DATA */

-- Data isn't stored into a variable's memory
-- but does need to be written to a table in the database (shown below).

CREATE TABLE fact_table
(id CHAR, v0 FLOAT, v1 INT, v2 CHAR);
COPY fact_table FROM '/data/fact_table.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE dim_table
(identifier CHAR, info VARCHAR, region INT);
COPY fact_table FROM '/data/dim_table.csv' DELIMITER ',' CSV HEADER;

/* RENAME */

SELECT id AS identifier
  , v0
  , v1
  , v2
FROM fact_table;

/* CREATE/DROP COLUMNS */

-- create
SELECT *
  , 'foo' AS new_column
FROM fact_table;

SELECT *
  , v1 + 1 AS new_column
FROM fact_table;

-- drop (omit from select)
SELECT id
  , v1
FROM fact_table;

/* SELECT */

SELECT id
  , v1
  , v2
FROM fact_table;

/* CONDITIONS (CASE STATEMENTS) */

-- simple
SELECT *
  , CASE WHEN v2 = 'Y' THEN 1 ELSE 0 END new_column
FROM fact_table;

-- case statment
SELECT *
  , CASE
      WHEN (id = 'A' AND v0 < 0) THEN 'Y'
      WHEN (id IN ('B','D','E') AND v0 > 0) THEN 'N'
      ELSE NULL
      END new_column
FROM fact_table;

/* SORTING */

SELECT id
  , v0
FROM fact_table
ORDER BY id, v0 DESC;

/* FILTER/WHERE */

-- filter
SELECT *
FROM fact_table
WHERE v0 > 0;

SELECT *
FROM fact_table
WHERE v0 > 0 AND v1 <= 5;

SELECT *
FROM fact_table
WHERE id ~ 'A|B';

-- filter using list
SELECT *
FROM fact_table
WHERE id IN ('A','B');

SELECT *
FROM fact_table
WHERE id NOT IN ('A','B');

-- filter nulls
SELECT *
FROM fact_table
WHERE id IS NULL;

SELECT *
FROM fact_table
WHERE id IS NOT NULL;

/* GROUP BY */

SELECT id
  , SUM(v0) AS sum_v0
  , SUM(v1) AS sum_v1
  , COUNT(v1) AS count_v1
FROM fact_table
GROUP BY id;

/* WINDOW */

-- lag window
SELECT *
  , LAG(v0, 1) OVER (PARTITION BY id ORDER BY v0)
FROM fact_table
ORDER BY id, v0;

-- window sum
SELECT *
  , SUM(v0) OVER (PARTITION BY id ORDER BY v0 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
FROM fact_table
ORDER BY id, v0;

-- cumulative sum
SELECT *
  , SUM(v0) OVER (PARTITION BY id ORDER BY v0 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM fact_table
ORDER BY id, v0;

/* JOIN */

SELECT *
FROM fact_table f
JOIN dim_table d
ON f.id = d.identifier;

/* UNION */

SELECT *
  , 0 AS duplicate
FROM fact_table
UNION
SELECT *
  , 1 AS duplicate
FROM fact_table;

/* UDF */

CREATE OR REPLACE FUNCTION udf_f(id CHAR, v0 FLOAT)
RETURNS VARCHAR(1)
AS
$$
  SELECT
    CASE
      WHEN (id = 'A' AND v0 < 0) THEN 'Y'
      WHEN (id IN ('B','D','E') AND v0 > 0) THEN 'N'
      ELSE NULL END;
$$
LANGUAGE SQL;
-- \df to view created functions

SELECT *
  , udf_f(id, v0)
FROM fact_table;

/* UDAF */

CREATE OR REPLACE FUNCTION udaf_sum(a FLOAT, b FLOAT)
RETURNS FLOAT
AS
$$
  SELECT a + b
$$
LANGUAGE SQL;

CREATE AGGREGATE agg_sum(FLOAT) (
  SFUNC = udaf_sum,
  STYPE = FLOAT,
  INITCOND = 0
);
-- \da to view created aggregates

SELECT agg_sum(v0) udaf_sum
  , sum(v0) base_sum
FROM fact_table
GROUP BY id;
