/* READ DATA */

-- not applicable; use select statement

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
  , v1
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
WHERE id IS NOT NULL;

SELECT *
FROM fact_table
WHERE id IS NOT NULL;

/* GROUP BY */

SELECT SUM(v0) AS sum_v0
  , SUM(v1) AS sum_v1
  , COUNT(v1) AS count_v1
FROM fact_table
GROUP BY id;

/* WINDOW */

-- lag window
SELECT *
  , LAG(v0, 1) OVER (PARTITION BY id ORDER BY v0)
FROM fact_table

-- window sum
SELECT *
  , SUM(v0) OVER (PARTITION BY id ORDER BY v0 ROWS 1 PRECEDING AND CURRENT ROW)
FROM fact_table

-- cumulative sum
SELECT *
  , SUM(v0) OVER (PARTITION BY id ORDER BY v0 ROWS UNBOUND PRECEDING AND CURRENT ROW)
FROM fact_table

/* JOIN */

SELECT *
FROM fact_table f
JOIN dim_table d
ON f.id = d.identifier;

/* UNION */

SELECT *
FROM fact_table
UNION
SELECT *
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
      ELSE NULL;
$$
LANGUAGE SQL;

SELECT *
  , udf_f(id, v0)
FROM fact_table;
