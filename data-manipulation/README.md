<!---
<p align="center">
<img href="" src="../images/dm_title.png">
</p>
--->

# Data Manipulation

What initially prompted me to start this repo was to show common data manipulations in different languages/packages in order to make it easier to move across languages.

Currently the following languages/packages are covered.
* Python
  * pandas
  * pyspark
* R
  * dplyr/tidyr
* SQL
  * postgres

The following data manipulations are covered.

1. [Read](#read-data)
2. [Schema](#schema)
3. [Rename columns](#rename-columns)
4. [Create/drop columns](#create-columns)
5. [Select](#select)
6. [Conditions (case statements)](#conditions)
7. [Sorting](#sorting)
8. [Filter/where](#filter)
9. [Group by](#groupby)
10. [Window functions](#window-functions)
11. [Pivot](#pivot)
12. [Join](#join)
13. [Union](#union)
14. [UDF](#udf)
14. [UDAF](#udaf)
15. [Appendix](#appendix)

The queries use the following data.

`fct_table`

```
id |         v0         | v1 | v2
----+--------------------+----+----
 A  |  4.162335551296317 |  8 | N
 E  | -3.463043847744304 |  3 | Y
 A  |  4.116661961802513 |  2 | N
 B  | 2.1683668815994643 |  4 | N
 C  |  5.940306422779212 |  5 | Y
 B  | -5.350971634624138 |  3 | Y
 B  |  3.094433010031948 |  5 | N
 B  | -3.568400083224966 |  6 | N
 D  | -6.691328447186232 |  3 | Y
 A  | -5.293135681469833 |  3 | N
```

`dim_table`

```
identifier |  info  | region
------------+--------+--------
 A          | meta_A |      3
 B          | meta_B |      1
 C          | meta_C |      2
 D          | meta_D |      3
 E          | meta_E |      2
```

## <a name="read-data"></a> Read Data

<!---
<p align="center">
<img href="" src="../images/dm_io.png" width="300">
</p>
--->

**Python - Pandas**

```python
# df = pd.read_csv(...)
fact_table = pd.read_csv('./data/fact_table.csv')
dim_table = pd.read_csv('./data/dim_table.csv')
```

**Python - PySpark**

```python
# df = spark.read.parquet(...)
fact_table = spark.read.csv('./data/fact_table.csv', inferSchema=True, header=True)
dim_table = spark.read.csv('./data/dim_table.csv', inferSchema=True, header=True)
# reading multiple partitions
# df = spark.read.csv('s3://data/fact_table/year=2021/month={1,3}/*')
# reading partition range
# df = spark.read.csv('s3://data/fact_table/year=2021/month=[1-3]/*')
```

**R - dplyr**

```r
fact_table <- read.csv('./data/fact_table.csv')
dim_table <- read.csv('./data/dim_table.csv')
```

**SQL - Postgres**

```sql
/*
Data isn't stored into a variable's memory
but does need to be written to a table in the database (shown below).
*/

CREATE TABLE fact_table
(id CHAR, v0 FLOAT, v1 INT, v2 CHAR);
COPY fact_table FROM '/data/fact_table.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE dim_table
(identifier CHAR, info VARCHAR, region INT);
COPY dim_table FROM '/data/dim_table.csv' DELIMITER ',' CSV HEADER;
```

## <a name="schema"></a> Schema

<!---
<p align="center">
<img href="" src="../images/dm_schema.png" width="300">
</p>
--->

**Python - Pandas**

```python
fact_table.dtypes
```

**Python - PySpark**

```python
fact_table.printSchema()
```

**R - dplyr**

```r
fact_table %>% summarize_all(class)
```

**SQL - Postgres**

```sql
-- \d fact_table
SELECT table_name
  , column_name
  , data_type
  , character_maximum_length
FROM information_schema.columns
WHERE table_name = 'fact_table';
```

## <a name="rename-columns"></a> Rename Columns

<!---
<p align="center">
<img href="" src="../images/dm_rename.png" width="300">
</p>
--->

**Python - Pandas**

```python
# fact_table.rename(columns={'existing_name':'new_name'})
fact_table.rename(columns={'id': 'identifier'})
```

**Python - PySpark**

```python
# one column (multiple column renames have to be chained)
# fact_table.withColumnRenamed('existing_name', 'new_name')
fact_table.withColumnRenamed('id', 'identifier').show()

# multiple columns (using select)
columns = ['v0','v1','v2']
fact_table.select(['id'] + [col(c).alias('field_' + c) for c in columns]).show()
```

**R - dplyr**

```r
# fact_table %>% rename(new_name = existing_name)
fact_table %>%
  rename(identifier = id)
```

**SQL - Postgres**

```sql
SELECT id AS identifier
  , v0
  , v1
  , v2
FROM fact_table;
```

## <a name="create-columns"></a> Create/Drop Columns

<!---
<p align="center">
<img href="" src="../images/dm_newcolumn.png" width="300">
</p>
--->

**Python - Pandas**

```python
# create
fact_table['new_column'] = 'foo'
fact_table['new_column'] = fact_table['v1'] + 1

# drop
fact_table = fact_table.drop(columns='new_column')
```

**Python - PySpark**

```python
# create
fact_table.withColumn('new_column', lit('foo')).show()
fact_table.withColumn('new_column', col('v1') + 1).show()

# drop
fact_table.drop('v0').show()
```

**R - dplyr**

```r
# create
fact_table %>%
  mutate(new_column = "foo")

fact_table %>%
  mutate(new_column = v1+1)

# drop
fact_table %>%
  select(-v1)
```

**Python - SQL**

```sql
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
```

## <a name="select"></a> Select

<!---
<p align="center">
<img href="" src="../images/dm_select.png" width="300">
</p>
--->

**Python - Pandas**

```python
# option 1
fact_table[['id','v0']]

# option 2
fact_table.loc[:, ['id','v0']]

# option 3
fact_table.iloc[:, [0,1]]
```

**Python - PySpark**

```python
# option 1
fact_table.select('id','v0').show()

# option 2
column_names = ['id','v0']
fact_table.select(*column_names).show()
```

**R - dplyr**

```r
fact_table %>%
  select(id, v0)
```

**SQL - Postgres**

```sql
SELECT id
  , v1
  , v2
FROM fact_table;
```

## <a name="conditions"></a> Conditions (Case Statements)

<!---
<p align="center">
<img href="" src="../images/dm_condition.png" width="300">
</p>
--->

**Python - Pandas**

```python
# simple
fact_table['v2'].apply(lambda x: 1 if x == 'Y' else 0)
```

**Python - PySpark**

```python
# simple
fact_table.withColumn('new_column',
    when(col('v2')=='Y', 1).otherwise(0)).show()

# case statement
fact_table.withColumn('new_column',
    when((col('id') == 'A') & (col('v0') < 0), 'Y').\
    when((col('id').isin(['B','D','E'])) & (col('v0') > 0), 'N').\
    otherwise(None)).show()
```

**R - dplyr**

```r
# simple
fact_table %>%
  mutate(new_column = if_else(v2 == 'Y', 1, 0))
```

**SQL - Postgres**

```sql
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
```

## <a name="sorting"></a> Sorting

<!---
<p align="center">
<img href="" src="../images/dm_sort.png" width="300">
</p>
--->

**Python - Pandas**

```python
fact_table.sort_values(by=['id','v0'], ascending=[True,False])
```

**Python - PySpark**

```python
fact_table.sort(['id','v0'], ascending=[True,False]).show()
```

**R - dplyr**

```r
fact_table %>%
  arrange(id, desc(v0))
```

**SQL - Postgres**

```sql
SELECT id
  , v0
FROM fact_table
ORDER BY id, v0 DESC;
```

## <a name="filter"></a> Filter/Where

<!---
<p align="center">
<img href="" src="../images/dm_where.png" width="300">
</p>
--->

**Python - Pandas**

```python
# filter
fact_table.loc[fact_table['v0']>0, :]
fact_table.loc[(fact_table['v0']>0) & (fact_table['v1']<=5), :]

# filter using list
fact_table.loc[fact_table['id'].isin(['A','B']), :]
fact_table.loc[~fact_table['id'].isin(['A','B']), :]

# filter nulls
fact_table.loc[fact_table['id'].isna()]
fact_table.loc[fact_table['id'].notna()]

# filter using regex
fact_table.loc[fact_table['id'].str.contains('A|B')]
```

**Python - PySpark**

```python
# filter
fact_table.filter(col('v0')>0).show()

# filter using list
fact_table.filter(col('id').isin(['A','B'])).show()
fact_table.filter(~col('id').isin(['A','B'])).show()

# filter nulls
fact_table.filter(col('id').isNull()).show()
fact_table.filter(col('id').isNotNull()).show()

# filter using regex
fact_table.filter(col('id').rlike('A|B')).show()
```

**R - dplyr**

```r
# filter
fact_table %>%
  filter(v0 > 0)

# filter using list
fact_table %>%
  filter(id %in% c('A','B','E'))
fact_table %>%
  filter(!id %in% c('A','B','E'))

# filter using regex
fact_table %>%
  filter(grepl('A|B', id))
```

**SQL - Postgres**

```sql
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
WHERE id IS NULL;

SELECT *
FROM fact_table
WHERE id IS NOT NULL;

-- filter using regex
SELECT *
FROM fact_table
WHERE id ~ 'A|B';
```

## <a name="groupby"></a> Group By

<!---
<p align="center">
<img href="" src="../images/dm_groupby.png" width="300">
</p>
--->

**Python - Pandas**

```python
fact_table_gpd = fact_table.groupby('id').agg({'v0': 'sum', 'v1': ['sum','max']})
fact_table_gpd.columns = ['_'.join(c) for c in fact_table_gpd.columns]
fact_table_gpd.reset_index()
```

**Python - PySpark**

```python
fact_table.groupBy('id').agg(sum('v0').alias('sum_v0'),
                             sum('v1').alias('sum_v1'),
                             max('v1').alias('max_v1')).show()
```

**R - dplyr**

```r
fact_table %>%
  group_by(id) %>%
  summarize(N = n(),
            v0_sum = sum(v0),
            v1_sum = sum(v1),
            v1_max = max(v1))
```

**SQL - Postgres**

```sql
SELECT id
  , SUM(v0) AS sum_v0
  , SUM(v1) AS sum_v1
  , COUNT(v1) AS count_v1
FROM fact_table
GROUP BY id;
```

## <a name="window-functions"></a> Window Functions

<!---
<p align="center">
<img href="" src="../images/dm_window.png" width="300">
</p>
--->

**Python - Pandas**

```python
# lag window
fact_table.sort_values(['id','v0']).groupby('id')['v0'].shift(1)

# window sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].rolling(2).sum().reset_index(drop=True)

# cumulative sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].expanding(1).sum().reset_index(drop=True)
```

**Python - PySpark**

```python
# lag window
window_spec = Window.partitionBy('id').orderBy('v0')
fact_table.withColumn('new_column', lag('v0', 1).over(window_spec)).show()

# window sum
window_spec = Window.partitionBy('id').orderBy('v0').rowsBetween(-1, Window.currentRow)
fact_table.withColumn('roll_sum_v0', sum('v0').over(window_spec))

# cumulative sum
window_spec = Window.partitionBy('id').orderBy('v0').rowsBetween(Window.unboundedPreceding, Window.currentRow)
fact_table.withColumn('cum_sum_v0', sum('v0').over(window_spec)).show()
```

**R - dplyr**

```r
# lag window
fact_table %>%
  group_by(v2) %>%
  arrange(v0) %>%
  mutate(v0_lag = lag(v0, 1, NA))

# window sum (not supported in dplyr but you can use RcppRoll)
fact_table %>%
  group_by(v2) %>%
  arrange(v2, v1) %>%
  mutate(v1_sum = RcppRoll::roll_sum(v1, 2, align="right", fill=NA),
         v1_sum_left = RcppRoll::roll_sum(v1, 2, align="left", fill=NA))

# cumulative sum
fact_table %>%
  group_by(v2) %>%
  arrange(v2, v1) %>%
  mutate(v1_sum = cumsum(v1))
```

**SQL - Postgres**

```sql
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
```

## <a name="pivot"></a> Pivot

**Python - Pandas**

```python
pd.pivot_table(fact_table, values='v1', index='id', columns='v2', aggfunc='sum', fill_value=0)
```

**Python - PySpark**

```python
fact_table.groupBy('id').pivot('v2').sum('v1').fillna(0).show()
```

**R - tidyr**

```r
fact_table %>%
  pivot_wider(id_cols = id,
              names_from = v2,
              values_from = v1,
              values_fill = 0,
              values_fn = sum)
```

**SQL - Postgres**

```sql
-- pivoting is done with crosstab which is enabled through the tablefunc extension.
CREATE extension tablefunc;
-- crosstab takes a SQL string: 'SELECT row, column, value FROM ...'
SELECT id
  , COALESCE(N, 0) AS "N"
  , COALESCE(Y, 0) AS "Y"
FROM crosstab('SELECT id, v2, SUM(v1)::INT FROM fact_table GROUP BY 1,2 ORDER BY 1,2')
AS ct(id CHAR, N INT, Y INT);
```

## <a name="join"></a> Join

<!---
<p align="center">
<img href="" src="../images/dm_joins.png" width="300">
</p>
--->

**Python - Pandas**

```python
fact_table.merge(dim_table, left_on='id', right_on='identifier', how='left')
```

**Python - PySpark**

```python
fact_table.join(dim_table, on=[fact_table.id==dim_table.identifier], how='left').show()
```

**R - dplyr**

```r
fact_table %>%
  left_join(dim_table, by=c("id"="identifier"))
```

**SQL - Postgres**

```sql
SELECT *
FROM fact_table f
JOIN dim_table d
ON f.id = d.identifier;
```

## <a name="union"></a> Union

<!---
<p align="center">
<img href="" src="../images/dm_union.png" width="300">
</p>
--->

**Python - Pandas**

```python
pd.concat([fact_table.iloc[:5], fact_table.iloc[5:]])
```

**Python - PySpark**

```python
# two tables
DataFrame.union(fact_table, fact_table)

# more than two tables
reduce(DataFrame.union, [fact_table, fact_table, fact_table]).show()
```

**R - dplyr**

```r
union_all(fact_table[1:5,],fact_table[6:10,])
```

**SQL - Postgres**

```sql
SELECT *
  , 0 AS duplicate
FROM fact_table
UNION
SELECT *
  , 1 AS duplicate
FROM fact_table;
```

## <a name="udf"></a> UDF

<!---
<p align="center">
<img href="" src="../images/dm_udf.png" width="300">
</p>
--->

**Python - Pandas**

```python
def udf_f(row):
    if row['id'] == 'A' and row['v0'] < 0:
        return('Y')
    elif (row['id'] in ['B','D','E']) and row['v0'] > 0:
        return('N')
    else:
        return(None)

fact_table.apply(udf_f, axis=1)
```

**Python - PySpark**

```python
print('UDF')

def udf_f(id, v0):
    if (id == 'A') and (v0 <0):
        return('Y')
    elif (id in ['A','B','D','E']) and (v0 > 0):
        return('N')
    else:
        return(None)

udf_f_reg = udf(udf_f, StringType())

fact_table.withColumn('new_column', udf_f_reg('id', 'v0')).show()
```

**R - dplyr**

```r
udf_f <- function(id, v0) {
  if (id == "A" & v0 < 0)
    return('Y')
  else if (id %in% c('D','E') & v0 > 0)
    return('N')
  else
    return(NA)
}

fact_table %>%
  rowwise() %>%
  mutate(new_column = udf_f(id, v0))
```

**SQL - Postgres**

```sql
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
```

## <a name="udaf"></a> UDAF

**Python - Pandas**

```python
def udaf_f(x):
    return(sum(x)/len(x))

fact_table.groupby('id').agg({'v0': udaf_f})
```

**Python - PySpark**

Here we use `spark.sql.functions.pandas_udf` to create a UDAF and apply it to a PySpark dataframe. The required dependencies to get this to work were:
- `pyspark==3.1.2`
- `pyarrow==4.0.1`
- Adding `ARROW_PRE_0_15_IPC_FORMAT=1` to `conf/spark-env.sh`

```python
@pandas_udf("double")
def udaf_f(x: pd.Series) -> float:
   return(np.sum(x)/len(x))

fact_table.groupBy('id').agg(udaf_f('v0').alias('mean')).show()
```

**R - dplyr**

```r
udaf_f <- function(x) {
  return(sum(x)/length(x))
}

fact_table %>%
  group_by(id) %>%
  summarize(mean = udaf_f(v0))
```

**SQL - Postgres**

```sql
-- accumulator
CREATE OR REPLACE FUNCTION float_accum(FLOAT[], FLOAT)
RETURNS FLOAT[]
AS
$$
  SELECT ARRAY[$1[1]+$2, $1[2]+1]
$$
LANGUAGE SQL;

-- final mean calculation
CREATE OR REPLACE FUNCTION float_mean(FLOAT[])
RETURNS FLOAT
AS
$$
  SELECT $1[1] / $1[2]
$$
LANGUAGE SQL;

-- udaf to wrap everything together
CREATE AGGREGATE udaf_f(FLOAT) (
  SFUNC = float_accum,
  STYPE = FLOAT[],
  FINALFUNC = float_mean,
  INITCOND = '{0,0}'
);
-- \da to view created aggregates

SELECT udaf_f(v0) mean
  , AVG(v0) base_sum
FROM fact_table
GROUP BY id;
```

## <a name="appendix"></a> Appendix

Some ad-hoc concepts that are useful to know and may not apply to all languages/frameworks.

### PySpark - Persisting data into memory

With PySpark sometimes it's useful to persist smaller data. It can help your queries complete faster.

```python
data.persist()
```

Just don't forget to unpersist the data to free up memory.

```python
data.unpersist()
```

### SQL - Subqueries

Sometimes you want to use the results of one query in another query, without writing the information to disk. One way to do this is with a subquery. A subquery is a SQL query that is nested inside another SQL query.

```sql
SELECT *
FROM fact_table
WHERE id IN (SELECT DISTINCT id FROM dim_table);
```

### SQL - CTEs

Sometimes you may want to create a temporary table that you don't want save to disk (similar to the subquery problem above). In most languages you can store this temporary table in a variable (that gets deleted from memory later on). In SQL you can use a common table expression (CTE) to create a temporary table that gets deleted from memory when your query completes.

```sql
WITH tmp_table AS (
	SELECT id, name, region FROM fact_table
)

SELECT *
FROM tmp_table
WHERE id = 1;
```
