from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum, max, lag, DataFrame, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from functools import reduce

spark = SparkSession.builder.appName('data-manipulation').getOrCreate()

### READ DATA

fact_table = spark.read.csv('./data/fact_table.csv', inferSchema=True, header=True)
dim_table = spark.read.csv('./data/dim_table.csv', inferSchema=True, header=True)

### RENAME

# one columns
fact_table.withColumnRenamed('id', 'identifier').show()

# multiple columns
columns = ['v0','v1','v2']
fact_table.select(['id'] + [col(c).alias('field_' + c) for c in columns]).show()

### CREATE/DROP COLUMNS

# create
fact_table.withColumn('new_column', lit('foo')).show()
fact_table.withColumn('new_column', col('v1') + 1).show()

# drop
fact_table.drop('v0').show()

### SELECT

# option 1
fact_table.select('id','v0').show()

# option 2
column_names = ['id','v0']
fact_table.select(*column_names).show()

### CONDITIONS (CASE STATEMENTS)

# simple
fact_table.withColumn('new_column',
    when(col('v2')=='Y', 1).otherwise(0)).show()

# case statement
fact_table.withColumn('new_column',
    when((col('id') == 'A') & (col('v0') < 0), 'Y').\
    when((col('id').isin(['B','D','E'])) & (col('v0') > 0), 'N').\
    otherwise(None)).show()

### SORTING

fact_table.sort(['id','v0'], ascending=[True,False]).show()

### FILTER/WHERE

# filter
fact_table.filter(col('v0')>0).show()

# filter using list
fact_table.filter(col('id').isin(['A','B'])).show()
fact_table.filter(~col('id').isin(['A','B'])).show()

# filter nulls
fact_table.filter(col('id').isNull()).show()
fact_table.filter(col('id').isNotNull()).show()

### GROUP BY

fact_table.groupBy('id').agg(sum('v0').alias('sum_v0'),
                             sum('v1').alias('sum_v1'),
                             max('v1').alias('max_v1')).show()

### WINDOW

# lag window
window_spec = Window.partitionBy('id').orderBy('v0')
fact_table.withColumn('new_column', lag('v0', 1).over(window_spec)).show()

# window sum
window_spec = Window.partitionBy('id').orderBy('v0').rowsBetween(-1, Window.currentRow)
fact_table.withColumn('roll_sum_v0', sum('v0').over(window_spec))

# cumulative sum
window_spec = Window.partitionBy('id').orderBy('v0').rowsBetween(Window.unboundedPreceding, Window.currentRow)
fact_table.withColumn('cum_sum_v0', sum('v0').over(window_spec)).show()

### JOIN

fact_table.join(dim_table, on=[fact_table.id==dim_table.identifier], how='left').show()

### UNION

# two tables
DataFrame.union(fact_table, fact_table)

# more than two tables
reduce(DataFrame.union, [fact_table, fact_table, fact_table]).show()

### UDF
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
