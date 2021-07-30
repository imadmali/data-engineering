from time import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum

spark = SparkSession.builder.appName('pyspark-performance').getOrCreate()

start = time()

file_path = 's3://imad-pyspark-test/data/wide/parquet/medium'
data = spark.read.csv(file_path, inferSchema=True, header=True)
agg = data.groupBy('v0').agg(count('v0').alias('count_v0'),
                             sum('v1').alias('sum_v1'),
                             sum('v2').alias('sum_v2'))
print(file_path)
print(agg.show())
print('Seconds elapsed: {0}'.format(time()-start))
