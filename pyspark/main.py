import os
from time import time
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum

spark = SparkSession.builder.appName('pyspark-performance').getOrCreate()

file_path = 's3://imad-pyspark-test/data/parquet/small'
loc, extension = os.path.splitext(file_path)
print(file_path)
seconds = []
N = 100

for i in range(N):
    start = time()

    if extension == '.csv':
        data = spark.read.csv(file_path, inferSchema=True, header=True)
    else:
        data = spark.read.parquet(file_path, inferSchema=True, header=True)

    agg = data.groupBy('v0').agg(count('v0').alias('count_v0'),
                                 sum('v1').alias('sum_v1'),
                                 sum('v2').alias('sum_v2'))
    agg.show()
    seconds.append(time()-start)
    if i == N-1:
        print(agg.show())

    del data, agg

seconds = np.array(seconds)
print('Avg seconds elapsed over {0} iter: {1}, sd: {2}'.format(N,
                                                               np.round(np.mean(seconds),4),
                                                               np.round(np.std(seconds),4)))
