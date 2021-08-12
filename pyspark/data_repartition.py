import sys
from time import time
import numpy as np
import pandas as pd
import boto3
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

start = time()

creds = boto3.Session().get_credentials()
sparkConf = SparkConf()
sparkConf.set("spark.hadoop.fs.s3a.access.key", creds.access_key)
sparkConf.set("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
sparkConf.set("spark.hadoop.fs.s3a.path.style.access", True)
sparkConf.set("spark.hadoop.fs.s3a.access.key", creds.access_key)
sparkConf.set("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
sparkConf.set("spark.hadoop.fs.s3a.endpoint", "s3-us-east-2.amazonaws.com")
sparkConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sparkConf.set("com.amazonaws.services.s3.enableV4", True)
sparkConf.set("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
sparkConf.set('spark.driver.memory', '4G')
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# Generate data
N = 1_000_000 # number of rows
data = pd.DataFrame({'v0': np.random.choice(['A','B','C','D','E'], size=N, replace=True),
                         'v1': np.random.binomial(1, 0.5, N),
                         'v2': np.random.normal(0, 1, N)})

spark_data = spark.createDataFrame(data)

# Write to S3
S3_BUCKET = 'imad-pyspark-test/'
partition_count = [1, 100, 500, 1000]
for p in partition_count:
    print('Writing parquet to s3 [{0} partitions].'.format(p))
    S3_KEY = 'data/repartition/parquet/' + str(p)
    spark_data.repartition(p).write.parquet('s3a://' + S3_BUCKET + S3_KEY, mode='overwrite')
    print('... Complete.')

print('Seconds elapsed: {0}'.format(time()-start))
