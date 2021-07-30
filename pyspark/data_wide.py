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

# print(spark.sparkContext.getConf().getAll())

# Generate data
N = 100_000 # number of rows
K = 30 # number of columns

data = pd.DataFrame({'v0': np.random.choice(['A','B','C','D','E'], size=N, replace=True)})
for k in range(1,K):
    mu = np.random.uniform(-10,10,1)[0]
    data['v{0}'.format(k)] = np.round(np.random.normal(mu, 3, size=N), 3)

print('Size: {0}kb'.format(sys.getsizeof(data)/1000))
print('Shape: {0}'.format(data.shape))
# print(data.info(verbose=True))

spark_data = spark.createDataFrame(data)

# Write to S3
data_size = 'medium'
S3_BUCKET = 'imad-pyspark-test/'

# CSV
S3_KEY = 'data/wide/csv/' + data_size + '.csv'
print('Writing csv to s3.')
# spark_data.write.csv('s3a://' + S3_BUCKET + S3_KEY, mode='overwrite')
data.to_csv('s3a://' + S3_BUCKET + S3_KEY, index=False)
print('... Complete.')

# Parquet
S3_KEY = 'data/wide/parquet/' + data_size
print('Writing parquet to s3.')
spark_data.write.option("maxRecordsPerFile", 1_000).parquet('s3a://' + S3_BUCKET + S3_KEY, mode='overwrite')
print('... Complete.')

print('Seconds elapsed: {0}'.format(time()-start))
