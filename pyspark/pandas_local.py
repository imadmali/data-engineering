import os
from time import time
import pandas as pd
import numpy as np

file_path = 's3://imad-pyspark-test/data/parquet/large'
print(file_path)
seconds = []
N = 100

for i in range(N):
    start = time()

    loc, extension = os.path.splitext(file_path)
    if extension == '.csv':
        data = pd.read_csv(file_path)
    else:
        data = pd.read_parquet(file_path)

    agg = data.groupby('v0').agg({'v0': 'count', 'v1': 'sum', 'v2': 'sum'})
    seconds.append(time()-start)
    if i == N-1:
        print(agg)

    del data, agg

seconds = np.array(seconds)
print('Avg seconds elapsed over {0} iter: {1}, sd: {2}'.format(N,
                                                               np.round(np.mean(seconds),4),
                                                               np.round(np.std(seconds),4)))
