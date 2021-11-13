import numpy as np
import pandas as pd
import string

N = 10
num_ids = 5
id_values = [string.ascii_uppercase[i] for i in range(0,num_ids)]

fact_table = pd.DataFrame({'id': np.random.choice(id_values, size=N, replace=True),
                           'v0' : np.random.normal(loc=0, scale=3, size=N),
                           'v1' : np.random.binomial(n=10, p=0.5, size=N),
                           'v2' : np.random.choice(['Y','N'], size=N, replace=True)})

dim_table = pd.DataFrame({'identifier': id_values,
                          'info': ['meta_' + item for item in id_values],
                          'region': np.random.choice([1,2,3], size=num_ids, replace=True)})

fact_table.to_csv('./fact_table.csv', header=True, index=False)
dim_table.to_csv('./dim_table.csv', header=True, index=False)
