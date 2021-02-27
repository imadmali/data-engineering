import numpy as np
import pandas as pd

id_values = ['A','B','C','D','E']
N = 10
data = pd.DataFrame({'id': np.random.choice(id_values, size=N, replace=True),
                    'v0' : np.random.normal(loc=0, scale=3, size=N),
                    'v1' : np.random.binomial(n=10, p=0.5, size=N),
                    'v2' : np.random.choice(['Y','N'], size=N, replace=True)})

print(data.head(3))
