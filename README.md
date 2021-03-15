# Data Manipulation

The purpose of this repo is to show common data manipulations in different languages/frameworks.

Currently the following language/frameworks are covered:
* Python
  * pandas
  * pyspark
* R
  * dplyr
* SQL
  * postgres

The following data manipulations are covered:

1. [Read](#read-data)
2. Rename columns
3. Create/drop columns
4. Select
5. Conditions (case statements)
6. Sorting
7. Filter/where
8. Group by
9. Window functions
10. UDFs

import pandas as pd

### <a name="read-data"></a> READ DATA 

**python-pandas**
```python
fact_table = pd.read_csv('./data/fact_table.csv')
dim_table = pd.read_csv('./data/dim_table.csv')
```
**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### RENAME

**python-pandas**
```python
fact_table.rename(columns={'id': 'identifier'})
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```


### CREATE/DROP COLUMNS

**python-pandas**
```python
# create
fact_table['new_column'] = 'foo'
fact_table['new_column'] = fact_table['v1'] + 1

# drop
fact_table = fact_table.drop(columns='new_column')
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### SELECT

**python-pandas**
```python
# option 1
fact_table[['id','v0']]

# option 2
fact_table.loc[:, ['id','v0']]

# option 3
fact_table.iloc[:, [0,1]]
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### CONDITIONS (CASE STATEMENTS)

**python-pandas**
```python
# simple
fact_table['v2'].apply(lambda x: 1 if x == 'Y' else 0)
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### SORTING

**python-pandas**
```python
fact_table.sort_values(by=['id','v0'], ascending=[True,False])
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### FILTER/WHERE

**python-pandas**
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
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### GROUP BY

**python-pandas**
```python
fact_table_gpd = fact_table.groupby('id').agg({'v0': 'sum', 'v1': ['sum','max']})
fact_table_gpd.columns = ['_'.join(c) for c in fact_table_gpd.columns]
fact_table_gpd.reset_index()
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### WINDOW

**python-pandas**
```python
# lag window
fact_table.sort_values(['id','v0']).groupby('id')['v0'].shift(1)

# window sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].rolling(2).sum().reset_index(drop=True)

# cumulative sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].expanding(1).sum().reset_index(drop=True)
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### JOIN

**python-pandas**
```python
fact_table.merge(dim_table, left_on='id', right_on='identifier', how='left')
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### UNION

**python-pandas**
```python
pd.concat([fact_table.iloc[:5], fact_table.iloc[5:]])
```

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```

### UDF

**python-pandas**
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

**python-pyspark**
```python
```
**r-dplyr**
```r
```
**sql-postgres**
```sql
```
