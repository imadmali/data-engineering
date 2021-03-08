import pandas as pd

### READ DATA

fact_table = pd.read_csv('./data/fact_table.csv')
dim_table = pd.read_csv('./data/dim_table.csv')

### RENAME

fact_table.rename(columns={'id': 'identifier'})

### CREATE/DROP COLUMNS

fact_table['new_column'] = 'foo'
fact_table['new_column'] = fact_table['v1'] + 1

fact_table = fact_table.drop(columns='new_column')

### SELECT

# option 1
fact_table[['id','v0']]

# option 2
fact_table.loc[:, ['id','v0']]

# option 3
fact_table.iloc[:, [0,1]]

### CONDITIONS (CASE STATEMENTS)

def f(row):
    if row['id'] == 'A' and row['v0'] < 0:
        return('Y')
    elif (row['id'] in ['B','D','E']) and row['v0'] > 0:
        return('N')
    else:
        return(None)

fact_table.apply(f, axis=1)

### SORTING

fact_table.sort_values(by=['id','v0'], ascending=[True,False])

### FILTER/WHERE

# filter
fact_table.loc[fact_table['v0']>0, :]
fact_table.loc[(fact_table['v0']>0) & (fact_table['v1']<=5), :]

# filter using list
fact_table.loc[fact_table['id'].isin(['A','B']), :]
fact_table.loc[~fact_table['id'].isin(['A','B']), :]

# filter nulls
fact_table.loc[fact_table['id'].isna()]
fact_table.loc[fact_table['id'].notna()]

### GROUP BY

fact_table_gpd = fact_table.groupby('id').agg({'v0': 'sum', 'v1': ['sum','max']})
fact_table_gpd.columns = ['_'.join(c) for c in fact_table_gpd.columns]
fact_table_gpd.reset_index()

### WINDOW

# lag window
fact_table.sort_values(['id','v0']).groupby('id')['v0'].shift(1)

# window sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].rolling(2).sum().reset_index(drop=True)

# cumulative sum
fact_table.sort_values(['id','v0']).groupby('id')['v0'].expanding(1).sum().reset_index(drop=True)

### JOIN

fact_table.merge(dim_table, left_on='id', right_on='identifier', how='left')

### UNION

pd.concat([fact_table.iloc[:5], fact_table.iloc[5:]])
