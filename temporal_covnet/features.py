import pandas as pd
import numpy as np

dtypes = {'store_nbr':np.int8,
          'unit_sales':np.float16,
          'cluster':np.int8,
          'class':np.int16
          }

stores = pd.read_csv('data/stores.csv', dtype=dtypes)
items = pd.read_csv('data/items.csv', dtype=dtypes)
train = pd.read_csv('data/train.csv', parse_dates=['date'], dtype=dtypes)
test = pd.read_csv('data/test.csv', parse_dates=['date'], dtype=dtypes)
z = train.iloc[:10000]
zz = test.iloc[:1000]

#%%
#zz = pd.read_csv('data/df_train.csv')
train.iloc[-100:]
#pd.isnull(test['onpromotion']).sum()

#test.groupby(['store_nbr','item_nbr']).mean()
#112777*2
#z = train.iloc[:10000]...

#test_bak = pd.read_csv('data/test.csv', dtype=dtypes)
