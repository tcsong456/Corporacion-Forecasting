import pandas as pd
import numpy as np

df = pd.read_csv('data/x_raw.txt')

cols = []
for col in df.columns:
    try:
        int(col)
        cols.append(col)
    except ValueError:
        pass
x = df[cols].values.astype(np.float16)
lags = [1,7,14,30,90,180,360,600]
x_lags = np.zeros([df.shape[0],x.shape[1],len(lags)],dtype=np.float16)
for i,lag in enumerate(lags):
    x_lags[:,lag:,i] = x[:,:-lag]

np.save('data/x_lags.npy',x_lags)
del x_lags,x

groups = [
    ['store_nbr'],
    ['item_nbr'],
    ['family'],
    ['class'],
    ['city'],
    ['state'],
    ['type'],
    ['cluster'],
    ['item_nbr', 'city'],
    ['item_nbr', 'type'],
    ['item_nbr', 'cluster'],
    ['family', 'city'],
    ['family', 'type'],
    ['family', 'cluster'],
    ['store_nbr', 'family'],
    ['store_nbr', 'class']
]
df_idx = df[['store_nbr', 'item_nbr', 'family', 'class', 'city', 'state', 'type', 'cluster']]

df[cols] = df[cols].astype(np.float16)
aux_data = np.zeros([df.shape[0],len(cols),len(groups)],dtype=np.float16)
for i,group in enumerate(groups):
    gdata = df.groupby(group).mean()[cols].reset_index()
    gdata = df_idx.merge(gdata,how='left',on=group)
    aux_data[:,:,i] = gdata[cols].fillna(0).values
np.save('data/aux_data.npy',aux_data)
del aux_data


#%%
