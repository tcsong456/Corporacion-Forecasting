import pandas as pd
import numpy as np

pivot_promo = pd.read_csv('data/op.txt',index_col=['store_nbr','item_nbr']).values.astype(np.int8)
for i in range(pivot_promo.shape[1]):
    nan = (pivot_promo[:,i] == 2)
    if nan.sum() < pivot_promo.shape[0]:
        p = 0.2 * pivot_promo[~nan,i].mean()
    else:
        p = 0
    fill = np.random.binomial(n=1,p=p,size=nan.sum())
    pivot_promo[nan,i] = fill

np.save('data/op.npy',pivot_promo.astype(np.int8))
#%%
