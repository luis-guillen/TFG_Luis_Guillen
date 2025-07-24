
import importlib
import pandas as pd
import os
import pickle
import sys
# import numpy as np
import time
from Libs import lib_cmlp as cmlp
from Libs import lib_bam  as lbam

#Path definitions and functions
#------------------------------

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

dat = 'tur_1'
with open(os.path.join(tmppathres,'bam_'+dat+'.pkl'),'rb') as f: data = pickle.load(f)



data = ['cma_6']

for dat in data:

    with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as f: data = pickle.load(f)
    data_group = data.groupby('nif').size().to_frame()
    data_group.reset_index(level=0, inplace=True)
    data_group.columns=['nif','count']
    nyears = data['year'].nunique(dropna=True)
    data_group_erase = data_group.drop(data_group["count"].loc[data_group["count"]==nyears].index)
    erase=list(data_group_erase.iloc[:,0].values)
    data_new = data[~data.nif.isin(erase)]
print('fin')