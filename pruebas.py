
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



data = ['con_1','tur_1']

for dat in data:

    with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as f: data = pickle.load(f)

    bam=pd.read_csv(os.path.join(tmppathint,'bam_'+dat+'.csv'), delimiter=';', encoding='latin1', decimal=',')

    with open(os.path.join(tmppathres,'bam_dic_'+dat+'.pkl'),'rb') as f: bam = pickle.load(f)
    with open(os.path.join(tmppathint,'cols_dic_'+dat+'.pkl'),'rb') as f: cols = pickle.load(f)
    with open(os.path.join(tmppathint,'rows_dic_'+dat+'.pkl'),'rb') as f: rows = pickle.load(f)
    with open(os.path.join(tmppathint,'difs_df_'+dat+'.pkl'),'rb') as f: difs = pickle.load(f)
    print(dat)
    print(difs[dat].sum())
    print('\b')
print('fin')