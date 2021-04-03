#%%
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

with open(os.path.join(tmppathint,'bam_dic_tur_1.pkl'),'rb') as f: bam = pickle.load(f)
with open(os.path.join(tmppathint,'difs_df_tur_1.pkl'),'rb') as f: difs = pickle.load(f)
with open(os.path.join(tmppathint,'cols_dic_tur_1.pkl'),'rb') as f: cols = pickle.load(f)
with open(os.path.join(tmppathint,'rows_dic_tur_1.pkl'),'rb') as f: rows = pickle.load(f)

# %%
