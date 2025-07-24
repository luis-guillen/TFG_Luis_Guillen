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

tmppathbam1 = r'Data/bam_1'

dat = 'tur_1'
with open(os.path.join(tmppathbam1,'A01007137.pkl'),'rb') as f: data = pickle.load(f)
#%%