import importlib
import pandas as pd
import os
import pickle
import sys
# import numpy as np
import time
from Libs import lib_cmlp as cmlp
from Libs import lib_bam  as lbam

with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as f:table = pickle.load(f)