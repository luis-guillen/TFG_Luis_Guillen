#from numpy.lib import nanfunctions
import importlib
import pandas as pd
import os
import pickle
import sys
# import numpy as np
# import matplotlib.pyplot as plt
# import statistics
# import numpy as np
# import seaborn as sns
import time
#sys.path.append("..")
from Libs import lib_cmlp as cmlp
from Libs import lib_bam  as lbam
#from pandas_profiling import ProfileReport

start_time = time.time()

start_time = time.time()


#Path definitions and functions
#------------------------------

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

# Labels
acctsr=['01.1.1',
        '01.2.1',
        '01.2.2',
        '01.3.2',
        '02.2.0',
        '03.1.0',
        '04.1.1',
        '04.1.3',
        '04.1.4',
        '04.2.0',
        '04.3.0',
        '05.1.0',
        '06.1.0',
        '07.1.0',
        '08.1.0',
        '08.2.0',
        '08.3.0',
        '09.1.0',
        '09.2.0',
        '09.3.0',
        '09.4.0',
        '10.1.0',
        '10.2.0',
        '11.1.0',
        '12.2.0',
        '13.1.0',
        '14.2.0',
        '15.1.0',
        '16.1.0']

acctsc=acctsr.copy()

def table1_etl_1(lista_data):
    data = lista_data  

# Read data file:
    years  = {}
    nifs   = {}
    bam    = {}
    for dat in data:
        with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as f:table = pickle.load(f)
        years[dat] = pd.unique(table[dat]['year'])   
        nifs[dat]  = pd.unique(table[dat]['nif']) 
        bam[dat] = lbam.bam_generator(table,dat,years,nifs,acctsr,acctsc)

    return bam

# print(years)



# table1 = pd.DataFrame(columns=['year','nif','bam_row','bam_col','value'])

# def table1_1(years,companies,lista_data):
# for year in years:
# 	for c in companies:
 		
# 			listavacia.append([y,e,i,j,lookup_tabla_sectores(y,e,i,j)])
# table1 







