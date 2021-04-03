# prg_1_2_bam.py

#from numpy.lib import nanfunctions
import importlib
import pandas as pd
import os
import pickle
import sys
import time
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


#Path definitions and functions
#------------------------------

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

# Labels
acctrs=['01.1.0','01.2.1','01.2.2','01.2.3','02.1.0','03.1.0',
        '04.1.1','04.1.2','04.1.3','04.2.0','04.3.0','05.1.0',
        '06.1.0','07.1.0','08.1.0','08.2.0','08.3.0','09.1.0',
        '09.2.0','09.3.0','10.1.0','10.2.0','11.1.0',
        '12.1.0','13.1.0','14.1.0','15.1.0','16.1.0']

acctcs=acctrs.copy()


def bam_etl_1(lista_data,lista_years,lista_nifs):
    data  = lista_data  
    years = lista_years
    nifs  = lista_nifs

# Read data file:
    bam0    = {}
    bam     = {}
    bam_dic = {}
    bam_arrays_dic = {}
    col_sums_dic = {}
    row_sums_dic = {}
    col_sums_dic = {}
    sum_difs_dic = {}
    sum_difs_df  = {}
    start_bam = time.time()    

    #print('\n')
    print('     1.- Generating and completing the bams')

    for dat in data:
        with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as f:data = pickle.load(f)
        table=data[dat].copy()
        bam[dat]                = lbam.bam_generator(table,years,nifs[dat],acctrs,acctcs)
        print(f'        Time (min) --> generation time: {( time.time() - start_bam)/60}')
        bam[dat]                = lbam.bam_completion(bam[dat],years,nifs[dat])
        print(f'        Time (min) --> completion time: {( time.time() - start_bam)/60}')
        bam_dic[dat]            = lbam.bam_dictionaries(bam[dat],years,nifs[dat])
        print(f'        Time (min) --> dictionaries time: {( time.time() - start_bam)/60}')
        col_sums_dic[dat],row_sums_dic[dat],sum_difs_dic[dat]   = lbam.bam_checking(bam_dic[dat],years,nifs[dat])
        print(f'        Time (min) --> checking time: {( time.time() - start_bam)/60}')
        sum_difs_df[dat]        = pd.DataFrame.from_dict(sum_difs_dic[dat], orient = 'index')
        sum_difs_df[dat]        = sum_difs_df[dat].transpose()
        print(f'        Time (min) --> difs df generation time: {( time.time() - start_bam)/60}')
    return bam, bam_dic, col_sums_dic, row_sums_dic, sum_difs_dic,sum_difs_df