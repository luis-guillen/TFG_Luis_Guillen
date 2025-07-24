# prg_1_2_bam.py

#Path definitions and functions
#------------------------------

# Standard library imports
import os
import sys
import argparse
import pickle
import time
import pandas as pd

# Local application imports
# from   Files import prg_1_1_lectura_datos
# from   Files import prg_1_2_bam
# from   Libs import lib_bam as lbam

import Files.prg_1_1_lectura_datos as prog1
import Files.prg_1_2_bam as prog2
import Libs.lib_bam as lbam

# Third party imports
#from Libs import lib_cmlp as cmlp
import Libs.lib_cmlp as cmlp

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

    # realización de la segunda fase del etl de la BAM: calculating table-1
    # =====================================================================
    
    
def bam_etl_1(lista_data,lista_years,lista_nifs):
    start_total = time.time()  
    data  = lista_data  
    years = lista_years
    nifs  = lista_nifs

# Read data file:
    nifs_dic= {}
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
    # seconds passed since epoch
    local_time = time.ctime(start_bam)
    #print("Local time:", local_time)	    

    for dat in data:
        print(dat)
        with open(os.path.join(tmppathint,'data_'+dat+'.pkl'),'rb') as  f:data = pickle.load(f)
        with open(os.path.join(tmppathint,'nifs_'+dat+'.pkl'),'rb') as f:nifs = pickle.load(f)
        nifs=nifs.tolist()
        #table          = data[dat].copy()
        table           = data.copy()
        table           = table.fillna(0)
        nifs_dic[dat]   = nifs
        print(f'                Time (min) --> start time: {local_time}')
        bam[dat]                = lbam.bam_generator(table,years,nifs,acctrs,acctcs)
        print(f'                Time (min) --> generation time: {( time.time() - start_bam)/60}')
        bam[dat]                = lbam.bam_completion(bam[dat],years,nifs)
        print(f'                Time (min) --> completion time: {( time.time() - start_bam)/60}')
        bam_dic[dat]            = lbam.bam_dictionaries(bam[dat],years,nifs)
        print(f'                Time (min) --> dictionaries time: {( time.time() - start_bam)/60}')
        col_sums_dic[dat],row_sums_dic[dat],sum_difs_dic[dat]   = lbam.bam_checking(bam_dic[dat],years,nifs_dic[dat])
        print(f'                Time (min) --> checking time: {( time.time() - start_bam)/60}')
        sum_difs_df[dat]        = pd.DataFrame.from_dict(sum_difs_dic[dat], orient = 'index')
        sum_difs_df[dat]        = sum_difs_df[dat].transpose()

        print(f'                Time (min) --> difs df generation time: {( time.time() - start_bam)/60}')
    return bam, bam_dic, col_sums_dic, row_sums_dic, sum_difs_dic, sum_difs_df

    main()