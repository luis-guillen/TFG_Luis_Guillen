# coding: utf-8
import os
import sys
import argparse
import pickle
import time
import pandas as pd
from   Files import prg_1_1_lectura_datos
from   Files import prg_1_2_bam

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

#Global settings
#---------------

data = ['tur_1']
maps = ['carga_datos','data','bam']
years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
#years = [2009,2010]

dirs = [tmppathent,tmppathint,tmppathres]

for dir in dirs:
    carpeta_datos = os.path.join(dir)
    if not os.path.isdir(carpeta_datos):
        print('\n!!!! Carpeta {} no existe\n'.format(carpeta_datos))
        sys.exit(0)

for dat in data: 
    files =  [os.path.join(tmppathent,'Sabi_Export_'+dat+'.csv')]

for file in files:
    if not os.path.isfile(file):
            print('\n!!!! Archivo {} no existe\n'.format(file))
            sys.exit(0)



def main():
    start_total = time.time()    
    #número de fases
    num_fases = 2
    
    # realización de la primera fase del etl de la BAM: reading and preparing data
    # ============================================================================

    print('\n*** Executing ETL of BAM: Reading and preparing data -phase 1/{}:'.format(num_fases))
    
    # Reading the data:
    data_dic  = prg_1_1_lectura_datos.data_etl_1(data)

    nifs = {}
    for dat in data: 
        with open(os.path.join(tmppathint,'data_'+dat)+'.pkl','wb') as f: pickle.dump(data_dic, f)
        data_dic[dat].to_csv(os.path.join(tmppathint,'data_'+dat)+'.csv', sep=';', decimal=',',index=False)
#        nifs[dat] = ['A07411499','B38326997','A35457258']
        nifs[dat]  = pd.unique(data_dic[dat]['nif']) 

    # Reading the maps:
    map_dic = prg_1_1_lectura_datos.data_etl_2(maps)
    
    with open(os.path.join(tmppathint,'maps')+'.pkl','wb') as f: pickle.dump(map_dic, f)

    for map in maps: 
        map_dic[map].to_csv(os.path.join(tmppathint,'map_'+map)+'.csv', sep=';', decimal=',',index=False)

    
    

    # Loading igic rates per year:
   
    igic = prg_1_1_lectura_datos.data_etl_3()

    with open(os.path.join(tmppathint,'igic.pkl'),'wb') as f: pickle.dump(igic, f)
    
    print(f'        Time (min) --> fase1: {(time.time() - start_total)/60}')

    # realización de la segunda fase del etl de la BAM: calculating table-1
    # =====================================================================

    print('\n*** Executing ETL of BAM: preparing table-1 -phase 2/{}:'.format(num_fases))
   
    # bam is a dictionary where only dat (e.g.: tur_1) is the key
    # bam_dic has the same information as bam but the keys are now dat, year and nif
    # col_sums_dic is a dictionary with the colum totals of each bam
    # row_sums_dic is a dictionary with the row totals of each bam
    # sum_difs_dic presents tha dictionary with the sum of the cl and row differences
    # sum_dif_df presents these results in a df that is exported to csv.

    bam, bam_dic, col_sums_dic, row_sums_dic, sum_difs_dic, sum_difs_df= prg_1_2_bam.bam_etl_1(data,years,nifs)

    for dat in data: 
        bam[dat].to_csv(os.path.join(tmppathint,'bam_'+dat)+'.csv', sep=';', decimal=',',index=False)
        sum_difs_df[dat].to_csv(os.path.join(tmppathint,'sum_difs_'+dat)+'.csv', sep=';', decimal=',',index=True)
        with open(os.path.join(tmppathint,'bam_dic_'+dat)+'.pkl','wb') as f: pickle.dump(bam_dic, f)
        with open(os.path.join(tmppathint,'cols_dic_'+dat)+'.pkl','wb') as f: pickle.dump(col_sums_dic, f)
        with open(os.path.join(tmppathint,'rows_dic_'+dat)+'.pkl','wb') as f: pickle.dump(row_sums_dic, f)
        with open(os.path.join(tmppathint,'difs_df_'+dat)+'.pkl','wb') as f: pickle.dump(sum_difs_df, f)

    print(f'        Time (min)  --> fase2: {(time.time() - start_total)/60}')
    #print(f'Total Time: {time.time() - start_total}')

main()
