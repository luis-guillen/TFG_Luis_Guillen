# coding: utf-8

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

#Global settings
#---------------

#data = ['pru_1','tur_1','tur_2','con_1','cmi_1','cmi_2','cmi_3']
#data = ['cma_1','cma_2','cma_3','cma_4','cma_5','cma_6','cma_7','cma_8','cma_9','cma_10','cma_11']
data = ['cma_11']
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



# for file in files:
#    if not os.path.isfile(file):
        #    print('\n!!!! Archivo {} no existe\n'.format(file))
        #    sys.exit(0)



def main():
    start_total = time.time()    
    #número de fases
    num_fases = 1
    
    # realización de la primera fase del etl de la BAM: reading and preparing data
    # ============================================================================

    print('\n*** Executing ETL of BAM: Reading and preparing data -phase 1/{}:'.format(num_fases))
    
    # Reading the data:
    data_dic,nifs  = prog1.data_etl_1(data)
    
    for dat in data:
        with open(os.path.join(tmppathint,'data_'+dat)+'.pkl','wb') as f: pickle.dump(data_dic[dat], f)
        data_dic[dat].to_csv(os.path.join(tmppathint,'data_'+dat)+'.csv', sep=';', decimal=',',index=False)
#       nifs[dat] = ['A07411499','B38326997','A35457258']
        with open(os.path.join(tmppathint,'nifs_'+dat)+'.pkl','wb') as f: pickle.dump(nifs[dat], f)
        pd.DataFrame(nifs[dat]).to_csv(os.path.join(tmppathint,'nifs_'+dat)+'.csv', sep=';', decimal=',',index=False)

    # Reading the maps:
    map_dic = prog1.data_etl_2(maps)
    
    with open(os.path.join(tmppathint,'maps')+'.pkl','wb') as f: pickle.dump(map_dic, f)

    for map in maps: 
        pd.DataFrame(map_dic[map]).to_csv(os.path.join(tmppathint,'map_'+map)+'.csv', sep=';', decimal=',',index=False)

    # Loading igic rates per year:
   
    igic = prog1.data_etl_3()

    with open(os.path.join(tmppathint,'igic.pkl'),'wb') as f: pickle.dump(igic, f)
    
    print(f'        Time (min) --> fase1: {(time.time() - start_total)/60}')

main()