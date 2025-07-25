#prg_1_1_lectura_datos.py

import importlib
import sys
import pandas as pd
import os
import pickle

import time
# from Libs import lib_cmlp as cmlp
# from Libs import lib_bam as lbam
import Libs.lib_bam as lbam

#Path definitions and functions
#------------------------------

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'


# 1.- ETL READ DATA
# =================

# Loading raw data:

def data_etl_1(lista_data):

        data = lista_data       
        
        # 1.- Loading the bam data
        # ========================      
        #print('\n')
        print('     1.- Loading the bam data')

        datos = {}
        nifs  = {}
        for dat in data:
            datos[dat]=lbam.import_bam(dat)

# Renaming columns:

        for dat in data:
                datos[dat].rename(columns={"año": "year"},inplace='True')    
                nifs[dat]  = pd.unique(datos[dat]['nif'])         
        return datos,nifs

# Loading mapping files:

def data_etl_2(lista_maps):

        maps = lista_maps
        map_dic = {}

        for map in maps:
            map_dic[map]=lbam.import_map(map)    
        
        return map_dic

def data_etl_3():
        igic=pd.read_csv(os.path.join(tmppathent,'igic.csv'), delimiter=';', encoding='latin1', decimal=',',
                        dtype={'year'  : int, 
                               'igic'  : float})
        return igic