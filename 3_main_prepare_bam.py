# coding: utf-8

# Standard library imports
import os
import sys
import argparse
import pickle
import time
import pandas as pd
import numpy as np

# Local application imports
# from   Files import prg_1_1_lectura_datos
# from   Files import prg_1_2_bam
# from   Libs import lib_bam as lbam

import Files.prg_1_1_lectura_datos as prog1
import Files.prg_1_2_bam as prog2
import Files.prg_1_3_prepare_bam as prog3
import Libs.lib_bam as lbam

# Third party importspip
#from Libs import lib_cmlp as cmlp
import Libs.lib_cmlp as cmlp

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'
tmppathbam = r'Data/bam_2'

#Global settings
#---------------

default_data = ['cma_1','cma_2','cma_3','cma_4','cma_5','cma_6','cma_7','cma_8','cma_9','cma_10']
default_years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
#default_data = ['cma_10']
#default_data = ['con_1']
#default_data = ['tur_1','tur_2','cmi_1','cmi_2','cmi_3']
#default_data = ['pru_1','con1','tur_1','tur_2','cmi_1','cmi_2','cmi_3']

dirs = [tmppathent,tmppathint,tmppathres]

for dir in dirs:
    carpeta_datos = os.path.join(dir)
    if not os.path.isdir(carpeta_datos):
        print('\n!!!! Carpeta {} no existe\n'.format(carpeta_datos))
        sys.exit(0)

# función para parsear empresas desde consola
def parse_data(data_str):
    if not data_str:
        return default_data
    return [d.strip() for d in data_str.split(',')]

# función para parsear años desde consola
def parse_years(year_str):
    if not year_str:
        return default_years
    years = []
    for part in year_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = map(int, part.split('-'))
            years.extend(range(start, end + 1))
        else:
            years.append(int(part))
    return sorted(set(years))


def main():
    # argumentos de consola
    parser = argparse.ArgumentParser(description="ETL BAM - Preparar BAM para ML")
    parser.add_argument('--data', type=str, help="Empresas a procesar. Ej: 'cma_1,cma_4,cma_9'")
    parser.add_argument('--years', type=str, help="Años a procesar. Ej: '2010' o '2011,2013' o '2012-2015'")
    args = parser.parse_args()

    data = parse_data(args.data)
    years = parse_years(args.years)

    for dat in data: 
        files =  [os.path.join(tmppathent,'Sabi_Export_'+dat+'.csv')]
        for file in files:
            if not os.path.isfile(file):
                print('\n!!!! Archivo {} no existe\n'.format(file))
                sys.exit(0)

    start_total = time.time()    
    #número de fases
    num_fases = 1
    
# Preparación de las bam para aplicar modelos de ML
# =================================================

    print('\n*** Executing ETL of BAM: preparing the bam for ML-phase 1/{}:'.format(num_fases))
    print('Empresas seleccionadas:', data)
    print('Años seleccionados:', years)
       
    prog3.bam_etl_3(data)
    
    print()
    print(f'\n        Time (min) --> dat: {(time.time() - start_total)/60}')
    
    finish_prepare_bam = time.time()
    local_time = time.ctime(finish_prepare_bam)
    print(f'\n        Time (min) --> finish time: {local_time}')
    
main()