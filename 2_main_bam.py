# coding: utf-8

# Standard library imports
import os
import sys
import argparse
import pickle
import time
import pandas as pd

# Local application imports
import Files.prg_1_1_lectura_datos as prog1
import Files.prg_1_2_bam as prog2
import Libs.lib_bam as lbam
import Libs.lib_cmlp as cmlp

# Paths
tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

data = ['cma_1','cma_2','cma_3','cma_4','cma_5','cma_6','cma_7','cma_8','cma_9','cma_10']
maps = ['carga_datos','data','bam']
default_years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]

dirs = [tmppathent, tmppathint, tmppathres]
for dir in dirs:
    if not os.path.isdir(dir):
        print(f'\n!!!! Carpeta {dir} no existe\n')
        sys.exit(0)

files = [os.path.join(tmppathent, f'Sabi_Export_{dat}.csv') for dat in data]
for file in files:
    if not os.path.isfile(file):
        print(f'\n!!!! Archivo {file} no existe\n')
        sys.exit(0)

# ----------- Nueva función para parsear años desde la consola -----------
def parse_years(year_str):
    years = []
    if not year_str:
        return default_years
    for part in year_str.split(','):
        part = part.strip()
        if '-' in part:
            start, end = map(int, part.split('-'))
            years.extend(range(start, end + 1))
        else:
            years.append(int(part))
    return sorted(set(years))

# ------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ETL BAM - Fase 1")
    parser.add_argument('--years', type=str, help="Años a procesar. Ej: '2012' o '2012,2015,2018' o '2010-2012'")
    args = parser.parse_args()

    years = parse_years(args.years)
    print(f'\nAños seleccionados: {years}')

    start_total = time.time()    
    num_fases = 1

    print(f'\n*** Executing ETL of BAM: preparing the bam - phase 1/{num_fases}:')
    print("Archivos a procesar:", files)

    for dat in data: 
        with open(os.path.join(tmppathint, f'nifs_{dat}.pkl'), 'rb') as f:
            nifs = pickle.load(f)
        nifs = nifs.tolist()

        bam, bam_dic, col_sums_dic, row_sums_dic, sum_difs_dic, sum_difs_df = prog2.bam_etl_1([dat], years, nifs)

        bam[dat].to_csv(os.path.join(tmppathint, f'bam_{dat}.csv'), sep=';', decimal=',', index=False)
        sum_difs_df[dat].to_csv(os.path.join(tmppathint, f'sum_difs_{dat}.csv'), sep=';', decimal=',', index=True)
        with open(os.path.join(tmppathres, f'bam_{dat}.pkl'), 'wb') as f: pickle.dump(bam, f)
        with open(os.path.join(tmppathres, f'bam_dic_{dat}.pkl'), 'wb') as f: pickle.dump(bam_dic, f)
        with open(os.path.join(tmppathint, f'cols_dic_{dat}.pkl'), 'wb') as f: pickle.dump(col_sums_dic, f)
        with open(os.path.join(tmppathint, f'rows_dic_{dat}.pkl'), 'wb') as f: pickle.dump(row_sums_dic, f)
        with open(os.path.join(tmppathint, f'difs_df_{dat}.pkl'), 'wb') as f: pickle.dump(sum_difs_df, f)

        print(f'\n{dat}:')
        print('------')
        print('Sum of row/column differences by year:')
        print(sum_difs_df[dat].sum())

    print(f'\n        Time (min) --> fase2: {(time.time() - start_total)/60:.2f}')
    print(f'\n        Time (min) --> finish time: {time.ctime(time.time())}')

if __name__ == '__main__':
    main()