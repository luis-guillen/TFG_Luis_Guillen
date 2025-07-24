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
import numpy as np
from collections import defaultdict
from joblib import Parallel, delayed

import Libs.lib_bam as lbam

# Third party imports
import Libs.lib_cmlp as cmlp

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'
#tmppathbam = r'Data/bam_1'
tmppathbam = r'Data/bam_2'

    # realización de la tercera fase del etl de la BAM: preparing bams for ML
    # =======================================================================
    
start_prepare_bam = time.time()
local_time = time.ctime(start_prepare_bam)


def bam_etl_3(lista_data):
    start_total = time.time()
    data = lista_data

    for dat in data:
        with open(os.path.join(tmppathres, f'bam_{dat}.pkl'), 'rb') as f:
            bam = pickle.load(f)[dat].copy()

        print(f'\n**** Procesando {dat} con {len(bam["nif"].unique())} empresas\n')

        acctr_unique = bam['acctr'].unique()
        acctc_unique = bam['acctc'].unique()

        # Agrupar por NIF y año
        grouped = bam.groupby(['nif', 'year'])

        # Crear estructura para almacenar los resultados por empresa
        empresas_bam = defaultdict(dict)

        def procesar_grupo(nif, year, grupo):
            df = lbam.crea_bam_dataframe(grupo.drop(columns=['nif', 'year']), acctr_unique, acctc_unique)
            return (nif, year, df)

        print("Creando BAMs...")

        resultados = Parallel(n_jobs=-1)(
            delayed(procesar_grupo)(nif, year, grupo)
            for (nif, year), grupo in grouped
        )

        # Organizar resultados
        for nif, year, df in resultados:
            empresas_bam[nif][year] = df

        # Guardar resultados
        for nif, bam_nif in empresas_bam.items():
            outfilename = os.path.join(tmppathbam, f'{nif}.pkl')
            with open(outfilename, 'wb') as f:
                pickle.dump(bam_nif, f)

        print(f'\n{dat} procesado en {(time.time() - start_total)/60:.2f} minutos\n')