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
from openpyxl import load_workbook
import shutil
import logging
from pathlib import Path

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

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proceso_excel.log'),
        logging.StreamHandler()
    ]
)

def bam_etl_3(lista_data):
    start_total = time.time()  
    data  = lista_data  

# Read data file:

    for dat in data:

        with open(os.path.join(tmppathres,'bam_'+dat+'.pkl'),'rb') as  f:bam = pickle.load(f)
        bam=bam[dat].copy()
        print()
        print('**** Data types : {}'.format(type(bam)))
        print()
        print('**** Dataframe information')
        print()
        print(bam.info())
        
        print()
        years = bam['year']
        print('years: {}'.format(years.unique()))

        print()
        nifs = bam['nif']
        print('number of companies: {}'.format(len(nifs.unique())))

        print()
        acctr = bam['acctr']
        print('acctr: {}'.format(acctr.unique()))

        print()
        acctc = bam['acctc']
        print('acctc: {}'.format(acctc.unique()))
        
        print()
        print(dat+':')
        print('------')

        for (i,one_nif) in enumerate(nifs.unique()):

            if (i % 50) == 0:
                print('procesando empresa {} de {}'.format(i,len(nifs.unique())))

            datos_empresa = bam[bam['nif'] == one_nif]
            bam_nif = {}
            for one_year in years.unique():
                datos_empresa_año = datos_empresa[datos_empresa['year'] == one_year]
                df = lbam.crea_bam_dataframe(datos_empresa_año.drop(columns=['year', 'nif']), acctr.unique(), acctc.unique())
                # print(df)
                # print(df.to_numpy().sum())
                bam_nif[one_year] = df
            outfilename = '{}/{}.pkl'.format(tmppathbam,one_nif)
            #print(outfilename)
            with open(outfilename, 'wb') as f: pickle.dump(bam_nif, f)

        print()
        print(dat+':')
        print('------') 

def group_by_company(df, headers_template):
    """
    Agrupa los datos por empresa y añade los encabezados estandarizados.
    
    Args:
        df (pd.DataFrame): DataFrame formateado
        headers_template (str): Ruta al archivo con los encabezados estandarizados
        
    Returns:
        pd.DataFrame: DataFrame agrupado por empresa
    """
    try:
        # Cargar los encabezados estandarizados
        headers_df = pd.read_excel(headers_template)
        headers_rows = headers_df.iloc[0:3].copy()
        
        # Ordenar por Nombre y luego por año
        df_sorted = df.sort_values(by=["Nombre", "year"])
        
        # Concatenar los encabezados con los datos ordenados
        grouped_df = pd.concat([headers_rows, df_sorted], ignore_index=True)
        
        return grouped_df
        
    except Exception as e:
        logging.error(f"Error al agrupar datos por empresa: {str(e)}")
        return None

def transfer_to_bam_template(df, template_file, company_cif, company_name):
    """
    Transfiere los datos de una empresa a la plantilla BAM.
    
    Args:
        df (pd.DataFrame): DataFrame con los datos de la empresa
        template_file (str): Ruta al archivo de plantilla BAM
        company_cif (str): CIF de la empresa
        company_name (str): Nombre de la empresa
        
    Returns:
        bool: True si el proceso fue exitoso, False en caso contrario
    """
    try:
        # Filtrar los datos de la empresa
        company_data = df[df["Código CIF"] == company_cif]
        
        if company_data.empty:
            logging.error(f"No se encontraron datos para la empresa {company_name} (CIF: {company_cif})")
            return False
        
        # Seleccionar solo las columnas de datos contables (desde la columna H)
        accounting_data = company_data.iloc[:, 7:]
        
        # Crear el archivo BAM para la empresa
        output_file = f"BAM_{company_cif} ({company_name}).xlsx"
        shutil.copy2(template_file, output_file)
        
        # Cargar la plantilla BAM
        with pd.ExcelWriter(output_file, engine='openpyxl', mode='a') as writer:
            # Transponer los datos y guardarlos en la hoja "Carga_datos"
            accounting_data.T.to_excel(writer, sheet_name="Carga_datos", startrow=1, startcol=3, header=False, index=False)
        
        logging.info(f"Datos transferidos a la plantilla BAM para {company_name} (CIF: {company_cif})")
        return True
        
    except Exception as e:
        logging.error(f"Error al transferir datos a la plantilla BAM: {str(e)}")
        return False

def main():
    # Directorio base del proyecto
    BASE_DIR = Path(__file__).parent.parent
    
    # Directorios y archivos
    input_dir = BASE_DIR / "Data" / "modificados"
    input_file = input_dir / "Sabi_Export_2008_2023_hoteles_paso_02.xlsx"
    output_file = input_dir / "Sabi_Export_2008_2023_hoteles_paso_03.xlsx"
    headers_template = input_dir / "0_Plantilla_encabezados_estandarizados.xlsx"
    bam_template = input_dir / "1_Medium_BAM_plantilla_10_años_copia.xlsx"
    
    # Cargar el DataFrame formateado
    try:
        df = pd.read_excel(input_file, sheet_name="Resultados")
    except Exception as e:
        logging.error(f"Error al cargar el archivo formateado: {str(e)}")
        return False
    
    # Agrupar datos por empresa
    grouped_df = group_by_company(df, headers_template)
    
    if grouped_df is not None:
        # Guardar el DataFrame agrupado
        grouped_df.to_excel(output_file, sheet_name="Resultados", index=False)
        logging.info(f"Archivo agrupado guardado en: {output_file}")
        
        # Transferir datos a la plantilla BAM para cada empresa
        companies = df[["Código CIF", "Nombre"]].drop_duplicates()
        
        for _, company in companies.iterrows():
            if not transfer_to_bam_template(df, bam_template, company["Código CIF"], company["Nombre"]):
                logging.error(f"Error al transferir datos para la empresa {company['Nombre']}")
                return False
        
        return True
    else:
        logging.error("Error al agrupar los datos")
        return False

if __name__ == "__main__":
    if main():
        logging.info("Proceso de preparación BAM completado exitosamente")
    else:
        logging.error("Hubo errores durante el proceso de preparación BAM") 