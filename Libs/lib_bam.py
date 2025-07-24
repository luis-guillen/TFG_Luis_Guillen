# lib_bam.py
# Modulo de funciones para el tratamiento de las BAM

import pandas as pd
import importlib
import numpy as np
import pickle
import os
import time
from Libs import lib_cmlp as cmlp


#Path definitions and functions
#------------------------------
tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'


# Import data
# -----------

def import_bam(dat):

   # total_var = ['id','idsabi','nombre','nif','año',
   #              '11000','11100','11200','11300','11400','11500','11600','11700','12000','12100',
   #              '12200','12210','12220','12230','12240','12250','12260','12300','12400','12500',
   #              '12600','12700','10000','20000','21000','21100','21200','21300','21400','21500',
   #              '21510','21520','21600','21700','21800','21900','22000','23000','31000','31100',
   #              '31200','31300','31400','31500','31600','31700','32000','32100','32200','32300',
   #              '32400','32500','32600','32700','30000','40100','40110','40120','40130','40200',
   #              '40300','40400','40410','40420','40430','40440','40500','40510','40520','40600',
   #              '40610','40620','40630','40700','40710','40720','40730','40740','40750','40800',
   #              '40900','41000','41100','41110','41120','41130','41200','41300','49100','41400',
   #              '41500','41600','41700','41800','41810','41820','42100','42110','42120','42130',
   #              '49200','49300','41900','49400','42000','49400','50010','50020','50030','50040',
   #              '50050','50060','50070','59200','50080','50090','50100','50110','50120','50130',
   #              '59300','59400','52013',
   #              ]
    
   data_files = [f for f in os.listdir(tmppathent) if f.endswith('Sabi_Export_'+dat+'.csv')]

   for file in data_files:
      data=pd.read_csv(os.path.join(tmppathent,file), delimiter=';', encoding='UTF-8', decimal=',',
                        dtype={#'id'     : object, 
                        	    'idsabi' : object,
                               'nombre' : object,
                               'nif'    : object,
                               'año'    : int,
                               '11000'  : float,'11100'  : float,'11200'  : float,'11300'  : float,'11400'  : float,
                               '11500'  : float,'11600'  : float,'11700'  : float,'12000'  : float,'12100'  : float,
                               '12200'  : float,'12210'  : float,'12220'  : float,'12230'  : float,'12240'  : float,
                               '12250'  : float,'12260'  : float,'12300'  : float,'12400'  : float,'12500'  : float,
                               '12600'  : float,'12700'  : float,'10000'  : float,'20000'  : float,'21000'  : float,
                               '21100'  : float,'21200'  : float,'21300'  : float,'21400'  : float,'21500'  : float,
                               '21510'  : float,'21520'  : float,'21600'  : float,'21700'  : float,'21800'  : float,
                               '21900'  : float,'22000'  : float,'23000'  : float,'31000'  : float,'31100'  : float,
                               '31200'  : float,'31300'  : float,'31400'  : float,'31500'  : float,'31600'  : float,
                               '31700'  : float,'32000'  : float,'32100'  : float,'32200'  : float,'32300'  : float,
                               '32400'  : float,'32500'  : float,'32600'  : float,'32700'  : float,'30000'  : float,
                               '40100'  : float,'40110'  : float,'40120'  : float,'40130'  : float,'40200'  : float,
                               '40300'  : float,'40400'  : float,'40410'  : float,'40420'  : float,'40430'  : float,
                               '40440'  : float,'40500'  : float,'40510'  : float,'40520'  : float,'40600'  : float,
                               '40610'  : float,'40620'  : float,'40630'  : float,'40700'  : float,'40710'  : float,
                               '40720'  : float,'40730'  : float,'40740'  : float,'40750'  : float,'40800'  : float,
                               '40900'  : float,'41000'  : float,'41100'  : float,'41110'  : float,'41120'  : float,
                               '41130'  : float,'41200'  : float,'41300'  : float,'49100'  : float,'41400'  : float,
                               '41500'  : float,'41600'  : float,'41700'  : float,'41800'  : float,'41810'  : float,
                               '41820'  : float,'42100'  : float,'42110'  : float,'42120'  : float,'42130'  : float,
                               '49200'  : float,'49300'  : float,'41900'  : float,'49400'  : float,'42000'  : float,
                               '49400'  : float,'50010'  : float,'50020'  : float,'50030'  : float,'50040'  : float,
                               '50050'  : float,'50060'  : float,'50070'  : float,'59200'  : float,'50080'  : float,
                               '50090'  : float,'50100'  : float,'50110'  : float,'50120'  : float,'50130'  : float,
                               '59300'  : float,'59400'  : float,'52013'  : float})
      
# Select variables needed
# -----------------------
#         
      data_var=['nif','año',
            '11000','11100','11200','11300','11400','11500','11600','11700','12000','12100',
            '12200','12210','12220','12230','12240','12250','12260','12300','12400','12500',
            '12600','12700','10000','20000','21000','21100','21200','21300','21400','21500',
            '21510','21520','21600','21700','21800','21900','22000','23000','31000','31100',
            '31200','31300','31400','31500','31600','31700','32000','32100','32200','32300',
            '32400','32500','32600','32700','30000','40100','40110','40120','40130','40200',
            '40300','40400','40410','40420','40430','40440','40500','40510','40520','40600',
            '40610','40620','40630','40700','40710','40720','40730','40740','40750','40800',
            '40900','41000','41100','41110','41120','41130','41200','41300','49100','41400',
            '41500','41600','41700','41800','41810','41820','42100','42110','42120','42130',
            '49200','49300','41900','49400','42000','49400','50010','50020','50030','50040',
            '50050','50060','50070','59200','50080','50090','50100','50110','50120','50130',
            '59300','59400','52013'
            ]

      data = data[data_var].copy()
      #This is to eliminate observations (nifs) for which we do not have information
      #for all the available years; if we do not so the rest of the calculations fail
      data_group = data.groupby('nif').size().to_frame()
      data_group.reset_index(level=0, inplace=True)
      data_group.columns=['nif','count']
      nyears = data['año'].nunique(dropna=True)
      data_group_erase = data_group.drop(data_group["count"].loc[data_group["count"]==nyears].index)
      erase=list(data_group_erase.iloc[:,0].values)
      data = data[~data.nif.isin(erase)]


# Change variable names
# ---------------------

      data_var_dict = {}

      data_var_dict= {'11000':'acc11000','11100':'acc11100','11200':'acc11200','11300':'acc11300','11400':'acc11400','11500':'acc11500','11600':'acc11600','11700':'acc11700','12000':'acc12000','12100':'acc12100',
                   '12200':'acc12200','12210':'acc12210','12220':'acc12220','12230':'acc12230','12240':'acc12240','12250':'acc12250','12260':'acc12260','12300':'acc12300','12400':'acc12400','12500':'acc12500',
                   '12600':'acc12600','12700':'acc12700','10000':'acc10000','20000':'acc20000','21000':'acc21000','21100':'acc21100','21200':'acc21200','21300':'acc21300','21400':'acc21400','21500':'acc21500',
                   '21510':'acc21510','21520':'acc21520','21600':'acc21600','21700':'acc21700','21800':'acc21800','21900':'acc21900','22000':'acc22000','23000':'acc23000','31000':'acc31000','31100':'acc31100',
                   '31200':'acc31200','31300':'acc31300','31400':'acc31400','31500':'acc31500','31600':'acc31600','31700':'acc31700','32000':'acc32000','32100':'acc32100','32200':'acc32200','32300':'acc32300',
                   '32400':'acc32400','32500':'acc32500','32600':'acc32600','32700':'acc32700','30000':'acc30000','40100':'acc40100','40110':'acc40110','40120':'acc40120','40130':'acc40130','40200':'acc40200',
                   '40300':'acc40300','40400':'acc40400','40410':'acc40410','40420':'acc40420','40430':'acc40430','40440':'acc40440','40500':'acc40500','40510':'acc40510','40520':'acc40520','40600':'acc40600',
                   '40610':'acc40610','40620':'acc40620','40630':'acc40630','40700':'acc40700','40710':'acc40710','40720':'acc40720','40730':'acc40730','40740':'acc40740','40750':'acc40750','40800':'acc40800',
                   '40900':'acc40900','41000':'acc41000','41100':'acc41100','41110':'acc41110','41120':'acc41120','41130':'acc41130','41200':'acc41200','41300':'acc41300','49100':'acc49100','41400':'acc41400',
                   '41500':'acc41500','41600':'acc41600','41700':'acc41700','41800':'acc41800','41810':'acc41810','41820':'acc41820','42100':'acc42100','42110':'acc42110','42120':'acc42120','42130':'acc42130',
                   '49200':'acc49200','49300':'acc49300','41900':'acc41900','49400':'acc49400','42000':'acc42000','49400':'acc49400','50010':'acc50010','50020':'acc50020','50030':'acc50030','50040':'acc50040',
                   '50050':'acc50050','50060':'acc50060','50070':'acc50070','59200':'acc59200','50080':'acc50080','50090':'acc50090','50100':'acc50100','50110':'acc50110','50120':'acc50120','50130':'acc50130',
                   '59300':'acc59300','59400':'acc59400','52013':'acc52013'}

      #bam.rename(columns=data_var_dict, errors='raise',inplace='True')
   return data

def import_map(file):
   map_files = [f for f in os.listdir(tmppathent) if f.endswith('map_'+file+'.csv')]


   for file in map_files:
      maps=pd.read_csv(os.path.join(tmppathent,file), delimiter=';', encoding='latin1', decimal='.')
   
   return maps



def build_lookup_cache(table):
    """
    Construye una caché {(year, nif): fila_dict_con_prev} para acceso ultrarrápido.
    """
    cache = {}

    # Agrupa por NIF
    grouped = table.groupby('nif')
    for nif, df in grouped:
        df_sorted = df.sort_values(by='year')
        prev_row = {}
        for _, row in df_sorted.iterrows():
            year = row['year']
            current = row.to_dict()
            for k, v in prev_row.items():
                current[f'{k}_prev'] = v
            cache[(year, nif)] = current
            prev_row = row.to_dict()
    return cache

def lookup_dat(data_cache, year, nif, acctr, acctc):
    """
    Evaluación rápida de fórmulas BAM con caché previa.
    """
    value_final = 0

    formulas = {
        ('01.1.0', '09.1.0'): lambda r: r.get('40200', 0),
        ('01.2.1', '09.1.0'): lambda r: (r.get('12210', 0) - r.get('12210_prev', 0)) +
                                        (r.get('12220', 0) - r.get('12220_prev', 0)),
        ('01.1.0', '09.2.0'): lambda r: r.get('40300', 0),
        ('01.2.2', '09.2.0'): lambda r: (-r.get('40300', 0)
                                         + (r.get('11100', 0) - r.get('11100_prev', 0))
                                         + (r.get('11200', 0) - r.get('11200_prev', 0))
                                         + (r.get('11300', 0) - r.get('11300_prev', 0))
                                         + (r.get('12100', 0) - r.get('12100_prev', 0))
                                         - r.get('40800', 0)
                                         - r.get('41110', 0)),
        ('01.2.3', '02.1.0'): lambda r: r.get('40600', 0),
        ('02.1.0', '01.1.0'): lambda r: r.get('40110', 0) + r.get('40120', 0) + r.get('40200', 0) + r.get('40300', 0) + r.get('40510', 0) + r.get('40520', 0),
        ('04.1.1', '03.1.0'): lambda r: r.get('40900', 0),
        ('04.1.2', '03.1.0'): lambda r: r.get('41000', 0),
        ('04.1.3', '03.1.0'): lambda r: r.get('41110', 0),
        ('04.2.0', '03.1.0'): lambda r: r.get('41120', 0),
        ('05.1.0', '11.1.0'): lambda r: r.get('50210', 0),
        ('06.1.0', '05.1.0'): lambda r: r.get('50400', 0),
        ('07.1.0', '06.1.0'): lambda r: r.get('50500', 0),
        ('07.1.0', '13.1.0'): lambda r: r.get('50510', 0),
        ('08.1.0', '07.1.0'): lambda r: r.get('50600', 0),
        ('08.1.0', '13.1.0'): lambda r: r.get('50610', 0),
        ('08.1.0', '14.1.0'): lambda r: r.get('50620', 0),
        ('08.2.0', '08.1.0'): lambda r: r.get('50700', 0),
        ('08.2.0', '11.1.0'): lambda r: r.get('50710', 0),
        ('08.2.0', '13.1.0'): lambda r: r.get('50720', 0),
        ('08.2.0', '14.1.0'): lambda r: r.get('50730', 0),
        ('09.2.0', '02.1.0'): lambda r: r.get('50800', 0),
        ('10.1.0', '09.3.0'): lambda r: r.get('60100', 0),
        ('10.2.0', '09.3.0'): lambda r: r.get('60200', 0),
        ('11.1.0', '01.2.1'): lambda r: r.get('70100', 0),
        ('11.1.0', '04.1.1'): lambda r: r.get('70110', 0),
        ('11.1.0', '04.1.3'): lambda r: r.get('70120', 0),
        ('11.1.0', '05.1.0'): lambda r: r.get('70130', 0),
        ('11.1.0', '06.1.0'): lambda r: r.get('70140', 0),
        ('11.1.0', '07.1.0'): lambda r: r.get('70150', 0),
        ('11.1.0', '08.2.0'): lambda r: r.get('70160', 0),
        ('12.1.0', '01.1.0'): lambda r: r.get('70200', 0),
        ('12.1.0', '01.2.1'): lambda r: r.get('70210', 0),
        ('12.1.0', '04.1.2'): lambda r: r.get('70220', 0),
        ('12.1.0', '04.2.0'): lambda r: r.get('70230', 0),
        ('12.1.0', '05.1.0'): lambda r: r.get('70240', 0),
        ('12.1.0', '06.1.0'): lambda r: r.get('70250', 0),
        ('13.1.0', '01.2.2'): lambda r: r.get('70300', 0),
        ('13.1.0', '08.2.0'): lambda r: r.get('70310', 0),
        ('14.1.0', '01.2.2'): lambda r: r.get('70400', 0),
        ('14.1.0', '08.2.0'): lambda r: r.get('70410', 0),
    }

    key = (acctr, acctc)
    row = data_cache.get((year, nif), {})
    try:
        value_final = formulas.get(key, lambda _: 0)(row)
    except:
        value_final = 0
    return value_final

    key = (acctr, acctc)
    row = data_cache.get((year, nif), {})
    if key in formulas:
        try:
            value_final = formulas[key](row)
        except:
            value_final = 0
    return value_final

def bam_generator(table, years, nifs, acctrs, acctcs):
    """
    Generador de BAMs optimizado con cache por (year, nif).
    """
    print('\n        1.1.- Generating the Bams (Fast Version)')
    start = time.time()
    data_cache = build_lookup_cache(table)

    rows = []
    for year in years:
        print(f'           Current year: {year}')
        for nif in nifs:
            for acctr in acctrs:
                for acctc in acctcs:
                    val = lookup_dat(data_cache, year, nif, acctr, acctc)
                    rows.append([year, nif, acctr, acctc, val])

    df_result = pd.DataFrame(rows, columns=['year', 'nif', 'acctr', 'acctc', 'value'])
    print(f'                 Elapsed time (sec) --> {time.time() - start:.2f}')
    return df_result

def bam_completion(table, years, nifs):
    print('\n        1.2.- Completing the Bams (Fast Version)')
    start_compl = time.time()

    # Creamos un diccionario {(year, nif, acctr, acctc): value}
    value_map = {
        (row['year'], row['nif'], row['acctr'], row['acctc']): row['value']
        for _, row in table.iterrows()
    }

    completion_formulas = {
        ('03.1.0', '02.1.0'): [('+', '02.1.0', '01.1.0'), ('-', '01.2.3', '02.1.0'), ('-', '09.2.0', '02.1.0')],
        ('04.3.0', '03.1.0'): [('+', '03.1.0', '02.1.0'), ('-', '04.1.1', '03.1.0'), ('-', '04.1.2', '03.1.0'),
                               ('-', '04.1.3', '03.1.0'), ('-', '04.2.0', '03.1.0')],
        ('05.1.0', '04.3.0'): [('+', '04.3.0', '03.1.0')],
        ('06.1.0', '05.1.0'): [('+', '05.1.0', '04.3.0'), ('+', '05.1.0', '11.1.0'), ('-', '11.1.0', '05.1.0'),
                               ('-', '12.1.0', '05.1.0')],
        ('07.1.0', '06.1.0'): [('+', '06.1.0', '05.1.0'), ('-', '11.1.0', '06.1.0'), ('-', '12.1.0', '06.1.0')],
        ('08.1.0', '07.1.0'): [('+', '07.1.0', '06.1.0'), ('+', '07.1.0', '13.1.0'), ('-', '11.1.0', '07.1.0')],
        ('08.2.0', '08.1.0'): [('+', '08.1.0', '07.1.0'), ('+', '08.1.0', '13.1.0'), ('+', '08.1.0', '14.1.0')],
        ('08.3.0', '08.2.0'): [('+', '08.2.0', '08.1.0'), ('+', '08.2.0', '11.1.0'), ('+', '08.2.0', '13.1.0'),
                               ('+', '08.2.0', '14.1.0'), ('-', '11.1.0', '08.2.0'), ('-', '13.1.0', '08.2.0'),
                               ('-', '14.1.0', '08.2.0')],
        ('09.1.0', '08.3.0'): [('+', '01.2.1', '09.1.0')],
        ('09.2.0', '08.3.0'): [('+', '01.1.0', '09.2.0'), ('+', '01.2.2', '09.2.0'), ('-', '09.2.0', '02.1.0')],
        ('09.3.0', '08.3.0'): [('+', '08.3.0', '08.2.0'), ('-', '09.1.0', '08.3.0'), ('-', '09.2.0', '08.3.0'),
                               ('-', '09.3.0', '08.3.0')],
        ('15.1.0', '10.1.0'): [('+', '10.1.0', '09.3.0'), ('-', '09.3.0', '10.1.0')],
        ('15.1.0', '10.2.0'): [('+', '10.2.0', '09.3.0'), ('-', '09.3.0', '10.2.0')],
        ('11.1.0', '15.1.0'): [('+', '01.1.0', '11.1.0'), ('+', '05.1.0', '11.1.0'), ('+', '08.1.0', '11.1.0'),
                               ('-', '11.1.0', '01.2.1'), ('-', '11.1.0', '04.1.1'), ('-', '11.1.0', '04.1.3'),
                               ('-', '11.1.0', '05.1.0'), ('-', '11.1.0', '06.1.0'), ('-', '11.1.0', '07.1.0'),
                               ('-', '11.1.0', '08.2.0')],
        ('12.1.0', '15.1.0'): [('-', '12.1.0', '01.1.0'), ('-', '12.1.0', '01.2.1'), ('-', '12.1.0', '04.1.2'),
                               ('-', '12.1.0', '04.2.0'), ('-', '12.1.0', '05.1.0'), ('-', '12.1.0', '06.1.0')],
        ('13.1.0', '15.1.0'): [('+', '07.1.0', '13.1.0'), ('+', '08.2.0', '13.1.0'), ('-', '13.1.0', '01.2.2'),
                               ('-', '13.1.0', '08.2.0')],
        ('14.1.0', '15.1.0'): [('+', '08.1.0', '14.1.0'), ('+', '08.2.0', '14.1.0'), ('-', '14.1.0', '01.2.2'),
                               ('-', '14.1.0', '08.2.0')],
    }

    for year in years:
        print(f'           Current year: {year}')
        for nif in nifs:
            for (acctr_target, acctc_target), formula in completion_formulas.items():
                total = 0
                for op, acctr_src, acctc_src in formula:
                    val = value_map.get((year, nif, acctr_src, acctc_src), 0)
                    total += val if op == '+' else -val
                # Actualiza valor en el diccionario
                value_map[(year, nif, acctr_target, acctc_target)] = total

    # Reconstruimos el DataFrame final
    final_rows = [
        {'year': year, 'nif': nif, 'acctr': acctr, 'acctc': acctc, 'value': val}
        for (year, nif, acctr, acctc), val in value_map.items()
    ]
    result_df = pd.DataFrame(final_rows)

    print(f'                 Elapsed time (min) --> {(time.time() - start_compl)/60:.2f}')
    return result_df

def bam_dictionaries(table, years, nifs):
    print('\n        1.3.- Generating Bam dictionaries (Fast Version)')

    # Agrupar de antemano por (year, nif)
    grouped = table.groupby(['year', 'nif'])

    # Crear diccionario directamente con el resultado del groupby
    bam_dic = {
        year: {
            nif: grouped.get_group((year, nif)).copy()
            for nif in nifs if (year, nif) in grouped.groups
        }
        for year in years
    }

    for year in years:
        print(f'           Current year: {year}')

    return bam_dic

def bam_checking(table,years,nifs):
   print('\n        1.4.- Checking Bams')
   bam_arrays = {}
   col_sums = {}
   row_sums = {}
   sum_difs = {}
   
   for year in years:
      bam_arrays[year]  = {}
      col_sums[year]    = {}
      row_sums[year]    = {}
      sum_difs[year]     = {}
      print('           Current year: {}'.format(year))
      for nif in nifs:
         bam_arrays[year][nif]   = pd.pivot_table(table[year][nif], values = 'value', index = ['acctr'], columns=['acctc'], aggfunc="sum")
         bam_arrays[year][nif]   = bam_arrays[year][nif].to_numpy()
         col_sums[year][nif]     = bam_arrays[year][nif].sum(axis=0)
         row_sums[year][nif]     = bam_arrays[year][nif].sum(axis=1)
         sum_difs[year][nif]     = np.sum(col_sums[year][nif]-row_sums[year][nif])
   
   return col_sums,row_sums,sum_difs

def crea_bam_dataframe(datos_bam, filas, columnas):
    # Pivotamos directamente el DataFrame
    df = datos_bam.pivot_table(
        index='acctr',
        columns='acctc',
        values='value',
        aggfunc='first'  # o 'sum' si puede haber duplicados
    )
    
    # Nos aseguramos de que tiene todas las filas y columnas que esperamos
    df = df.reindex(index=filas, columns=columnas)
    
    return df