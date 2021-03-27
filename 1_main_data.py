# coding: utf-8
import os
import sys
import argparse
import pickle
from   Files import prg_1_1_lectura_datos
from   Files import prg_1_2_bam

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

#Global settings
#---------------

data = ['tur_1']
maps = ['carga_datos','data','bam']

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
    
    #número de fases
    num_fases = 2
    
    # realización de la primera fase del etl de la BAM: reading and preparing data
    # ============================================================================

    print('\n*** Executing ETL of BAM: Reading and preparing data -phase 1/{}:'.format(num_fases))
    
    # Reading the data:
    data_dic  = prg_1_1_lectura_datos.data_etl_1(data)

    for dat in data: 
        with open(os.path.join(tmppathint,'data_'+dat)+'.pkl','wb') as f: pickle.dump(data_dic, f)
        data_dic[dat].to_csv(os.path.join(tmppathint,'data_'+dat)+'.csv', sep=';', decimal=',',index=False)

    # Reading the maps:
    map_dic = prg_1_1_lectura_datos.data_etl_2(maps)
    
    with open(os.path.join(tmppathint,'maps')+'.pkl','wb') as f: pickle.dump(map_dic, f)

    for map in maps: 
        map_dic[map].to_csv(os.path.join(tmppathint,'map_'+map)+'.csv', sep=';', decimal=',',index=False)


    # Loading igic rates per year:
   
    igic = prg_1_1_lectura_datos.data_etl_3()

    with open(os.path.join(tmppathint,'igic.pkl'),'wb') as f: pickle.dump(igic, f)
    

    # realización de la segunda fase del etl de la BAM: calculating table-1
    # =====================================================================

    print('\n*** Executing ETL of BAM: preparing table-1 -phase 2/{}:'.format(num_fases))
   
    bam= prg_1_2_bam.bam_etl_1(data)
    
    for dat in data: 
        #print(bam[dat].dtypes)
        #print(bam[dat].head())
        #print(bam[dat].iloc[450:480, 1:5])
        #bam[dat].to_csv(os.path.join(tmppathint,'bam_'+dat)+'.csv', sep=';', decimal=',',index=False)
        #bam1 = bam[dat].loc[(bam[dat]['nif'] == 'A07411499') & (bam[dat]["year"] == '2010')]
        #print(bam1.iloc[0:50,0:6])
        bam[dat].to_csv(os.path.join(tmppathint,'bam1_'+dat)+'.csv', sep=';', decimal=',',index=False)
main()
