import importlib
import pandas as pd
import os
import pickle
import sys
# import numpy as np
import time
from Libs import lib_cmlp as cmlp
from Libs import lib_bam  as lbam

#Path definitions and functions
#------------------------------

tmppathent = r'Data/ent'
tmppathint = r'Data/int'
tmppathres = r'Data/res'

with open(os.path.join(tmppathint,'data_tur_1.pkl'),'rb') as f:data = pickle.load(f)


table=data['tur_1'].copy()

#print(table.head())

years = pd.unique(table['year'])   
nifs  = pd.unique(table['nif']) 

#print(years)
#print(nifs)

# Labels
acctsr=['01.1.1',
        '01.2.1',
        '01.2.2',
        '01.3.2',
        '02.2.0',
        '03.1.0',
        '04.1.1',
        '04.1.3',
        '04.1.4',
        '04.2.0',
        '04.3.0',
        '05.1.0',
        '06.1.0',
        '07.1.0',
        '08.1.0',
        '08.2.0',
        '08.3.0',
        '09.1.0',
        '09.2.0',
        '09.3.0',
        '09.4.0',
        '10.1.0',
        '10.2.0',
        '11.1.0',
        '12.2.0',
        '13.1.0',
        '14.2.0',
        '15.1.0',
        '16.1.0']

acctsc=acctsr.copy()

def lookup_dat(table,year,nif,acctr,acctc):

    value1 = 0
    value2 = 0
    value_final = 0

    if acctr == '01.1.1' and acctc=='01.1.1':
        value1 = table.loc[(table['year']==year) & (table['nif']==nif)]['11000'].values[0]
        value2 = table.loc[(table['year']==year) & (table['nif']==nif)]['40400'].values[0]*0
        value_final = value1 + value2






        
    return value_final

def bam_generator(table,years,nifs,acctsr,acctsc):
    empty_list = []
    for year in years:
        for nif in nifs:
            for acctr in acctsr:
                for acctc in acctsc:
                    empty_list.append([year,nif,acctr,acctc,lookup_dat(table,year,nif,acctr,acctc)])

    result = pd.DataFrame(empty_list,columns=['year','nif','acctr','acctc','value'])	
    return result


a=table.loc[(table['year']=='2018') & (table['nif']=='B38326997')]['11000'].tolist()
print(type(a))
print(len(a))
print(a)
bam = bam_generator(table,years,nifs,acctsr,acctsc)
#print(bam.dtypes)
print(bam.iloc[1:450, 0:6])
#print(years)
#print(nifs)
#print(table.head())
print(bam.head())
#print(bam)
#bam1 = bam.loc[(bam["acctr"] == '01.1.1') & (bam["acctc"] == '01.1.1')]
#print(bam1.shape)
#bam1.to_csv(os.path.join(tmppathint,'bam1')+'.csv', sep=';', decimal=',',index=False)
#print(years)
#print(table.loc[(table['year']=='2018') & (table['nif']=='B96199807')]['11000'].values[0])
bam.to_csv(os.path.join(tmppathint,'bam')+'.csv', sep=';', decimal=',',index=False)