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
      data=pd.read_csv(os.path.join(tmppathent,file), delimiter=';', encoding='latin1', decimal='.',
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

def lookup_dat(table,year,nif,acctr,acctc):


   value1 = 0
   value2 = 0
   value_final = 0
   
   #Sales of merchandises & production
   if acctr == '02.1.0' and acctc=='01.1.0':
      #40110	a) Sales
      v_40110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40110'].values[0]
      #40120	b) Services provided
      v_40120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40120'].values[0]
      #40200	2. Changes in inventories of finished goods & work in progress
      v_40200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40200'].values[0]
      #40300	3. Work carried out by the company for assets
      v_40300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40300'].values[0]
      #40510	a) Non-trading & other operating income
      v_40510 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40510'].values[0]
      #40520	b) Operating grants taken to income
      v_40520 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40520'].values[0]

      value_final = v_40110 + v_40120 + v_40200 + v_40300 + v_40510 + v_40520


   #Net taxes on sold merchandises, products & services (output tax minus subsidies)
   elif acctr == '12.2.0' and acctc=='01.1.0':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      # igic tax rate
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #40100	1. Revenue
      v_40100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40100'].values[0]
      #40510	a) Non-trading & other operating income
      v_40510 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40510'].values[0]
      #40520	b) Operating grants taken to income
      v_40520 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40520'].values[0]
      
      value_final = v_igic*(v_40100+v_40510) - v_40520


   #Acquisition of goods & services, including financial services (net taxes included)
   elif acctr == '11.1.0' and acctc=='01.2.1':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      # igic tax rate
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #Intermediate consumption:   
      #40410	a) Merchandise used
      v_40410 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40410'].values[0]
      #40420	b) Raw materials & other consumables used
      v_40420 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40420'].values[0]
      #40430	c) Subcontracted work
      v_40430 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40430'].values[0]
      #40440	4.d) Impairment of merchandise, raw materials & other supplies
      v_40440 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40440'].values[0]
      #40710	a) External services
      v_40710 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40710'].values[0]
      #40740	d) Other operating expenses
      v_40740 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40740'].values[0]
      #40750	e) Greenhouse gas emission expenses
      v_40750 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40750'].values[0]
      #12210	1. Goods for resale (changes)
      v_12210 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12210'].values[0]
      #12210	1. Goods for resale (previous year)
      v_12210_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12210'].values[0]
      #12200	2. Raw materials & other supplies (changes)
      v_12220 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12220'].values[0]
      #12200	2. Raw materials & other supplies (previous year)
      v_12220_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12220'].values[0]
      #12220	(-) Changes in II.1. Goods for resale  (balance)

      value_final = - (v_40410 + v_40420 + v_40430 + v_40440 + v_40710 + v_40740 + v_40750 - (v_12210-v_12210_prev) - (v_12220-v_12220_prev)
                    + (v_igic * (v_40410 + v_40420 + v_40430 + v_40440 + v_40710 - (v_12210-v_12210_prev) - (v_12220-v_12220_prev))))


#-Supported and deductible indirect taxes (goods & services)
   elif acctr == '12.2.0' and acctc=='01.2.1':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      # igic tax rate
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #Intermediate consumption:   
      #40410	a) Merchandise used
      v_40410 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40410'].values[0]
      #40420	b) Raw materials & other consumables used
      v_40420 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40420'].values[0]
      #40430	c) Subcontracted work
      v_40430 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40430'].values[0]
      #40440	4.d) Impairment of merchandise, raw materials & other supplies
      v_40440 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40440'].values[0]
      #40710	a) External services
      v_40710 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40710'].values[0]
      #12210	1. Goods for resale (changes)
      v_12210 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12210'].values[0]
      v_12210_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12210'].values[0]
      #12220	2. Raw materials & other supplies (changes)
      v_12220 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12220'].values[0]
      v_12220_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12220'].values[0]
      
      value_final = (v_igic * (v_40410 + v_40420 + v_40430 + v_40440 + v_40710 - (v_12210-v_12210_prev) - (v_12220-v_12220_prev)))


   #-Acquisition of fixed capital goods (net taxes included)
   elif acctr == '13.1.0' and acctc=='01.2.2':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #Acquisitions -  Disposal / derecognition of non-current assest (except financial instruments) (net balance)
      #11100: I. Intangible assets
      v_11100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11100'].values[0]
      v_11100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11100'].values[0]
      #11200	II. Property, plant & equipment
      v_11200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11200'].values[0]
      v_11200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11200'].values[0]
      #11300	III. Investment property
      v_11300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11300'].values[0]
      v_11300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11300'].values[0]
      #12100	I. Non-current assets held for sale
      v_12100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12100'].values[0]
      v_12100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12100'].values[0]
      #40800	8. Amortisation & depreciation
      v_40800 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40800'].values[0]
      #41110	11. Impairment & gains/(losses) on disposal of fixed assets
      v_41110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41110'].values[0]
      #40300	3. Work carried out by the company for assets
      v_40300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40300'].values[0]
      #41120	b) Gains/(losses) on disposal & other
      v_41120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41120'].values[0]

      value_final = (1+v_igic)* ((v_11100-v_11100_prev) + (v_11200-v_11200_prev) + (v_11300-v_11300_prev) + (v_12100-v_12100_prev)
                     - v_40800 - v_41110 - v_40300) + v_igic * v_41120


   #Net taxes on fixed capital goods (output tax minus subsidies & supported and deductible indirect taxes)
   elif acctr == '14.2.0' and acctc=='01.2.2':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #Acquisitions -  Disposal / derecognition of non-current assets (except financial instruments) (net balance)
      #11100: I. Intangible assets
      v_11100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11100'].values[0]
      v_11100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11100'].values[0]
      #11200	II. Property, plant & equipment
      v_11200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11200'].values[0]
      v_11200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11200'].values[0]
      #11300	III. Investment property
      v_11300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11300'].values[0]
      v_11300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11300'].values[0]
      #12100	I. Non-current assets held for sale
      v_12100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12100'].values[0]
      v_12100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12100'].values[0]
      #40800	8. Amortisation & depreciation
      v_40800 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40800'].values[0]
      #41110	11. Impairment & gains/(losses) on disposal of fixed assets
      v_41110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41110'].values[0]
      #40300	3. Work carried out by the company for assets
      v_40300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40300'].values[0]
      #41120	b) Gains/(losses) on disposal & other
      v_41120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41120'].values[0]

      value_final = v_igic * ((v_11100-v_11100_prev) + (v_11200-v_11200_prev) + (v_11300-v_11300_prev) + (v_12100-v_12100_prev)
                     - v_40800 - v_41110 - v_40300)


   #Cost of sold merchandises & Intermediate comsuption
   elif acctr == '01.2.1' and acctc=='01.2.3':
      #40410-00	(-) 4.a) Merchandise used
      v_40410 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40410'].values[0]
      #40420   (-) 4.b) Consumption of raw materials & other consumables
      v_40420 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40420'].values[0]      
      #40430	(-) 4.c) Subcontracted work
      v_40430 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40430'].values[0]      
      #40440	(-) 4.d) Impairment of merchandise, raw materials & other supplies
      v_40440 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40440'].values[0]      
      #40710	(-) 7.a) External servicies
      v_40710 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40710'].values[0]      
      #40740	(-) 7.d) Other operating expenses
      v_40740 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40740'].values[0]      
      #40750	(-) 7.e) Greenhouse gas emission expenses
      v_40750 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40750'].values[0]

      value_final = -(v_40410 + v_40420 + v_40430 + v_40440 + v_40710 + v_40740 + v_40750)


   #Cost of sold merchandises & Intermediate comsuption
   elif acctr == '01.2.3' and acctc=='02.1.0':
      #40410-00	(-) 4.a) Merchandise used
      v_40410 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40410'].values[0]
      #40420   (-) 4.b) Consumption of raw materials & other consumables
      v_40420 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40420'].values[0]      
      #40430	(-) 4.c) Subcontracted work
      v_40430 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40430'].values[0]      
      #40440	(-) 4.d) Impairment of merchandise, raw materials & other supplies
      v_40440 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40440'].values[0]      
      #40710	(-) 7.a) External servicies
      v_40710 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40710'].values[0]      
      #40740	(-) 7.d) Other operating expenses
      v_40740 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40740'].values[0]      
      #40750	(-) 7.e) Greenhouse gas emission expenses
      v_40750 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40750'].values[0]

      value_final = -(v_40410 + v_40420 + v_40430 + v_40440 + v_40710 + v_40740 + v_40750)


   #Fixed capital consumption
   elif acctr == '09.2.0' and acctc=='02.1.0':
      #40800	(-) 8. Amortisation & depreciation
      v_40800 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40800'].values[0]      
      #41110	(+/-) 11.a) Impairment & losses
      v_41110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41110'].values[0]      

      value_final = -(v_40800 + v_41110)


   #Wages and Salaries & Compensations
   elif acctr == '04.1.1' and acctc=='03.1.0':
      #40610	(-) 6.a) Salaries & wages
      v_40610 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40610'].values[0]      

      value_final = -v_40610


   #Employer social security
   elif acctr == '04.1.2' and acctc=='03.1.0':
      #40620	(-) 6.b) Employee benefits expense (hypothesis: all for Social Security)
      v_40620 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40620'].values[0]      

      value_final = -v_40620


   #Social benefits
   elif acctr == '04.1.3' and acctc=='03.1.0':
      #40630	(-) 6.c) Provisions
      v_40630 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40630'].values[0]      

      value_final = -v_40630


   #Other production taxes
   elif acctr == '04.2.0' and acctc=='03.1.0':
      #40720	(-) 7.b) Taxes (hypothesis: on production)
      v_40720 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40720'].values[0]      

      value_final = -v_40720


   #Wages and Salaries & Compensations
   elif acctr == '11.1.0' and acctc=='04.1.1':
      #40610	(-) 6.a) Salaries & wages
      v_40610 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40610'].values[0]      

      value_final = -v_40610


   #Employer Social Security
   elif acctr == '12.1.0' and acctc=='04.1.2':
      #40620	(-) 6.b) Employee benefits expense (hypothesis: all for Social Security)
      v_40620 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40620'].values[0]      

      value_final = -v_40620


   #Social benefits
   elif acctr == '11.1.0' and acctc=='04.1.3':
      #40630	(-) 6.c) Provisions
      v_40630 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40630'].values[0]      

      value_final = -v_40630


   #Other Production Taxes
   elif acctr == '12.1.0' and acctc=='04.2.0':
      #40720	(-) 7.b) Taxes (hypothesis: on production)
      v_40720 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40720'].values[0]      

      value_final = -v_40720


   #Property income paid (taxes included)
   elif acctr == '11.1.0' and acctc=='05.1.0':
      #41500	15. Finance expenses
      v_41500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41500'].values[0]      
      #Supported VAT on property income
      vat_propinc = 0

      value_final = -(v_41500 + vat_propinc)


   # -Supported and deductible indirect taxes (property income)
   elif acctr == '12.1.0' and acctc=='05.1.0':
      #VAT on property income
      vat_propinc = 0
      #Supported VAT on property income
      vat_propinc_sup = 0

      value_final = -(vat_propinc + vat_propinc_sup)


   # Current non-repayable paid transfers
   elif acctr == '11.1.0' and acctc=='06.1.0':
      #41300	13. Other results
      v_41300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41300'].values[0]  

      value_final = -v_41300


   # Current non-repayable paid transfers (included income tax & fines and penalties)
   elif acctr == '12.1.0' and acctc=='06.1.0':
      #41900	(+/-) 20. Income tax
      v_41900 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41900'].values[0]  

      value_final = -v_41900


   # Dividends paid
   elif acctr == '11.1.0' and acctc=='07.1.0':
      #52013	(-) Dividends paid
      v_52013 = table.loc[(table['year']==year)  & (table['nif']==nif)]['52013'].values[0]  

      value_final = -v_52013


   # Adjustments & Operating losses
   elif acctr == '11.1.0' and acctc=='08.2.0':
      #40730	7.c) (+/-) Losses, impairment & changes in trade provisions
      v_40730 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40730'].values[0]  

      value_final = -v_40730


   # Adjustments & Non-operating losses
   elif acctr == '13.1.0' and acctc=='08.2.0':
      #41810	(+/-) 18.a) Impairment (losses & reversal) (in financial instruments non operating)
      v_41810 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41810'].values[0]  
      #50080	(-) VIII. Measurement of financial instruments
      v_50080 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50080'].values[0]  
      #50090	(-) IX. Cash flow hedges
      v_50090 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50090'].values[0]  
      #50110	(-) XI. Non current assets held for sale & associated liabilities
      v_50110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50110'].values[0]  
      #50120	(-) XII. Conversion differences
      v_50120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50120'].values[0]  

      value_final = v_41810 + v_50080 + v_50090 + v_50110 + v_50120


   # Negative tax adjustments
   elif acctr == '14.1.0' and acctc=='08.2.0':
      #50130	(-) XIII. Tax effect (income & expense recognised directly in equity)
      v_50130 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50130'].values[0]  

      value_final = v_50130


   # Discontinued operations, net of income tax
   elif acctr == '09.3.0' and acctc=='08.3.0':
      #42000	21. Profit / (loss) from discontinued operations, net of income tax
      v_42000 = table.loc[(table['year']==year)  & (table['nif']==nif)]['42000'].values[0]  

      value_final = v_42000


   # Change in inventories (production)
   elif acctr == '01.1.0' and acctc=='09.1.0':
      #40200	(+/-) 2. Changes in inventories of finished goods & work in progress
      v_40200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40200'].values[0]  

      value_final = v_40200


   # Change in inventories (goods & services acquired)
   elif acctr == '01.2.1' and acctc=='09.1.0':
      #12210	1. Goods for resale
      v_12210 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12210'].values[0]  
      v_12210_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12210'].values[0]  
      #12220	2. Raw materials & other supplies
      v_12220 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12220'].values[0]  
      v_12220_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12220'].values[0]  

      value_final = (v_12210 - v_12210_prev) + (v_12220 - v_12220_prev)


   #  Gross fixed capital formation (self-buildt) 
   elif acctr == '01.1.0' and acctc=='09.2.0':
      #40300	(+) 3. Work carried out by the company for assets
      v_40300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40300'].values[0]  

      value_final = v_40300


   # Gross fixed capital formation (acquired) 
   elif acctr == '01.2.2' and acctc=='09.2.0':
      #40300	(-) 3 .Work carried out by the company
      v_40300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40300'].values[0]  
      #11100	I. Intangible assets
      v_11100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11100'].values[0]  
      v_11100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11100'].values[0]  
      #11200	II. Property, plant & equipment
      v_11200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11200'].values[0]  
      v_11200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11200'].values[0]  
      #11300	III. Investment property
      v_11300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11300'].values[0]  
      v_11300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11300'].values[0]  
      #12100	I. Non-current assets held for sale
      v_12100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12100'].values[0]  
      v_12100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12100'].values[0]  
      #40800	(-) 8. Amortisation & depreciation
      v_40800 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40800'].values[0]  
      #41110	(+/-) 11.a) Impairment & losses
      v_41110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41110'].values[0]  

      value_final = -v_40300 + (v_11100-v_11100_prev) + (v_11200-v_11200_prev) + (v_11300-v_11300_prev) + (v_12100-v_12100_prev) -  v_40800 - v_41110


   #Change in financial assets (operating)
   elif acctr == '10.1.0' and acctc=='09.3.0':
      #11600	VI. Deferred tax assets
      v_11600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11600'].values[0]  
      v_11600_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11600'].values[0]  
      #11700	VII. Non-current trade & other receivables
      v_11700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11700'].values[0]  
      v_11700_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11700'].values[0]  
      #12260	6. Advances to suppliers
      v_12260 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12260'].values[0]  
      v_12260_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12260'].values[0]  
      #12300	III. Trade & other receivables
      v_12300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12300'].values[0]  
      v_12300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12300'].values[0]  
      #12600	VI. Prepayments for current assets
      v_12600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12600'].values[0]  
      v_12600_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12600'].values[0]  

      value_final = (v_11600-v_11600_prev) + (v_11700-v_11700_prev) + (v_12260-v_12260_prev) + (v_12300-v_12300_prev) + (v_12600-v_12600_prev)


   #Change in financial assets (non-operating)
   elif acctr == '10.2.0' and acctc=='09.3.0':

      #11400	IV. Non-current financial investments in group companies & associates
      v_11400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11400'].values[0]  
      v_11400_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11400'].values[0]  
      #11500	V. Non-current financial investments
      v_11500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['11500'].values[0]  
      v_11500_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['11500'].values[0]  
      #12400	IV. Current investments in group companies & associates
      v_12400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12400'].values[0]  
      v_12400_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12400'].values[0]  
      #12500	V. Current investments
      v_12500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12500'].values[0]  
      v_12500_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12500'].values[0]  
      #12700	VII Cash & cash equivalents
      v_12700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['12700'].values[0]  
      v_12700_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['12700'].values[0]  

      value_final = (v_11400-v_11400_prev) + (v_11500-v_11500_prev) + (v_12400-v_12400_prev) + (v_12500-v_12500_prev) + (v_12700-v_12700_prev)


   # Net borrowing (operating)
   elif acctr == '09.3.0' and acctc=='10.1.0':
      #31100	I. Non-current provisions
      v_31100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31100'].values[0]  
      v_31100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31100'].values[0]  
      #31400	IV. Deferred tax liabilities
      v_31400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31400'].values[0]  
      v_31400_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31400'].values[0]  
      #31500	V. Non-current accruals
      v_31500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31500'].values[0]  
      v_31500_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31500'].values[0]  
      #31600	VI. Non-current trade & other payables
      v_31600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31600'].values[0]  
      v_31600_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31600'].values[0]  
      #32200	II. Current provisions
      v_32200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32200'].values[0]  
      v_32200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32200'].values[0]  
      #32500	V. Trade & other payables
      v_32500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32500'].values[0]  
      v_32500_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32500'].values[0]  
      #32600	VI. Current accruals
      v_32600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32600'].values[0]  
      v_32600_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32600'].values[0]  

      value_final = (v_31100-v_31100_prev) + (v_31400-v_31400_prev) + (v_31500-v_31500_prev) + (v_31600-v_31600_prev) + (v_32200-v_32200_prev) + (v_32500-v_32500_prev) + (v_32600-v_32600_prev)
   

   # Net borrowing (non-operating)
   elif acctr == '09.3.0' and acctc=='10.2.0':
      #31200	II. Non-current payables
      v_31200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31200'].values[0]  
      v_31200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31200'].values[0]  
      #31300	III. Non-current payables, group companies & associates
      v_31300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31300'].values[0]  
      v_31300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31300'].values[0]  
      #31700	VII. Non-current debts with special characteristics
      v_31700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['31700'].values[0]  
      v_31700_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['31700'].values[0]  
      #32100	I. Liabilities associated with non-current assets held for sale
      v_32100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32100'].values[0]  
      v_32100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32100'].values[0]  
      #32300	III. Current payables
      v_32300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32300'].values[0]  
      v_32300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32300'].values[0]  
      #32400	IV. Current payables, group & associated companies
      v_32400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32400'].values[0]  
      v_32400_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32400'].values[0]  
      #32700	VII. Current debts with special characteristics
      v_32700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['32700'].values[0]  
      v_32700_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['32700'].values[0]  

      value_final = (v_31200-v_31200_prev) + (v_31300-v_31300_prev) + (v_31700-v_31700_prev) + (v_32100-v_32100_prev) + (v_32300-v_32300_prev) + (v_32400-v_32400_prev) + (v_32700-v_32700_prev)


   # Sales of merchandises, finished goods & services (net taxes included)
   elif acctr == '01.1.0' and acctc=='11.1.0':
      with open(os.path.join(tmppathint,'igic.pkl'),'rb') as f: igic = pickle.load(f)
      # igic tax rate
      v_igic = igic.loc[(igic['year']==year)]['igic'].values[0]
      #40110	a) Sales
      v_40110 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40110'].values[0]
      #40120	b) Services provided
      v_40120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40120'].values[0]
      #40510	a) Non-trading & other operating income
      v_40510 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40510'].values[0]      
      #40100	1. Revenue
      v_40100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40100'].values[0]
      
      value_final = v_igic*(v_40100+v_40510) + v_40110 + v_40120 + v_40510

   # Property income received (incl. financial investments) (net taxes included)
   elif acctr == '05.1.0' and acctc=='11.1.0':
      #41400	14. Finance income
      v_41400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41400'].values[0]
      #40130	c) Financial income of holding companies
      v_40130 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40130'].values[0]      
      #42100	19. Other finance income & expenses
      v_42100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['42100'].values[0]      
      #Supported VAT on property income
      vat_propinc = 0

      value_final = v_41400 + v_40130 + v_42100 + vat_propinc
      

   # Adjustments & operating gains
   elif acctr == '08.2.0' and acctc=='11.1.0':
      #41700	(+/-) 17. Exchange gains / (losses)
      v_41700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41700'].values[0]

      value_final = v_41700


   # Capital contribution (capital & reserves)
   elif acctr == '07.1.0' and acctc=='13.1.0':
      #21100	I. Capital
      v_21100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21100'].values[0]  
      v_21100_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21100'].values[0]  
      #21400	IV. (Own shares & equity holdings)
      v_21400 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21400'].values[0]  
      v_21400_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21400'].values[0]  
      #21200	II. Share premium
      v_21200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21200'].values[0]  
      v_21200_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21200'].values[0]  
      #21300	III. Reserves
      v_21300 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21300'].values[0]  
      v_21300_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21300'].values[0]  
      #21500	V. Prior periods'  profit & losses
      v_21500 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21500'].values[0]  
      v_21500_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21500'].values[0]  
      #21600	VI. Other equity holder contributions
      v_21600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21600'].values[0]  
      v_21600_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21600'].values[0]  
      #21700	VII. Profit/(loss) for the period
      v_21700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21700'].values[0]  
      v_21700_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21700'].values[0]  
      #21800	VIII. (Interim dividend)
      v_21800 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21800'].values[0]  
      v_21800_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21800'].values[0]  
      #21900	IX. Other equity instruments
      v_21900 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21900'].values[0]  
      v_21900_prev = table.loc[(table['year']==year-1)  & (table['nif']==nif)]['21900'].values[0]  
      #21700	VII. Profit/(loss) for the period
      v_21700 = table.loc[(table['year']==year)  & (table['nif']==nif)]['21700'].values[0]        
      #52013	(-) Dividends paid
      v_52013 = table.loc[(table['year']==year)  & (table['nif']==nif)]['52013'].values[0]  

      value_final = (v_21200-v_21200_prev) + (v_21300-v_21300_prev) + (v_21500-v_21500_prev) + (v_21600-v_21600_prev) + (v_21700-v_21700_prev) + (v_21800-v_21800_prev) + (v_21900-v_21900_prev) + (v_21100-v_21100_prev) + (v_21400-v_21400_prev) - v_21700 -v_52013


   # Adjustments & non-operating gains
   elif acctr == '08.2.0' and acctc=='13.1.0':
      #50010	I. Measurement of financial instruments
      v_50010 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50010'].values[0]        
      #50020	II. Cash flow hedges
      v_50020 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50020'].values[0]  
      #50040	IV. Actuarial gains & losses & other adjustments
      v_50040 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50040'].values[0]  
      #50050	V. Non current assets held for sale & associated liabilities
      v_50050 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50050'].values[0]
      #50060	VI. Conversion differences
      v_50060 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50060'].values[0]  
      #41000	10. Provision surpluses
      v_41000 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41000'].values[0]  
      #41120	b) Gains/(losses) on disposal & other
      v_41120 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41120'].values[0]  
      #41820	b) Gains/(losses) on disposal & other
      v_41820 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41820'].values[0]  
      #41200	12. Negative goodwill on business combinations
      v_41200 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41200'].values[0]        
      #41600	16. Change in fair value of financial instruments
      v_41600 = table.loc[(table['year']==year)  & (table['nif']==nif)]['41600'].values[0]  

      value_final = v_50010 + v_50020 + v_50040 + v_50050 + v_50060 + v_41000  + v_41120 + v_41820 + v_41200 + v_41600


   # Non-current non-repayable transfers received
   elif acctr == '08.1.0' and acctc=='14.1.0':
      #50030	III. Grants, donations & bequests received 
      v_50030 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50030'].values[0]  
      #50100	X. Grants, donations & bequests received
      v_50100 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50100'].values[0]  
      #40900	9. Non-financial & other capital grants
      v_40900 = table.loc[(table['year']==year)  & (table['nif']==nif)]['40900'].values[0]  
           
      value_final = v_50030 + v_50100 + v_40900


   # Positive tax adjustments
   elif acctr == '08.2.0' and acctc=='14.1.0':
      #50130	XIII. Tax effect
      v_50130 = table.loc[(table['year']==year)  & (table['nif']==nif)]['50130'].values[0]        

      value_final = v_50130

   else: value_final = 0
   
   return value_final


def bam_generator(table,years,nifs,acctrs,acctcs):
   print('        1.1.- Generating the Bams')
   empty_list = []
   for year in years:
      #print('\n')
      print('           Current year: {}'.format(year))
      for nif in nifs:
         #print('Current nif: {}'.format(nif))
         for acctr in acctrs:
            for acctc in acctcs:
               empty_list.append([year,nif,acctr,acctc,lookup_dat(table,year,nif,acctr,acctc)])

   result = pd.DataFrame(empty_list,columns=['year','nif','acctr','acctc','value'])	
   return result

def bam_completion(table,years,nifs):
   print('        1.2.- Completing the Bams')
   for year in years:
      print('           Current year: {}'.format(year))
      for nif in nifs:
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='03.1.0') & (table['acctc']=='02.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='02.1.0') & (table['acctc']=='01.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='01.2.3') & (table['acctc']=='02.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.2.0') & (table['acctc']=='02.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.3.0') & (table['acctc']=='03.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='03.1.0') & (table['acctc']=='02.1.0')), ['value']].values[0] -    \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.1.1') & (table['acctc']=='03.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.1.2') & (table['acctc']=='03.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.1.3') & (table['acctc']=='03.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.2.0') & (table['acctc']=='03.1.0')), ['value']].values[0]
         
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='05.1.0') & (table['acctc']=='04.3.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='04.3.0') & (table['acctc']=='03.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='06.1.0') & (table['acctc']=='05.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='05.1.0') & (table['acctc']=='04.3.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='05.1.0') & (table['acctc']=='11.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='05.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='05.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='07.1.0') & (table['acctc']=='06.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='06.1.0') & (table['acctc']=='05.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='06.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='06.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='07.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='07.1.0') & (table['acctc']=='06.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='07.1.0') & (table['acctc']=='13.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='07.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='08.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='07.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='13.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='14.1.0')), ['value']].values[0] 

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.3.0') & (table['acctc']=='08.2.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='08.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='11.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='13.1.0')), ['value']].values[0] +   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='14.1.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='13.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0] -   \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='14.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.1.0') & (table['acctc']=='08.3.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='01.2.1') & (table['acctc']=='09.1.0')), ['value']].values[0]
   
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.2.0') & (table['acctc']=='08.3.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='01.1.0') & (table['acctc']=='09.2.0')), ['value']].values[0]  +  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='01.2.2') & (table['acctc']=='09.2.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.2.0') & (table['acctc']=='02.1.0')), ['value']].values[0]
   
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.3.0') & (table['acctc']=='08.3.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.3.0') & (table['acctc']=='08.2.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.1.0') & (table['acctc']=='08.3.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.2.0') & (table['acctc']=='08.3.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.3.0') & (table['acctc']=='08.3.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='15.1.0') & (table['acctc']=='10.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='10.1.0') & (table['acctc']=='09.3.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.3.0') & (table['acctc']=='10.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='15.1.0') & (table['acctc']=='10.2.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='10.2.0') & (table['acctc']=='09.3.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='09.3.0') & (table['acctc']=='10.2.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='15.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='01.1.0') & (table['acctc']=='11.1.0')), ['value']].values[0]  +  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='05.1.0') & (table['acctc']=='11.1.0')), ['value']].values[0]  +  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='11.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='01.2.1')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='04.1.1')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='04.1.3')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='05.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='06.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='07.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='11.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0]
         
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='15.1.0')), ['value']] =          -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='01.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='01.2.1')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='04.1.2')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='04.2.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='05.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='12.1.0') & (table['acctc']=='06.1.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='13.1.0') & (table['acctc']=='15.1.0')), ['value']] =          -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='07.1.0') & (table['acctc']=='13.1.0')), ['value']].values[0]  +  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='13.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='13.1.0') & (table['acctc']=='01.2.2')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='13.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0]

         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='14.1.0') & (table['acctc']=='15.1.0')), ['value']] =             \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.1.0') & (table['acctc']=='14.1.0')), ['value']].values[0]  +  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='08.2.0') & (table['acctc']=='14.1.0')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='14.1.0') & (table['acctc']=='01.2.2')), ['value']].values[0]  -  \
         table.loc[((table['year']==year)  & (table['nif']==nif) & (table['acctr']=='14.1.0') & (table['acctc']=='08.2.0')), ['value']].values[0]

   return table


def bam_dictionaries(table,years,nifs):
   print('        1.3.- Generating Bam dictionaries')
   bam_dic = {}
   for year in years:
      bam_dic[year] = {}
      print('           Current year: {}'.format(year))
      for nif in nifs:
         bam_dic[year][nif] = table.loc[((table['year']==year)  & (table['nif']==nif))]

   return bam_dic

def bam_checking(table,years,nifs):
   print('        1.4.- Checking Bams')
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
         bam_arrays[year][nif]   = pd.pivot_table(table[year][nif], values = 'value', index = ['acctr'], columns=['acctc'], aggfunc=np.sum)
         bam_arrays[year][nif]   = bam_arrays[year][nif].to_numpy()
         col_sums[year][nif]     = bam_arrays[year][nif].sum(axis=0)
         row_sums[year][nif]     = bam_arrays[year][nif].sum(axis=1)
         sum_difs[year][nif]     = np.sum(col_sums[year][nif]-row_sums[year][nif])
   
   return col_sums,row_sums,sum_difs