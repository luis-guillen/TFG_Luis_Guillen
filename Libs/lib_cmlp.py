# coding: utf8

import pandas as pd
import numpy as np
import os

# 1.- Showing the basic info of a data frame

def basic_info(a):
    print("\n", "First 3 rows of the dataset", "\n", "=================================")
    print(a[:3])
    print("\n", "data frame shape", "\n", "=================================")
    print(a.shape)
    print("\n", "data frame columns", "\n", "=================================")
    print(a.columns)
    print("\n", "data frame info()", "\n", "=================================")
    print(a.info())
    print("\n", "data frame count()", "\n", "=================================")
    print(a.count())
    print("")

# 2.- Preparing a table for the zero values of a data frame

def missing_zero_values(df):
    zero_val = (df == 0.00).astype(int).sum(axis=0)
    mis_val = df.isnull().sum()
    mis_val_percent = 100 * df.isnull().sum() / len(df)
    mz_table = pd.concat([zero_val, mis_val, mis_val_percent], axis=1)
    mz_table = mz_table.rename(
    columns = {0 : 'Zero Values', 1 : 'Missing Values', 2 : '% of Total Values'})
    mz_table['Total Zero Missing Values'] = mz_table['Zero Values'] + mz_table['Missing Values']
    mz_table['% Total Zero Missing Values'] = 100 * mz_table['Total Zero Missing Values'] / len(df)
    mz_table['Data Type'] = df.dtypes
    mz_table = mz_table[
        mz_table.iloc[:,1] != 0].sort_values(
    '% of Total Values', ascending=False).round(1)
    print ("Your selected dataframe has " + str(df.shape[1]) + " columns and " + str(df.shape[0]) + " Rows.\n"      
        "There are " + str(mz_table.shape[0]) +
        " columns that have missing values.")
    #         mz_table.to_excel('D:/sampledata/missing_and_zero_values.xlsx', freeze_panes=(1,0), index = False)
    return mz_table

# 3.- Search for column duplicate names
def getDuplicateColumns(df):
    '''
    Get a list of duplicate columns.
    It will iterate over all the columns in dataframe and find the columns whose contents are duplicate.
    :param df: Dataframe object
    :return: List of columns whose contents are duplicates.
    '''
    duplicateColumnNames = set()
    # Iterate over all the columns in dataframe
    for x in range(df.shape[1]):
        # Select column at xth index.
        col = df.iloc[:, x]
        # Iterate over all the columns in DataFrame from (x+1)th index till end
        for y in range(x + 1, df.shape[1]):
            # Select column at yth index.
            otherCol = df.iloc[:, y]
            # Check if two columns at x 7 y index are equal
            if col.equals(otherCol):
                duplicateColumnNames.add(df.columns.values[y])
    return list(duplicateColumnNames)

# 4.- Weighted perentile function using numpy
def weight_array(ar, weights):
     zipped = zip(ar, weights)
     weighted = []
     for i in zipped:
         for j in range(i[1]):
             weighted.append(i[0])
     return weighted

# 5.- Calculate percentiles using weights (negative values dropped)

def percentiles(file,value,weight,tiles):
    ar = file[value]
    ar = ar.to_numpy()
    ar = pd.Series(ar[:,0])
    w  = file[weight]
    w = w.to_numpy()
    w = np.round(w,0)
    w  = pd.Series(w[:,0])
    final = ar.repeat(w)
    final = final.to_numpy(copy=True)
    final = final[final>=0]
    final = pd.Series(final)

    perc = []
    for tile in tiles:
        a=np.percentile(final, tile)
        a=pd.Series(a)
        perc = np.append(perc,a, axis = 0)
    return perc