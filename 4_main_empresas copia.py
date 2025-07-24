import pickle
import pandas as pd
import numpy as np

bam_filename = "/Users/luisguillen/Downloads/210309-BAM_OG/Data/bam_2/B66406935.pkl"

with open(bam_filename, "rb") as f:
    bam = pickle.load(f)

print()
print('**** tipo de datos : {}'.format(type(bam)))
print()
print('**** Información dataframe')
print()
print(bam.info())

years = bam['year']
print('años: {}'.format(years.unique()))

nifs = bam['nif']
print('núm. empresas: {}'.format(len(nifs.unique())))

acctr = bam['acctr']
print('acctr: {}'.format(acctr.unique()))

acctc = bam['acctc']
print('acctc: {}'.format(acctc.unique()))

def crea_bam_dataframe(datos_bam, filas, columnas):
    df = pd.DataFrame(columns=columnas, index=filas)
    for cuenta_fila in filas:
        for cuenta_columna in columnas:
            tmp = datos_bam[(datos_bam['acctr'] == cuenta_fila) & (datos_bam['acctc'] == cuenta_columna)]['value'].iloc[0]
            # print("tmp {}".format(type(tmp)))
            # print('{} - {}: \n{}'.format(cuenta_fila, cuenta_columna, tmp))
            df.loc[cuenta_fila][cuenta_columna] = tmp

    return df

for (i,un_nif) in enumerate(nifs.unique()):
    if i % 50 == 0:
        print('procesando empresa {} de {}'.format(i,len(nifs.unique())))
    datos_empresa = bam[bam['nif'] == un_nif]
    bam_nif = {}
    for un_año in years.unique():
        datos_empresa_año = datos_empresa[datos_empresa['year'] == un_año]
        df = crea_bam_dataframe(datos_empresa_año.drop(columns=['year', 'nif']), acctr.unique(), acctc.unique())
        # print(df)
        print(df.to_numpy().sum())
        bam_nif[un_año] = df
    outfilename = 'bam-empresas/{}.pkl'.format(un_nif)
    print(outfilename)
    with open(outfilename, 'wb') as f:
        pickle.dump(bam_nif, f)
