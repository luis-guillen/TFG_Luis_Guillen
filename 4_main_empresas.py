import pickle
import pandas as pd

bam_filename = "/Users/luisguillen/Downloads/210309-BAM_OG/Data/bam_2/B66406935.pkl"

with open(bam_filename, "rb") as f:
    bam = pickle.load(f)

print()
print('**** tipo de datos : {}'.format(type(bam)))
print('Claves del diccionario (años):', list(bam.keys()))

# Elige el código de cuenta que quieres analizar
codigo_cuenta = '01.1.0'  # Cambia esto por el código que te interese

valores_por_año = {}

for año, df in bam.items():
    if isinstance(df, pd.DataFrame):
        # Busca el valor en la diagonal (cuenta contra sí misma)
        if codigo_cuenta in df.index and codigo_cuenta in df.columns:
            valor = df.loc[codigo_cuenta, codigo_cuenta]
            valores_por_año[año] = valor
        else:
            valores_por_año[año] = None  # No existe ese código en ese año

print(f"\nEvolución de la cuenta '{codigo_cuenta}' a lo largo de los años:")
for año in sorted(valores_por_año.keys()):
    print(f"Año {año}: {valores_por_año[año]}")