import pandas as pd
import os
import openpyxl

# CONFIGURACIÓN
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

start_year = 2008
end_year = 2023
base_dir = 'Data/modificados'
plantilla_path = os.path.join(base_dir, '0_Plantilla_encabezados_estandarizados.xlsx')
plantilla_destino_path = os.path.join(base_dir, '1_Medium_BAM_plantilla_10_años_copia.xlsx')
output_path = os.path.join(base_dir, 'plantilla_actualizada.xlsx')
output_path2 = os.path.join(base_dir, 'datos_consolidados_con_plantilla.xlsx')


# FUNCIONES AUXILIARES
def add_year_column(df, year):
    nif_col_idx = df.columns.get_loc('Código NIF')
    df.insert(nif_col_idx + 1, 'year', year)
    return df

def clean_nd_values(df):
    return df.replace('n.d.', '')

def print_dataset_summary(df):
    print("\n" + "="*80)
    print("RESUMEN DEL DATASET FINAL")
    print("="*80)
    print(f"\n- Filas totales: {len(df):,}")
    print(f"- Empresas únicas: {df['Código NIF'].nunique():,}")
    print(f"- Años cubiertos: {sorted(df['year'].unique())}")
    print(f"- Columnas: {len(df.columns)}\n")

# PROCESO DE CONSOLIDACIÓN
dfs = []
columns_2008 = None

print(f"Iniciando consolidación de datos BAM ({start_year}-{end_year})...")

for year in range(start_year, end_year + 1):
    file_path = os.path.join(base_dir, f"SABI_Export_{year}.xlsx")
    if not os.path.exists(file_path):
        print(f"Advertencia: No encontrado {file_path}")
        continue

    print(f"Procesando {year}: {file_path}")
    if year == 2008:
        df = pd.read_excel(file_path, sheet_name="Resultados")
        columns_2008 = df.columns.tolist()
    else:
        df = pd.read_excel(file_path, sheet_name="Resultados", header=0).iloc[1:].reset_index(drop=True)
        df.columns = columns_2008

    df = add_year_column(df, year)
    dfs.append(df)

if not dfs:
    print("No se encontraron archivos válidos. Saliendo...")
    exit()

consolidated_df = pd.concat(dfs, ignore_index=True)
formatted_df = clean_nd_values(consolidated_df)
grouped_df = formatted_df.sort_values(by=["Nombre", "year"])
grouped_df = grouped_df.iloc[:, :-4]  # Eliminar últimas 4 columnas si sobran

print_dataset_summary(grouped_df)

# UNIR CON PLANTILLA
plantilla_df = pd.read_excel(plantilla_path, sheet_name="Resultados")

if len(plantilla_df.columns) != len(grouped_df.columns):
    print("Error: número de columnas no coinciden con plantilla.")
    exit()

grouped_df.columns = plantilla_df.columns
df_final = pd.concat([plantilla_df, grouped_df], ignore_index=True)

# FILTRAR EMPRESA Y PEGAR DATOS TRASPUESTOS
datos_empresas = df_final.iloc[3:]
#si no se introduce uno válido, preguntar de nuevo
empresa = input("\nIntroduce el nombre exacto de la empresa: ")
while empresa not in datos_empresas.iloc[:, 1].values:
    print("Nombre de empresa no válido. Por favor, introduce un nombre válido.")
    empresa = input("\nIntroduce el nombre exacto de la empresa: ")

filtro = datos_empresas[datos_empresas.iloc[:, 1] == empresa]
resultado = filtro.iloc[:, 7:]

if resultado.empty:
    print(f"No se encontraron registros para la empresa '{empresa}'.")
    exit()

print(f"\nSe encontraron {len(resultado)} filas para la empresa '{empresa}'")

# Trasponer los primeros 11 registros
datos_traspuestos = resultado.head(11).transpose()

# Abrir plantilla y pegar
wb = openpyxl.load_workbook(plantilla_destino_path)
ws = wb["Carga_datos"]
start_row, start_col = 2, 4

for i, fila in enumerate(datos_traspuestos.values):
    for j, valor in enumerate(fila):
        ws.cell(row=start_row + i, column=start_col + j, value=valor)

wb.save(output_path)
print(f"\nDatos pegados correctamente en '{output_path}'")

#Decir que hay que esperar a que se guarde el archivo
print("\nGuardando archivo consolidado...")


# Guardar el DataFrame unificado
df_final.to_excel(output_path2, index=False)
print(f"\nArchivo consolidado guardado en: {output_path2}")