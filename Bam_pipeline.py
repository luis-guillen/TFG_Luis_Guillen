import pandas as pd
import os

# Configuración de pandas para mostrar todo al revisar
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_colwidth', None)

# =========================
# FUNCIONES AUXILIARES
# =========================
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
    print("\n1. INFORMACIÓN BÁSICA:")
    print(f"   - Número total de filas: {len(df):,}")
    print(f"   - Número de empresas únicas: {df['Código NIF'].nunique():,}")
    print(f"   - Años cubiertos: {sorted(df['year'].unique())}")
    print(f"   - Número de columnas: {len(df.columns)}")
    print("\n2. DISTRIBUCIÓN DE DATOS POR AÑO:")
    for year, count in df['year'].value_counts().sort_index().items():
        print(f"   - Año {year}: {count:,} registros")
    print("\n3. EJEMPLO DE DATOS (primeras 5 filas):")
    print(df.head().to_string())
    print("\n4. Últimas 5 FILAS DEL DATASET:")
    print(df.tail().to_string())
    print("\n5. COLUMNAS DISPONIBLES:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:3d}. {col}")

# =========================
# PARÁMETROS Y CONSOLIDACIÓN
# =========================
start_year = 2008
end_year = 2023
base_dir = 'Data/datos_SABI'
plantilla_path = os.path.join(base_dir, '0_Plantilla_encabezados_estandarizados.xlsx')

print(f"Iniciando proceso de consolidación de datos BAM para los años {start_year}-{end_year}")

dfs = []
columns_2008 = None

for year in range(start_year, end_year + 1):
    input_file = os.path.join(base_dir, f"SABI_Export_{year}.xlsx")
    if not os.path.exists(input_file):
        print(f"Advertencia: Archivo {input_file} no encontrado. Se omite el año {year}.")
        continue
    print(f"Procesando archivo del año {year}: {input_file}")
    df = pd.read_excel(input_file, sheet_name="Resultados")
    if year == 2008:
        columns_2008 = df.columns.tolist()
    else:
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = columns_2008
    df = add_year_column(df, year)
    dfs.append(df)

if not dfs:
    raise RuntimeError("No se encontraron archivos de datos. Verifica las rutas.")

# Consolidar y formatear
consolidated_df = pd.concat(dfs, ignore_index=True)
formatted_df = clean_nd_values(consolidated_df)
grouped_df = formatted_df.sort_values(by=["Nombre", "year"])

# Mostrar resumen
print_dataset_summary(grouped_df)

# =========================
# UNIFICAR CON PLANTILLA
# =========================
plantilla_df = pd.read_excel(plantilla_path, sheet_name="Resultados")
grouped_df = grouped_df.iloc[:, :-4]

if len(plantilla_df.columns) != len(grouped_df.columns):
    raise ValueError("Los DataFrames no tienen el mismo número de columnas")

grouped_df.columns = plantilla_df.columns

df_final = pd.concat([plantilla_df, grouped_df], ignore_index=True)

# =========================
# LIMPIEZA FINAL
# =========================
df_final = df_final.drop(index=[0, 1])
df_final = df_final.drop(df_final.columns[[4, 5, 6]], axis=1)
df_final.columns.values[:4] = ['idsabi', 'nombre', 'nif', 'año']
df_final['año'] = df_final['año'].astype(int).astype(str)
df_final = df_final.applymap(lambda x: str(x).strip().replace('\n', ' ').replace('\r', '') if pd.notnull(x) else "")

for col in df_final.columns[4:]:
    df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

df_final = df_final[df_final['idsabi'].str.strip() != '']

# Ordenar alfabéticamente por nombre y luego por año
df_final = df_final.sort_values(by=['nombre', 'año'])

# Guardar limpio directamente con separador decimal en coma
df_final.to_csv('resultado_limpio.csv', index=False, sep=';', encoding='utf-8-sig', decimal=',')

# =========================
# DIVISIÓN EN CSVs POR EMPRESA
# =========================
dividendo = pd.read_csv('resultado_limpio.csv', sep=';', encoding='utf-8-sig', decimal=',')
dividendo = dividendo.sort_values(by=['nombre', 'año'])

output_dir = "Data/ent"
os.makedirs(output_dir, exist_ok=True)

# Agrupar por nombre (empresa), para preservar el orden alfabético en la división
grupos = dividendo.groupby('nombre', sort=False)
num_partes = 10
filas_totales = len(dividendo)
filas_por_parte = filas_totales // num_partes

particiones, grupo_actual, filas_actuales = [], [], 0
nombres_limites = []

for nombre_empresa, grupo_empresa in grupos:
    nfilas = len(grupo_empresa)
    if filas_actuales + nfilas > filas_por_parte and len(particiones) < num_partes - 1:
        df_part = pd.concat(grupo_actual).sort_values(by=['nombre', 'año'])
        nombre_inicio = df_part['nombre'].iloc[0][:1].upper()
        nombre_fin = df_part['nombre'].iloc[-1][:1].upper()
        nombres_limites.append((nombre_inicio, nombre_fin))
        particiones.append(df_part)
        grupo_actual = [grupo_empresa]
        filas_actuales = nfilas
    else:
        grupo_actual.append(grupo_empresa)
        filas_actuales += nfilas

# Último grupo
df_part = pd.concat(grupo_actual).sort_values(by=['nombre', 'año'])
nombre_inicio = df_part['nombre'].iloc[0][:1].upper()
nombre_fin = df_part['nombre'].iloc[-1][:1].upper()
nombres_limites.append((nombre_inicio, nombre_fin))
particiones.append(df_part)

# Guardar archivos
for i, (parte, (ini, fin)) in enumerate(zip(particiones, nombres_limites), 1):
    archivo = os.path.join(output_dir, f'Sabi_Export_com_{ini}-{fin}.csv')
    parte.to_csv(archivo, index=False, sep=';', encoding='utf-8-sig', decimal=',')
    print(f'Archivo guardado: {archivo} ({len(parte)} filas, {parte["nif"].nunique()} empresas)')