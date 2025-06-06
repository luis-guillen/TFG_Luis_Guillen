import pandas as pd
import os
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_colwidth', None)

def add_year_column(df, year):
    """Añade columna de año después de la columna 'Código NIF'"""
    nif_col_idx = df.columns.get_loc('Código NIF')
    df.insert(nif_col_idx + 1, 'year', year)
    return df

def clean_nd_values(df):
    """Reemplaza valores 'n.d.' con cadenas vacías"""
    return df.replace('n.d.', '')

def print_dataset_summary(df):
    """Imprime un resumen detallado del dataset"""
    print("\n" + "="*80)
    print("RESUMEN DEL DATASET FINAL")
    print("="*80)
    
    # Información básica
    print("\n1. INFORMACIÓN BÁSICA:")
    print(f"   - Número total de filas: {len(df):,}")
    print(f"   - Número de empresas únicas: {df['Código NIF'].nunique():,}")
    print(f"   - Años cubiertos: {sorted(df['year'].unique())}")
    print(f"   - Número de columnas: {len(df.columns)}")
    
    # Distribución por año
    print("\n2. DISTRIBUCIÓN DE DATOS POR AÑO:")
    year_dist = df['year'].value_counts().sort_index()
    for year, count in year_dist.items():
        print(f"   - Año {year}: {count:,} registros")
    
    # Ejemplo de datos (primeras 5 filas con todas las columnas)
    print("\n3. EJEMPLO DE DATOS (primeras 5 filas):")
    pd.set_option('display.max_rows', 5)
    print(df.head().to_string())
    
    # Últimas 5 filas
    print("\n4. ÚLTIMAS 5 FILAS DEL DATASET:")
    print(df.tail().to_string())
    
    # Columnas disponibles
    print("\n5. COLUMNAS DISPONIBLES:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:3d}. {col}")

def main():
    # Configuración
    start_year = 2008
    end_year = 2023
    base_dir = 'Data/modificados'  # Directorio donde están los datos
    
    print(f"Iniciando proceso de consolidación de datos BAM para los años {start_year}-{end_year}")
    
    dfs = []
    columns_2008 = None
    for year in range(start_year, end_year + 1):
        input_file = os.path.join(base_dir, f"SABI_Export_{year}.xlsx")
        if not os.path.exists(input_file):
            print(f"Advertencia: Archivo {input_file} no encontrado. Se omite el año {year}.")
            continue
        print(f"Procesando archivo del año {year}: {input_file}")
        
        if year == 2008:
            df = pd.read_excel(input_file, sheet_name="Resultados")
            columns_2008 = df.columns.tolist()
        else:
            df = pd.read_excel(input_file, sheet_name="Resultados", header=0)
            df = df.iloc[1:].reset_index(drop=True)
            df.columns = columns_2008
        
        df = add_year_column(df, year)
        dfs.append(df)
    
    if not dfs:
        print("No se encontraron archivos de datos. Por favor, verifique las rutas de los archivos.")
        return
    
    # Paso 1: Consolidación
    print("\nPaso 1: Consolidando datos...")
    consolidated_df = pd.concat(dfs, ignore_index=True)
    
    # Paso 2: Formateo
    print("\nPaso 2: Formateando dataset...")
    formatted_df = clean_nd_values(consolidated_df)
    
    # Paso 3: Agrupación por empresa
    print("\nPaso 3: Agrupando datos por empresa...")
    grouped_df = formatted_df.sort_values(by=["Nombre", "year"])
    
    # Mostrar resumen detallado
    print_dataset_summary(grouped_df)

if __name__ == "__main__":
    main()