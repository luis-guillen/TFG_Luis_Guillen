import pandas as pd
import numpy as np
from io import StringIO
import re
import os
from pathlib import Path
import logging

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proceso_excel.log'),
        logging.StreamHandler()
    ]
)

def procesar_dataframes_bam(directorio_entrada=None):
    """
    Procesa los archivos Excel de SABI y los consolida en un único DataFrame.
    
    Args:
        directorio_entrada (str): Directorio donde se encuentran los archivos Excel procesados
    
    Returns:
        pd.DataFrame: DataFrame consolidado con todos los datos
    """
    # Si no se proporciona un directorio, usar el directorio por defecto
    if directorio_entrada is None:
        directorio_entrada = Path(__file__).parent.parent / "Data" / "modificados"
    
    # Lista de años a procesar
    años = list(range(2008, 2024))
    
    # Lista para almacenar los DataFrames
    dfs = []
    
    # Contador de archivos procesados
    archivos_procesados = 0
    
    # Procesar cada año
    for año in años:
        # Construir el nombre del archivo
        nombre_archivo = f"SABI_Export_{año}.xlsx"
        ruta_archivo = directorio_entrada / nombre_archivo
        
        try:
            if ruta_archivo.exists():
                print(f"Leyendo archivo: {nombre_archivo}")
                # Leer el archivo Excel con openpyxl
                df = pd.read_excel(ruta_archivo, engine='openpyxl')
                
                # Añadir columna de año
                df['year'] = año
                
                # Añadir el DataFrame a la lista
                dfs.append(df)
                archivos_procesados += 1
                print(f"Archivo {nombre_archivo} procesado. Filas: {len(df)}")
            else:
                print(f"No se encontró el archivo: {nombre_archivo}")
        except Exception as e:
            print(f"Error al procesar {nombre_archivo}: {str(e)}")
    
    if not dfs:
        print("No se encontraron archivos para procesar.")
        return None
    
    # Consolidar todos los DataFrames
    print("\nConsolidando datos...")
    datos_consolidados = pd.concat(dfs, ignore_index=True)
    print(f"Total de filas consolidadas: {len(datos_consolidados)}")
    
    # Reemplazar 'n.d.' por NaN
    datos_consolidados = datos_consolidados.replace('n.d.', np.nan)
    
    # Agrupar por nombre de empresa
    datos_contables = datos_consolidados.groupby('Nombre').first()
    
    # Ejemplo de análisis para RIU HOTELS SA
    print("\nEjemplo de análisis para RIU HOTELS SA:")
    empresa_ejemplo = datos_consolidados[datos_consolidados['Nombre'] == "RIU HOTELS SA"]
    datos_transpuestos = empresa_ejemplo.transpose()
    
    # Imprimir estadísticas
    print(f"\nEstadísticas del proceso:")
    print(f"Archivos procesados: {archivos_procesados}")
    print(f"Dimensiones del DataFrame consolidado: {datos_consolidados.shape}")
    print(f"Número de empresas únicas: {len(datos_contables)}")
    
    # Calcular y mostrar el porcentaje de valores nulos
    porcentaje_nulos = (datos_transpuestos.isna().sum().sum() / (datos_transpuestos.shape[0] * datos_transpuestos.shape[1])) * 100
    print(f"Porcentaje de valores nulos en el DataFrame formateado: {porcentaje_nulos:.2f}%")
    
    # Mostrar las columnas disponibles
    print("\nColumnas disponibles en el DataFrame:")
    print(datos_consolidados.columns.tolist())
    
    # Buscar columnas que contengan 'activo' o 'ingresos'
    columnas_activo = [col for col in datos_consolidados.columns if 'activo' in col.lower()]
    columnas_ingresos = [col for col in datos_consolidados.columns if 'ingresos' in col.lower()]
    
    print("\nColumnas que contienen 'activo':")
    print(columnas_activo)
    print("\nColumnas que contienen 'ingresos':")
    print(columnas_ingresos)
    
    # Ejemplo de análisis con las columnas encontradas
    if columnas_activo:
        suma_activos = datos_consolidados[columnas_activo[0]].sum()
        print(f"\nSuma de {columnas_activo[0]}: {suma_activos:,.2f}")
    
    if columnas_ingresos:
        media_ingresos = datos_consolidados[columnas_ingresos[0]].mean()
        print(f"Media de {columnas_ingresos[0]}: {media_ingresos:,.2f}")
    
    return datos_consolidados

def generar_datos_para_bam(df, empresa_NIF=None, empresa_nombre=None):
    """
    Genera los datos específicos para una empresa en el formato necesario para BAM
    
    Args:
        df (pandas.DataFrame): DataFrame con los datos ordenados por empresas y años
        empresa_NIF (str): NIF de la empresa a procesar
        empresa_nombre (str): Nombre de la empresa a procesar
    
    Returns:
        pandas.DataFrame: DataFrame con los datos contables transpuestos para cargar en BAM
    """
    print("\nGeneración de datos para plantilla BAM")
    
    if not empresa_NIF and not empresa_nombre:
        print("  ✗ Error: Debe especificar NIF o nombre de empresa")
        return None
    
    # Filtrar el DataFrame para la empresa seleccionada
    if empresa_NIF:
        df_empresa = df[df["Código NIF"] == empresa_NIF]
        print(f"  ✓ Filtrando por NIF: {empresa_NIF}")
    else:
        df_empresa = df[df["Nombre"] == empresa_nombre]
        print(f"  ✓ Filtrando por Nombre: {empresa_nombre}")
    
    if df_empresa.empty:
        print(f"  ✗ Error: No se encontraron datos para la empresa especificada")
        return None
    
    # Obtenemos el NIF y Nombre para información
    NIF = df_empresa["Código NIF"].iloc[0]
    nombre = df_empresa["Nombre"].iloc[0]
    print(f"  ✓ Empresa: {nombre} (NIF: {NIF})")
    
    # Extraemos solo las columnas de datos contables (a partir de la columna H)
    columnas_datos = df_empresa.columns[7:]  # Asumiendo que los datos empiezan en la columna H
    datos_contables = df_empresa[columnas_datos]
    
    # Transponemos los datos para que coincidan con el formato de la plantilla BAM
    datos_bam = datos_contables.T
    
    print(f"  ✓ Datos transpuestos preparados para BAM - {datos_bam.shape[0]} filas, {datos_bam.shape[1]} columnas")
    
    return {
        'datos_originales': df_empresa,
        'datos_contables': datos_contables,
        'datos_bam': datos_bam
    }

def ejecutar_procedimiento_pandas(años, directorio_entrada="./", empresa_NIF=None, empresa_nombre=None):
    """
    Ejecuta el procedimiento completo utilizando solo DataFrames de pandas
    
    Args:
        años (list): Lista de años a procesar
        directorio_entrada (str): Directorio donde se encuentran los archivos
        empresa_NIF (str): NIF de la empresa para generar datos BAM
        empresa_nombre (str): Nombre de la empresa para generar datos BAM
    
    Returns:
        dict: Diccionario con todos los resultados del proceso
    """
    # Ejecutar los 3 primeros pasos
    resultados = procesar_dataframes_bam(directorio_entrada)
    
    # Si tenemos los datos del paso 3 y una empresa especificada, generamos datos para BAM
    if 'paso3' in resultados and (empresa_NIF or empresa_nombre):
        resultados['paso4'] = generar_datos_para_bam(
            resultados['paso3'], 
            empresa_NIF, 
            empresa_nombre
        )
    
    print("\nProcedimiento completado con éxito.")
    return resultados

def mostrar_estadisticas(resultados):
    """
    Muestra estadísticas sobre los DataFrames resultantes
    
    Args:
        resultados (dict): Diccionario con los resultados del proceso
    """
    print("\n===== ESTADÍSTICAS DEL PROCESO =====")
    
    # Paso 0
    if 'paso0' in resultados:
        dfs_anuales = resultados['paso0']
        print(f"\nPaso 0: {len(dfs_anuales)} archivos procesados")
        for año, df in dfs_anuales.items():
            print(f"  - {año}: {df.shape[0]} filas, {df.shape[1]} columnas")
    
    # Paso 1
    if 'paso1' in resultados:
        df_consolidado = resultados['paso1']
        print(f"\nPaso 1: DataFrame consolidado")
        print(f"  - Dimensiones: {df_consolidado.shape[0]} filas, {df_consolidado.shape[1]} columnas")
        print(f"  - Memoria utilizada: {df_consolidado.memory_usage().sum() / (1024*1024):.2f} MB")
    
    # Paso 2
    if 'paso2' in resultados:
        df_formateado = resultados['paso2']
        valores_nulos = df_formateado.isna().sum().sum()
        print(f"\nPaso 2: DataFrame formateado")
        print(f"  - Valores nulos: {valores_nulos}")
        print(f"  - Porcentaje de valores nulos: {valores_nulos/(df_formateado.shape[0]*df_formateado.shape[1])*100:.2f}%")
    
    # Paso 3
    if 'paso3' in resultados:
        df_ordenado = resultados['paso3']
        empresas_unicas = df_ordenado['Nombre'].nunique()
        años_por_empresa = df_ordenado.groupby('Nombre')['year'].nunique()
        print(f"\nPaso 3: DataFrame ordenado por empresas")
        print(f"  - Empresas únicas: {empresas_unicas}")
        print(f"  - Promedio de años por empresa: {años_por_empresa.mean():.2f}")
        print(f"  - Años mínimos por empresa: {años_por_empresa.min()}")
        print(f"  - Años máximos por empresa: {años_por_empresa.max()}")
    
    # Paso 4
    if 'paso4' in resultados and resultados['paso4']:
        datos_bam = resultados['paso4']
        print(f"\nPaso 4: Datos para BAM generados")
        if 'datos_originales' in datos_bam:
            empresa = datos_bam['datos_originales']['Nombre'].iloc[0]
            años_disponibles = datos_bam['datos_originales']['year'].nunique()
            print(f"  - Empresa: {empresa}")
            print(f"  - Años disponibles: {años_disponibles}")
        if 'datos_bam' in datos_bam:
            print(f"  - Dimensiones datos transpuestos: {datos_bam['datos_bam'].shape[0]} filas, {datos_bam['datos_bam'].shape[1]} columnas")

def cargar_datos_excel():
    # Directorio donde se encuentran los archivos Excel
    directorio = Path("Data/modificados")
    
    # Lista para almacenar los DataFrames
    dfs = []
    
    # Procesar archivos para los años 2008-2023
    for año in range(2008, 2024):
        archivo = directorio / f"SABI_Export_{año}.xlsx"
        
        if archivo.exists():
            print(f"\nLeyendo archivo {archivo.name}...")
            df = pd.read_excel(archivo)
            
            # Añadir columna de año
            df['Año'] = año
            
            # Mostrar información básica
            print(f"- Dimensiones: {df.shape}")
            print(f"- Columnas: {list(df.columns)}")
            print("\n        - Primeras 5 filas:")
            print(df.head())
            
            dfs.append(df)
            print("-" * 80)
    
    # Consolidar todos los DataFrames
    print("\nConsolidando datos...")
    df_consolidado = pd.concat(dfs, ignore_index=True)
    
    # Mostrar información del DataFrame consolidado
    print("\nInformación del DataFrame consolidado:")
    print(f"- Dimensiones: {df_consolidado.shape}")
    print(f"- Años disponibles: {sorted(df_consolidado['Año'].unique().tolist())}")
    print("\nPrimeras 5 filas:")
    print(df_consolidado.head())
    
    return df_consolidado

# Ejemplo de uso:
if __name__ == "__main__":
    # Importar las bibliotecas necesarias
    import pandas as pd
    import numpy as np
    from pathlib import Path
    
    # Procesar los datos
    datos = procesar_dataframes_bam()