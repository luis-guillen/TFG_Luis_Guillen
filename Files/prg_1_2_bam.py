# prg_1_2_bam.py

import os
import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proceso_excel.log'),
        logging.StreamHandler()
    ]
)

def format_consolidated_file(df):
    """
    Formatea el DataFrame consolidado:
    - Elimina filas en blanco
    - Elimina encabezados duplicados
    - Reemplaza 'n.d.' por espacios en blanco
    
    Args:
        df (pd.DataFrame): DataFrame a formatear
        
    Returns:
        pd.DataFrame: DataFrame formateado
    """
    try:
        # Hacer una copia del DataFrame
        formatted_df = df.copy()
        
        # Obtener el encabezado (primera fila)
        header = formatted_df.iloc[0]
        
        # Reemplazar 'n.d.' por NaN
        formatted_df = formatted_df.replace('n.d.', np.nan)
        
        # Eliminar filas completamente vacías
        formatted_df = formatted_df.dropna(how='all')
        
        # Eliminar filas que son iguales al encabezado
        formatted_df = formatted_df[~formatted_df.apply(lambda x: x.equals(header), axis=1)]
        
        # Restablecer el índice
        formatted_df = formatted_df.reset_index(drop=True)
        
        return formatted_df
        
    except Exception as e:
        logging.error(f"Error al formatear DataFrame: {str(e)}")
        return None

def main():
    # Directorio base del proyecto
    BASE_DIR = Path(__file__).parent.parent
    
    # Directorios y archivos
    input_dir = BASE_DIR / "Data" / "modificados"
    input_file = input_dir / "Sabi_Export_2008_2023_hoteles_paso_01.xlsx"
    output_file = input_dir / "Sabi_Export_2008_2023_hoteles_paso_02.xlsx"
    
    try:
        # Cargar el archivo consolidado
        df = pd.read_excel(input_file, sheet_name="Resultados", engine='openpyxl')
        logging.info(f"Archivo consolidado cargado: {input_file}")
        
        # Formatear el DataFrame
        formatted_df = format_consolidated_file(df)
        
        if formatted_df is not None:
            # Guardar el DataFrame formateado
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Crear la hoja "Estrategia de búsqueda"
                writer.book.create_sheet("Estrategia de búsqueda")
                
                # Guardar los datos formateados en la hoja "Resultados"
                formatted_df.to_excel(writer, sheet_name="Resultados", index=False)
                
            logging.info(f"Archivo formateado guardado en: {output_file}")
            return True
        else:
            logging.error("Error al formatear los datos")
            return False
            
    except Exception as e:
        logging.error(f"Error al procesar el archivo: {str(e)}")
        return False

if __name__ == "__main__":
    if main():
        logging.info("Proceso de formateo completado exitosamente")
    else:
        logging.error("Hubo errores durante el proceso de formateo")