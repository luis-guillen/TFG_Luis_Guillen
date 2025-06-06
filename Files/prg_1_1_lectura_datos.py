# prg_1_1_lectura_datos.py

import pandas as pd
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

def consolidate_files(input_dir, start_year=2008, end_year=2023):
    """
    Consolida todos los archivos procesados en uno solo.
    
    Args:
        input_dir (str): Directorio que contiene los archivos procesados
        start_year (int): Año inicial
        end_year (int): Año final
        
    Returns:
        pd.DataFrame: DataFrame consolidado
    """
    try:
        input_dir = Path(input_dir)
        if not input_dir.exists():
            logging.error(f"El directorio {input_dir} no existe")
            return None
            
        # Lista para almacenar los DataFrames
        dfs = []
        
        # Procesar cada archivo
        for year in range(start_year, end_year + 1):
            file_name = f"SABI_Export_{year}_processed.xlsx"
            file_path = input_dir / file_name
            
            if not file_path.exists():
                logging.warning(f"Archivo no encontrado: {file_path}")
                continue
                
            logging.info(f"Leyendo archivo: {file_path}")
            
            try:
                # Leer el archivo Excel
                df = pd.read_excel(file_path, sheet_name="Resultados", engine='openpyxl')
                dfs.append(df)
                
                # Añadir dos filas en blanco (como separador)
                empty_df = pd.DataFrame(columns=df.columns, index=range(2))
                dfs.append(empty_df)
                
                logging.info(f"Archivo {file_name} leído correctamente")
                
            except Exception as e:
                logging.error(f"Error al leer {file_name}: {str(e)}")
                continue
        
        if not dfs:
            logging.error("No se encontraron archivos para consolidar")
            return None
            
        # Consolidar todos los DataFrames
        consolidated_df = pd.concat(dfs, ignore_index=True)
        return consolidated_df
        
    except Exception as e:
        logging.error(f"Error al consolidar archivos: {str(e)}")
        return None

def main():
    # Directorio base del proyecto
    BASE_DIR = Path(__file__).parent.parent
    
    # Directorios y archivos
    input_dir = BASE_DIR / "Data" / "modificados"
    output_file = input_dir / "Sabi_Export_2008_2023_hoteles_paso_01.xlsx"
    
    # Consolidar archivos
    consolidated_df = consolidate_files(input_dir)
    
    if consolidated_df is not None:
        try:
            # Guardar el DataFrame consolidado
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Crear la hoja "Estrategia de búsqueda"
                writer.book.create_sheet("Estrategia de búsqueda")
                
                # Guardar los datos consolidados en la hoja "Resultados"
                consolidated_df.to_excel(writer, sheet_name="Resultados", index=False)
                
            logging.info(f"Archivo consolidado guardado en: {output_file}")
            return True
        except Exception as e:
            logging.error(f"Error al guardar el archivo consolidado: {str(e)}")
            return False
    else:
        logging.error("Error al consolidar los archivos")
        return False

if __name__ == "__main__":
    if main():
        logging.info("Proceso de consolidación completado exitosamente")
    else:
        logging.error("Hubo errores durante el proceso de consolidación")