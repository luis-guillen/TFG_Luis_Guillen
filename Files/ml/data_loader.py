import pandas as pd

def load_bam_data(file_path):
    """Carga datos BAM desde diferentes formatos"""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xls', '.xlsx')):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Formato de archivo no soportado")