import os
import pandas as pd

# Ruta al CSV
csv_path = 'resultado_limpio.csv'

# Leer CSV
df = pd.read_csv(csv_path, dtype={'nif': str, 'año': str})

# Asegurarse de que las columnas necesarias existen
if 'nif' not in df.columns or 'año' not in df.columns:
    raise ValueError("El CSV debe tener columnas llamadas 'nif' y 'año'")

# Obtener NIFs y años únicos
nifs = df['nif'].dropna().unique()
years = df['año'].dropna().unique()

# Base path de la estructura de carpetas
base_path = os.path.join('Data', 'Experimentos')

# Crear carpetas
for nif in nifs:
    for year in years:
        path = os.path.join(base_path, nif, str(year))
        os.makedirs(path, exist_ok=True)

print(f"Estructura creada correctamente en '{base_path}'.")