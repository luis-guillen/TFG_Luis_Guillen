import os
from datetime import datetime
from collections import defaultdict

# Cambia este path si quieres analizar otro directorio
root_dir = 'Data/bam_1'

# Diccionario para agrupar archivos por fecha de modificación
archivos_por_fecha = defaultdict(list)

for dirpath, dirnames, filenames in os.walk(root_dir):
    for filename in filenames:
        filepath = os.path.join(dirpath, filename)
        try:
            mtime = os.path.getmtime(filepath)
            fecha = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
            archivos_por_fecha[fecha].append(filepath)
        except Exception as e:
            print(f"Error con {filepath}: {e}")

# Mostrar resultados agrupados
print("\nResumen de archivos por fecha de modificación:\n")
for fecha in sorted(archivos_por_fecha.keys()):
    print(f"Fecha: {fecha} - Total archivos: {len(archivos_por_fecha[fecha])}")
    if fecha == '2024-07-06':  # Cambia aquí si buscas otra fecha
        print("  Archivos modificados el 6 de julio:")
        for archivo in archivos_por_fecha[fecha]:
            print(f"    {archivo}")
    else:
        print("  Otros archivos modificados en esta fecha:")
        for archivo in archivos_por_fecha[fecha]:
            print(f"    {archivo}")
    print()

# Totales por fecha
print("Totales por fecha:")
for fecha in sorted(archivos_por_fecha.keys()):
    print(f"{fecha}: {len(archivos_por_fecha[fecha])} archivos")