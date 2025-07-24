# Criterio de verificación BAM: Suma de filas = Suma de columnas
# =============================================================

import os
import pickle
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Directorio con los archivos pkl
pkl_dir = Path('Data/bam_2')

# Lista para almacenar los resultados de verificación
verification_results = []
detailed_errors = []

def verify_bam_balance(matrix, tolerance=1e-10):
    """
    Verifica que la suma de filas sea igual a la suma de columnas en una matriz BAM.
    
    Args:
        matrix: Matriz numpy o pandas DataFrame
        tolerance: Tolerancia para comparaciones de punto flotante
    
    Returns:
        dict: Resultado de la verificación con estadísticas
    """
    if isinstance(matrix, pd.DataFrame):
        matrix = matrix.values
    
    # Calcular sumas de filas y columnas
    row_sums = np.sum(matrix, axis=1)
    col_sums = np.sum(matrix, axis=0)
    
    # Verificar balance
    row_total = np.sum(row_sums)
    col_total = np.sum(col_sums)
    
    is_balanced = abs(row_total - col_total) < tolerance
    
    # Calcular estadísticas de desbalance
    max_row_sum = np.max(np.abs(row_sums))
    max_col_sum = np.max(np.abs(col_sums))
    min_row_sum = np.min(row_sums)
    min_col_sum = np.min(col_sums)
    
    return {
        'is_balanced': is_balanced,
        'row_total': row_total,
        'col_total': col_total,
        'difference': abs(row_total - col_total),
        'max_row_sum': max_row_sum,
        'max_col_sum': max_col_sum,
        'min_row_sum': min_row_sum,
        'min_col_sum': min_col_sum,
        'row_sums': row_sums,
        'col_sums': col_sums
    }

# Analizar cada archivo pkl
print("Verificando balance de matrices BAM...")
print("=" * 60)

for pkl_file in pkl_dir.glob('*.pkl'):
    try:
        with open(pkl_file, 'rb') as f:
            bam_data = pickle.load(f)
        
        # Verificar que sea un diccionario con años
        if not isinstance(bam_data, dict):
            print(f"⚠️  {pkl_file.name}: No es un diccionario")
            continue
            
        file_results = {
            'file_name': pkl_file.name,
            'years_processed': 0,
            'balanced_years': 0,
            'unbalanced_years': 0,
            'max_difference': 0.0,
            'avg_difference': 0.0,
            'total_row_sum': 0.0,
            'total_col_sum': 0.0
        }
        
        differences = []
        
        # Verificar cada año
        for year, matrix in bam_data.items():
            if isinstance(matrix, pd.DataFrame):
                result = verify_bam_balance(matrix)
                file_results['years_processed'] += 1
                
                if result['is_balanced']:
                    file_results['balanced_years'] += 1
                else:
                    file_results['unbalanced_years'] += 1
                    detailed_errors.append({
                        'file': pkl_file.name,
                        'year': year,
                        'difference': result['difference'],
                        'row_total': result['row_total'],
                        'col_total': result['col_total']
                    })
                
                differences.append(result['difference'])
                file_results['total_row_sum'] += result['row_total']
                file_results['total_col_sum'] += result['col_total']
        
        if differences:
            file_results['max_difference'] = max(differences)
            file_results['avg_difference'] = np.mean(differences)
        
        verification_results.append(file_results)
        
    except Exception as e:
        print(f" Error procesando {pkl_file.name}: {str(e)}")

# Crear DataFrame con los resultados
df_verification = pd.DataFrame(verification_results)

# Mostrar resumen general
print(f"\n RESUMEN DE VERIFICACIÓN BAM")
print("=" * 60)
print(f"Total de archivos analizados: {len(df_verification)}")
print(f"Total de años procesados: {df_verification['years_processed'].sum()}")
print(f"Total de años balanceados: {df_verification['balanced_years'].sum()}")
print(f"Total de años desbalanceados: {df_verification['unbalanced_years'].sum()}")

if len(df_verification) > 0:
    balance_rate = (df_verification['balanced_years'].sum() / df_verification['years_processed'].sum()) * 100
    print(f"Porcentaje de años balanceados: {balance_rate:.2f}%")
    
    print(f"\n📈 ESTADÍSTICAS DE DIFERENCIAS")
    print("-" * 40)
    print(f"Diferencia máxima: {df_verification['max_difference'].max():.6f}")
    print(f"Diferencia promedio: {df_verification['avg_difference'].mean():.6f}")
    print(f"Diferencia mediana: {df_verification['avg_difference'].median():.6f}")

# Mostrar archivos con problemas
if detailed_errors:
    print(f"\n  ARCHIVOS CON PROBLEMAS DE BALANCE")
    print("-" * 50)
    
    df_errors = pd.DataFrame(detailed_errors)
    df_errors = df_errors.sort_values('difference', ascending=False)
    
    print("Top 10 errores más grandes:")
    print(df_errors.head(10)[['file', 'year', 'difference', 'row_total', 'col_total']])
    
    # Agrupar por archivo
    file_errors = df_errors.groupby('file').agg({
        'difference': ['count', 'max', 'mean'],
        'year': lambda x: list(x)
    }).round(6)
    
    file_errors.columns = ['años_con_error', 'max_diferencia', 'promedio_diferencia', 'años_afectados']
    print(f"\nResumen por archivo (primeros 10):")
    print(file_errors.head(10))

# Crear visualizaciones
if len(df_verification) > 0:
    # Gráfico 1: Distribución de diferencias
    plt.figure(figsize=(15, 5))
    
    plt.subplot(1, 3, 1)
    plt.hist(df_verification['avg_difference'], bins=50, alpha=0.7, color='skyblue', edgecolor='black')
    plt.title('Distribución de Diferencias Promedio')
    plt.xlabel('Diferencia promedio')
    plt.ylabel('Frecuencia')
    plt.yscale('log')
    
    plt.subplot(1, 3, 2)
    plt.hist(df_verification['max_difference'], bins=50, alpha=0.7, color='lightcoral', edgecolor='black')
    plt.title('Distribución de Diferencias Máximas')
    plt.xlabel('Diferencia máxima')
    plt.ylabel('Frecuencia')
    plt.yscale('log')
    
    plt.subplot(1, 3, 3)
    balance_ratio = df_verification['balanced_years'] / df_verification['years_processed']
    plt.hist(balance_ratio, bins=20, alpha=0.7, color='lightgreen', edgecolor='black')
    plt.title('Distribución de Ratio de Balance')
    plt.xlabel('Ratio de años balanceados')
    plt.ylabel('Frecuencia')
    
    plt.tight_layout()
    plt.show()
    
    # Gráfico 2: Scatter plot de diferencias vs totales
    plt.figure(figsize=(12, 8))
    
    plt.subplot(2, 2, 1)
    plt.scatter(df_verification['total_row_sum'], df_verification['avg_difference'], alpha=0.6)
    plt.xlabel('Suma total de filas')
    plt.ylabel('Diferencia promedio')
    plt.title('Diferencia vs Suma de Filas')
    plt.yscale('log')
    
    plt.subplot(2, 2, 2)
    plt.scatter(df_verification['total_col_sum'], df_verification['avg_difference'], alpha=0.6)
    plt.xlabel('Suma total de columnas')
    plt.ylabel('Diferencia promedio')
    plt.title('Diferencia vs Suma de Columnas')
    plt.yscale('log')
    
    plt.subplot(2, 2, 3)
    plt.scatter(df_verification['years_processed'], df_verification['unbalanced_years'], alpha=0.6)
    plt.xlabel('Años procesados')
    plt.ylabel('Años desbalanceados')
    plt.title('Años Desbalanceados vs Total Procesados')
    
    plt.subplot(2, 2, 4)
    balance_rate_per_file = df_verification['balanced_years'] / df_verification['years_processed']
    plt.hist(balance_rate_per_file, bins=20, alpha=0.7, color='gold', edgecolor='black')
    plt.xlabel('Porcentaje de años balanceados por archivo')
    plt.ylabel('Frecuencia')
    plt.title('Distribución de Balance por Archivo')
    
    plt.tight_layout()
    plt.show()

print(f"\n Verificación completada. Revisa los gráficos y estadísticas arriba.") 