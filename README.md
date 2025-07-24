# Proyecto TFG: Análisis Financiero con Business Accounting Matrices (BAMs)

Este repositorio contiene el código y materiales necesarios para preparar y analizar datos financieros de empresas mediante **Business Accounting Matrices (BAMs)**.

## 📁 Estructura del proyecto

- `1_main_data.py`, `2_main_bam.py`, `3_main_prepare_bam.py`, `4_main_empresas.py`:  
  Scripts **principales** del pipeline. Estos son los archivos que deben utilizarse para ejecutar la preparación de los datos.

- Archivos con `"copia"` en el nombre (por ejemplo: `1_main_data copia.py`) o similares:  
  ⚠️ **No deben usarse.** Son versiones de prueba o desarrollo que se mantuvieron como referencia.

- `analisis_bams.ipynb`:  
  Notebook que contiene el análisis exploratorio de los BAMs generados. Aquí se visualizan, interpretan y comparan los resultados obtenidos.

## ⚙️ Descripción técnica

El pipeline actual replica la lógica de la implementación original, pero se ha optimizado para:

- Reducir significativamente el tiempo de ejecución
- Guardar automáticamente las BAMs en archivos `.pkl` ya preparados para análisis
- Asegurar consistencia en la estructura de datos y su tratamiento

---

### ⚠️ Disclaimer

El pipeline de preparación de datos sigue los mismos pasos que la implementación anterior, pero ha sido **reescrito y optimizado** para mayor eficiencia.  
Como resultado, las matrices BAM generadas están **listas para su análisis** sin necesidad de reprocesar los datos.

---

## 🚀 Uso

1. Ejecutar los scripts `X_main.py` en orden (`1_main_data.py` → `4_main_empresas.py`)
2. Analizar los resultados en el notebook `analisis_bams.ipynb`

---

Para cualquier duda, sugerencia o mejora, puedes abrir un *issue* o contactar al autor.