import pandas as pd
import os
import openpyxl

# CONFIGURACIÓN
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

BASE_DIR = 'Data/modificados'
PLANTILLA_PATH = os.path.join(BASE_DIR, '0_Plantilla_encabezados_estandarizados.xlsx')
PLANTILLA_DESTINO_PATH = os.path.join(BASE_DIR, '1_Medium_BAM_plantilla_10_años_copia.xlsx')
OUTPUT_PATH = os.path.join(BASE_DIR, 'plantilla_actualizada.xlsx')
OUTPUT_PATH2 = os.path.join(BASE_DIR, 'datos_consolidados_con_plantilla.xlsx')
START_YEAR = 2008
END_YEAR = 2023

def ejecutar_pipeline_empresa(empresa):
    def add_year_column(df, year):
        nif_col_idx = df.columns.get_loc('Código NIF')
        df.insert(nif_col_idx + 1, 'year', year)
        return df

    def clean_nd_values(df):
        return df.replace('n.d.', '')

    dfs = []
    columns_2008 = None

    for year in range(START_YEAR, END_YEAR + 1):
        file_path = os.path.join(BASE_DIR, f"SABI_Export_{year}.xlsx")
        if not os.path.exists(file_path):
            continue

        if year == 2008:
            df = pd.read_excel(file_path, sheet_name="Resultados")
            columns_2008 = df.columns.tolist()
        else:
            df = pd.read_excel(file_path, sheet_name="Resultados", header=0).iloc[1:].reset_index(drop=True)
            df.columns = columns_2008

        df = add_year_column(df, year)
        dfs.append(df)

    if not dfs:
        raise ValueError("No se encontraron archivos válidos para consolidar.")

    consolidated_df = pd.concat(dfs, ignore_index=True)
    formatted_df = clean_nd_values(consolidated_df)
    grouped_df = formatted_df.sort_values(by=["Nombre", "year"])
    grouped_df = grouped_df.iloc[:, :-4]  # Eliminar columnas sobrantes

    plantilla_df = pd.read_excel(PLANTILLA_PATH, sheet_name="Resultados")

    if len(plantilla_df.columns) != len(grouped_df.columns):
        raise ValueError("Error: el número de columnas no coincide con la plantilla.")

    grouped_df.columns = plantilla_df.columns
    df_final = pd.concat([plantilla_df, grouped_df], ignore_index=True)

    datos_empresas = df_final.iloc[3:]

    if empresa not in datos_empresas.iloc[:, 1].values:
        raise ValueError(f"La empresa '{empresa}' no fue encontrada en los datos.")

    filtro = datos_empresas[datos_empresas.iloc[:, 1] == empresa]
    resultado = filtro.iloc[:, 7:]

    if resultado.empty:
        raise ValueError(f"No se encontraron registros para la empresa '{empresa}'.")

    # Trasponer los primeros 11 registros
    datos_traspuestos = resultado.head(11).transpose()

    # Abrir plantilla y pegar
    wb = openpyxl.load_workbook(PLANTILLA_DESTINO_PATH)
    ws = wb["Carga_datos"]
    start_row, start_col = 2, 4

    for i, fila in enumerate(datos_traspuestos.values):
        for j, valor in enumerate(fila):
            ws.cell(row=start_row + i, column=start_col + j, value=valor)

    wb.save(OUTPUT_PATH)

    # Guardar DataFrame completo
    df_final.to_excel(OUTPUT_PATH2, index=False)

    return resultado.transpose()

def ejecutar_pipeline_empresa_con_progreso(nombre_empresa, signal_callback):
    # Simulación de etapas
    etapas = 5
    for i in range(etapas):
        # Ejecutar etapa i (simulada con sleep o lógica real)
        time.sleep(0.5)
        signal_callback.emit(int((i + 1) / etapas * 100))

    df_resultado = ejecutar_pipeline_empresa(nombre_empresa)  # Tu función real
    return df_resultado