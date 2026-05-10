import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from io import StringIO

def main():
    # Rutas
    data_path = os.path.join("datasets", "processed", "processed_records.csv")
    output_dir = os.path.join("evidence", "eda")
    log_path = os.path.join(output_dir, "analisis_resumen.txt")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Cargando datos desde {data_path}...")
    try:
        # Usamos el encoding del notebook
        df = pd.read_csv(data_path, encoding='ISO-8859-1', low_memory=False)

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en {data_path}")
        return

    # Captura de Logs
    print(f"Generando log de datos en {log_path}...")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== RESUMEN DEL DATASET ===\n\n")
        f.write(f"Shape: {df.shape}\n\n")
        
        f.write("--- Primeras 5 filas ---\n")
        f.write(df.head().to_string())
        f.write("\n\n--- Dtypes ---\n")
        f.write(df.dtypes.to_string())
        
        f.write("\n\n--- Información General ---\n")
        buffer = StringIO()
        df.info(buf=buffer)
        f.write(buffer.getvalue())
        
        f.write("\n--- Estadísticas Descriptivas ---\n")
        f.write(df.describe(include='all').to_string())
        
        f.write("\n\n--- Valores Nulos ---\n")
        f.write(df.isnull().sum().to_string())

    # Configuracion visual
    sns.set_theme(style="whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)

    print("Preprocesando datos...")
    # Limpieza de fechas
    df['FEC_INGRESO_STR'] = df['FEC_INGRESO'].astype(str).replace('--', np.nan)
    df['YEAR_INGRESO'] = pd.to_numeric(df['FEC_INGRESO_STR'].str[:4], errors='coerce')

    df['PUB_PAGWEB_STR'] = df['PUB_PAGWEB'].astype(str).replace('--', np.nan)
    df['YEAR_PAGWEB'] = pd.to_numeric(df['PUB_PAGWEB_STR'].str[:4], errors='coerce')

    # Calcular variable cuantitativa: Tiempo de Resolución (en años)
    df['TIEMPO_RESOLUCION_AÑOS'] = df['YEAR_PAGWEB'] - df['YEAR_INGRESO']

    # ==========================================
    # GRAFICO 1: Evolución Historica
    # ==========================================
    print("Generando gráfico de evolución histórica...")
    plt.figure(figsize=(14, 6))
    casos_por_anio = df['YEAR_INGRESO'].value_counts().sort_index()
    # Filtrar rango razonable si es necesario, el notebook usa 2000-2024
    subset_anio = casos_por_anio.loc[2000:2024] if not casos_por_anio.empty else casos_por_anio
    sns.lineplot(x=subset_anio.index, y=subset_anio.values, marker='o', color='#b22222', linewidth=2)
    plt.title('Evolución Histórica de Expedientes Ingresados al TC (2000 - 2024)', fontsize=14, fontweight='bold')
    plt.xlabel('Año de Ingreso')
    plt.ylabel('Cantidad de Expedientes')
    plt.xticks(np.arange(2000, 2025, 2))
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'evolucion.png'))

    # ==========================================
    # GRAFICO 2: Cantidad de procesos segun sus tipos
    # ==========================================
    print("Generando gráfico de tipos de procesos...")
    plt.figure(figsize=(12, 7))
    top_procesos = df['CDES_TIPOPROCESO'].value_counts().head(10)
    sns.barplot(y=top_procesos.index, x=top_procesos.values, palette='Blues_r', hue=top_procesos.index, legend=False)
    plt.title('Top 10 Tipos de Procesos más Frecuentes', fontsize=14, fontweight='bold')
    plt.xlabel('Cantidad de Casos')
    plt.ylabel('Tipo de Proceso')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'tipos_proceso.png'))

    # ==========================================
    # GRAFICO 3: Distribución por Departamento
    # ==========================================
    print("Generando gráfico de distribución por departamento...")
    plt.figure(figsize=(12, 8))
    dept_counts = df['DEPARTAMENTO'].value_counts()
    sns.barplot(y=dept_counts.index, x=dept_counts.values, palette='viridis', hue=dept_counts.index, legend=False)
    plt.title('Distribución de Carga Procesal por Departamento', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Expedientes')
    plt.ylabel('Departamento')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'departamentos.png'))

    # ==========================================
    # GRAFICO 4: Distribución del Tiempo de Resolución
    # ==========================================
    print("Generando gráfico de distribución del tiempo de resolución...")
    plt.figure(figsize=(10, 6))
    # Limpiamos nulos y valores negativos imposibles
    df_clean_time = df[df['TIEMPO_RESOLUCION_AÑOS'].notnull() & (df['TIEMPO_RESOLUCION_AÑOS'] >= 0)]
    sns.histplot(df_clean_time['TIEMPO_RESOLUCION_AÑOS'], bins=30, kde=True, color='darkblue')
    plt.title('Distribución del Tiempo de Resolución de Casos', fontsize=14, fontweight='bold')
    plt.xlabel('Años Transcurridos (Ingreso a Publicación Web)')
    plt.ylabel('Frecuencia')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'distribucion_tiempo.png'))

    print(f"Análisis EDA completado. Gráficos guardados en {output_dir}")

if __name__ == "__main__":
    main()