import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import csv
import logging
from collections import Counter
from io import StringIO
from pathlib import Path

# Agregar el directorio raíz al path para importar common
if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, GRAPHICS_DIR, ensure_dir, setup_logger, make_png_bar_chart, write_kv_report, write_text_table

def analyze_original_dataset(csv_path: Path, logger: logging.Logger) -> None:
    """Lógica proveniente de eda_original.py para análisis crudo de grandes volúmenes."""
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        null_counter = Counter()
        process_counter = Counter()
        year_counter = Counter()
        row_count = 0

        for row in reader:
            row_count += 1
            for key in fieldnames:
                value = (row.get(key) or "").strip()
                if not value or value in {"--", "N/A", "NA", "null", "NULL"}:
                    null_counter[key] += 1
            proceso = (row.get("CDES_TIPOPROCESO") or "NO_ESPECIFICADO").strip() or "NO_ESPECIFICADO"
            process_counter[proceso] += 1
            fecha = (row.get("FEC_INGRESO") or "").strip()
            if len(fecha) >= 4 and fecha[:4].isdigit():
                year_counter[fecha[:4]] += 1
            if row_count % 100000 == 0:
                logger.info("Filas analizadas (Original): %d", row_count)

    top_nulls = null_counter.most_common(10)
    top_process = process_counter.most_common(10)
    top_years = year_counter.most_common(10)

    # El usuario pidió guardar esto en "evidence/features/original"
    original_output_dir = EVIDENCE_DIR / "features" / "original"
    ensure_dir(original_output_dir)

    write_kv_report(
        original_output_dir / "original_eda_summary.txt",
        "Resumen del dataset original",
        [
            ("Archivo", str(csv_path)),
            ("Filas", row_count),
            ("Columnas", len(fieldnames)),
            ("Columnas con nulos detectados", len(null_counter)),
        ],
    )

    write_text_table(
        original_output_dir / "original_nulls_table.txt",
        "Top 10 columnas con nulos",
        ["Columna", "Nulos"],
        top_nulls,
    )
    write_text_table(
        original_output_dir / "original_process_table.txt",
        "Top 10 procesos",
        ["Proceso", "Frecuencia"],
        top_process,
    )
    write_text_table(
        original_output_dir / "original_year_table.txt",
        "Top 10 anios",
        ["Anio", "Frecuencia"],
        top_years,
    )

    make_png_bar_chart(
        original_output_dir / "nulls.png",
        "Nulos por columna",
        [label[:12] for label, _ in top_nulls],
        [value for _, value in top_nulls],
    )
    make_png_bar_chart(
        original_output_dir / "processes.png",
        "Distribucion de procesos",
        [label[:12] for label, _ in top_process],
        [value for _, value in top_process],
    )
    make_png_bar_chart(
        original_output_dir / "years.png",
        "Distribucion por anio",
        [label for label, _ in top_years],
        [value for _, value in top_years],
    )

    logger.info("Analisis original completado: %d filas", row_count)

def main():
    # Rutas para el análisis EDA con Pandas (Dataset Procesado)
    processed_data_path = os.path.join("datasets", "processed", "processed_records.csv")
    output_dir = os.path.join("evidence", "eda")
    log_path = os.path.join(output_dir, "analisis_resumen.txt")
    
    # También ejecutamos el análisis del dataset original (logic de eda_original.py)
    original_data_path = Path("datasets/raw/dataset.csv")
    original_logger = setup_logger("eda-original", EVIDENCE_DIR / "features/original/analysis.log")
    
    if original_data_path.exists():
        print(f"Ejecutando análisis del dataset original desde {original_data_path}...")
        analyze_original_dataset(original_data_path, original_logger)
    else:
        print(f"Aviso: No se encontró el dataset original en {original_data_path}")

    # Continuar con el EDA de Pandas para el dataset procesado
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Cargando datos procesados desde {processed_data_path}...")
    try:
        df = pd.read_csv(processed_data_path, encoding='utf-8', low_memory=False)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en {processed_data_path}")
        return

    # Captura de Logs
    print(f"Generando log de datos procesados en {log_path}...")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== RESUMEN DEL DATASET PROCESADO ===\n\n")
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

    # Configuracion visual premium
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Liberation Sans']
    sns.set_theme(style="whitegrid", palette="muted")
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['axes.titlesize'] = 16
    plt.rcParams['axes.titleweight'] = 'bold'

    print("Preprocesando datos para gráficos de pandas...")
    # Limpieza de fechas
    # Limpieza de fechas: Consideramos 'N/A' (de Go) y '--' como nulos
    df['FEC_INGRESO_STR'] = df['FEC_INGRESO'].astype(str).replace(['--', 'N/A', 'nan'], np.nan)
    df['YEAR_INGRESO'] = pd.to_numeric(df['FEC_INGRESO_STR'].str[:4], errors='coerce')

    df['PUB_PAGWEB_STR'] = df['PUB_PAGWEB'].astype(str).replace(['--', 'N/A', 'nan'], np.nan)
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
    sns.barplot(y=top_procesos.index, x=top_procesos.values, palette='viridis', hue=top_procesos.index, legend=False)
    plt.title('Top 10 Tipos de Procesos más Frecuentes', pad=20)
    plt.xlabel('Cantidad de Casos')
    plt.ylabel('Tipo de Proceso')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'tipos_proceso.png'), dpi=300)

    # ==========================================
    # GRAFICO 3: Distribución por Departamento
    # ==========================================
    print("Generando gráfico de distribución por departamento...")
    plt.figure(figsize=(12, 8))
    dept_counts = df['DEPARTAMENTO'].value_counts()
    sns.barplot(y=dept_counts.index, x=dept_counts.values, palette='magma', hue=dept_counts.index, legend=False)
    plt.title('Distribución de Carga Procesal por Departamento', pad=20)
    plt.xlabel('Número de Expedientes')
    plt.ylabel('Departamento')
    plt.grid(axis='x', linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'departamentos.png'), dpi=300)

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

    print(f"Análisis EDA completado. Resultados en {output_dir} y evidence/features/original")

if __name__ == "__main__":
    main()