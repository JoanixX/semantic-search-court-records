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

def run_streaming_eda(csv_path: Path, output_dir: Path, logger: logging.Logger) -> None:
    """Análisis eficiente por streaming para detectar nulos y distribuciones básicas."""
    ensure_dir(output_dir)
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
                logger.info("[%s] Filas analizadas: %d", csv_path.name, row_count)

    top_nulls = null_counter.most_common(10)
    top_process = process_counter.most_common(10)
    top_years = year_counter.most_common(10)

    write_kv_report(
        output_dir / "eda_streaming_summary.txt",
        f"Resumen Streaming: {csv_path.name}",
        [
            ("Archivo", str(csv_path)),
            ("Filas", row_count),
            ("Columnas", len(fieldnames)),
            ("Columnas con nulos detectados", len(null_counter)),
        ],
    )

    write_text_table(output_dir / "nulls_table.txt", "Top 10 columnas con nulos", ["Columna", "Nulos"], top_nulls)
    write_text_table(output_dir / "process_table.txt", "Top 10 procesos", ["Proceso", "Frecuencia"], top_process)
    write_text_table(output_dir / "year_table.txt", "Top 10 anios", ["Anio", "Frecuencia"], top_years)

    make_png_bar_chart(output_dir / "nulls.png", "Nulos por columna", [label[:12] for label, _ in top_nulls], [value for _, value in top_nulls])
    make_png_bar_chart(output_dir / "processes.png", "Distribucion de procesos", [label[:12] for label, _ in top_process], [value for _, value in top_process])
    make_png_bar_chart(output_dir / "years.png", "Distribucion por anio", [label for label, _ in top_years], [value for _, value in top_years])

    logger.info("Streaming EDA completado para %s: %d filas", csv_path.name, row_count)

def run_pandas_eda(csv_path: Path, output_dir: Path, logger: logging.Logger) -> None:
    """Análisis detallado usando Pandas para gráficos avanzados."""
    ensure_dir(output_dir)
    logger.info("Cargando %s con Pandas...", csv_path.name)
    
    df = None
    # Intento de carga con múltiples encodings para soportar dataset original
    for encoding in ['utf-8', 'latin1', 'cp1252']:
        try:
            df = pd.read_csv(csv_path, encoding=encoding, low_memory=False)
            logger.info("Cargado exitosamente con encoding: %s", encoding)
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error("Error inesperado cargando %s: %s", csv_path, e)
            return

    if df is None:
        logger.error("No se pudo cargar %s con ningún encoding soportado.", csv_path)
        return

    # Log de resumen
    log_path = output_dir / "pandas_summary.txt"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"=== RESUMEN PANDAS: {csv_path.name} ===\n\n")
        f.write(f"Shape: {df.shape}\n\n")
        f.write("--- Dtypes ---\n")
        f.write(df.dtypes.to_string())
        f.write("\n\n--- Información General ---\n")
        buffer = StringIO()
        df.info(buf=buffer)
        f.write(buffer.getvalue())
        f.write("\n--- Estadísticas Descriptivas ---\n")
        f.write(df.describe(include='all').to_string())

    # Configuracion visual
    plt.rcParams['font.family'] = 'sans-serif'
    sns.set_theme(style="whitegrid", palette="muted")
    plt.rcParams['figure.figsize'] = (12, 6)

    # --- PRE-PROCESAMIENTO DE COLUMNAS PARA ANÁLISIS ---
    # Convertimos fechas a años y calculamos la diferencia (Tiempo de Resolución)
    df['YEAR_INGRESO'] = pd.to_numeric(df['FEC_INGRESO'].astype(str).str[:4], errors='coerce')
    df['YEAR_PAGWEB'] = pd.to_numeric(df['PUB_PAGWEB'].astype(str).str[:4], errors='coerce')
    df['DIFF_TIME'] = df['YEAR_PAGWEB'] - df['YEAR_INGRESO']

    # 1. Evolución Histórica
    if not df['YEAR_INGRESO'].dropna().empty:
        logger.info("Generando evolución histórica...")
        plt.figure(figsize=(14, 6))
        casos_por_anio = df['YEAR_INGRESO'].value_counts().sort_index()
        subset_anio = casos_por_anio.loc[2000:2024] if not casos_por_anio.empty else casos_por_anio
        if not subset_anio.empty:
            sns.lineplot(x=subset_anio.index, y=subset_anio.values, marker='o', color='#b22222', linewidth=2)
            plt.title(f'Evolución Histórica ({csv_path.name})', fontsize=14, fontweight='bold')
            plt.xlabel('Año de Ingreso')
            plt.ylabel('Cantidad de Expedientes')
            plt.tight_layout()
            plt.savefig(output_dir / 'evolucion.png')
            plt.close()

    # 2. Tipos de Proceso
    if 'CDES_TIPOPROCESO' in df.columns:
        logger.info("Generando tipos de procesos...")
        plt.figure(figsize=(12, 7))
        top_procesos = df['CDES_TIPOPROCESO'].value_counts().head(10)
        sns.barplot(y=top_procesos.index, x=top_procesos.values, palette='viridis', hue=top_procesos.index, legend=False)
        plt.title('Top 10 Tipos de Procesos', pad=20)
        plt.tight_layout()
        plt.savefig(output_dir / 'tipos_proceso.png', dpi=300)
        plt.close()

    # 3. Distribución por Departamento
    if 'DEPARTAMENTO' in df.columns:
        logger.info("Generando carga por departamento...")
        plt.figure(figsize=(12, 8))
        dept_counts = df['DEPARTAMENTO'].value_counts()
        sns.barplot(y=dept_counts.index, x=dept_counts.values, palette='magma', hue=dept_counts.index, legend=False)
        plt.title('Distribución por Departamento', pad=20)
        plt.tight_layout()
        plt.savefig(output_dir / 'departamentos.png', dpi=300)
        plt.close()

    # 4. Tiempo de Resolución
    if not df['DIFF_TIME'].dropna().empty:
        logger.info("Generando tiempo de resolución...")
        df_clean_time = df['DIFF_TIME'][df['DIFF_TIME'].notnull() & (df['DIFF_TIME'] >= 0)]
        if not df_clean_time.empty:
            plt.figure(figsize=(10, 6))
            sns.histplot(df_clean_time, bins=30, kde=True, color='darkblue')
            plt.title('Distribución del Tiempo de Resolución', fontsize=14, fontweight='bold')
            plt.xlabel('Años Transcurridos')
            plt.tight_layout()
            plt.savefig(output_dir / 'distribucion_tiempo.png')
            plt.close()

    # 5. Distribución de Tipo de Resolución (8vo Gráfico anterior)
    if 'TIPO_RESOLUCION' in df.columns:
        logger.info("Generando tipos de resolución...")
        plt.figure(figsize=(10, 6))
        tipo_counts = df['TIPO_RESOLUCION'].value_counts().head(10)
        sns.barplot(x=tipo_counts.values, y=tipo_counts.index, palette='coolwarm', hue=tipo_counts.index, legend=False)
        plt.title('Distribución de Tipos de Resolución', fontsize=14, fontweight='bold')
        plt.xlabel('Frecuencia')
        plt.tight_layout()
        plt.savefig(output_dir / 'tipos_resolucion.png')
        plt.close()

    # --- NUEVOS GRÁFICOS SOLICITADOS ---

    # 6. Heatmap: Tipo de Proceso vs Fallo
    if 'CDES_TIPOPROCESO' in df.columns and 'FALLO' in df.columns:
        logger.info("Generando heatmap Proceso vs Fallo...")
        top_procs = df['CDES_TIPOPROCESO'].value_counts().head(7).index
        top_fallos = df['FALLO'].value_counts().head(7).index
        subset = df[df['CDES_TIPOPROCESO'].isin(top_procs) & df['FALLO'].isin(top_fallos)]
        
        pivot = pd.crosstab(subset['CDES_TIPOPROCESO'], subset['FALLO'])
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot, annot=True, fmt="d", cmap="YlGnBu", cbar_kws={'label': 'Cantidad de Casos'})
        plt.title('Correlación: Tipo de Proceso vs Sentencia (Fallo)', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig(output_dir / 'heatmap_proceso_fallo.png')
        plt.close()

    # 7. Evolución de Tiempo Promedio de Resolución
    if not df['DIFF_TIME'].dropna().empty:
        logger.info("Generando evolución de tiempo promedio...")
        # Filtrar datos realistas (2000-2024)
        time_evol = df[(df['YEAR_INGRESO'] >= 2000) & (df['YEAR_INGRESO'] <= 2024) & (df['DIFF_TIME'] >= 0)]
        if not time_evol.empty:
            avg_time_by_year = time_evol.groupby('YEAR_INGRESO')['DIFF_TIME'].mean()
            
            plt.figure(figsize=(12, 6))
            sns.lineplot(x=avg_time_by_year.index, y=avg_time_by_year.values, marker='s', color='darkorange', linewidth=2.5)
            plt.title('Evolución del Tiempo Promedio de Resolución (Años)', fontsize=14, fontweight='bold')
            plt.ylabel('Promedio de Años (Ingreso -> Publicación)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(output_dir / 'evolucion_tiempo_promedio.png')
            plt.close()

    # 8. Tiempo Promedio por Materia
    if 'MATERIA' in df.columns and not df['DIFF_TIME'].dropna().empty:
        logger.info("Generando tiempo por materia...")
        # Filtrar materias con carga significativa (>50 casos) para evitar outliers
        materia_counts = df['MATERIA'].value_counts()
        valid_materias = materia_counts[materia_counts > 50].index
        materia_time = df[df['MATERIA'].isin(valid_materias) & (df['DIFF_TIME'] >= 0)]
        if not materia_time.empty:
            avg_by_materia = materia_time.groupby('MATERIA')['DIFF_TIME'].mean().sort_values(ascending=False).head(10)
            plt.figure(figsize=(12, 7))
            sns.barplot(x=avg_by_materia.values, y=avg_by_materia.index, palette='rocket', hue=avg_by_materia.index, legend=False)
            plt.title('Top 10 Materias con Mayor Tiempo de Resolución Promedio', fontsize=14, fontweight='bold')
            plt.xlabel('Años Promedio')
            plt.tight_layout()
            plt.savefig(output_dir / 'tiempo_por_materia.png')
            plt.close()

    # 9. Gráfico de Clusters: Carga Procesal vs Tiempo de Resolución (Por Departamento)
    if 'DEPARTAMENTO' in df.columns and not df['DIFF_TIME'].dropna().empty:
        logger.info("Generando cluster Carga vs Tiempo...")
        dept_stats = df[df['DIFF_TIME'] >= 0].groupby('DEPARTAMENTO').agg(
            carga=('FEC_INGRESO', 'count'),
            tiempo_promedio=('DIFF_TIME', 'mean')
        ).reset_index()

        if not dept_stats.empty:
            plt.figure(figsize=(12, 8))
            sns.scatterplot(data=dept_stats, x='carga', y='tiempo_promedio', size='carga', sizes=(100, 1000), alpha=0.6, color='blue')
            
            # Etiquetar puntos
            for i in range(dept_stats.shape[0]):
                plt.text(dept_stats.carga[i], dept_stats.tiempo_promedio[i], dept_stats.DEPARTAMENTO[i], fontsize=9)

            plt.title('Cluster: Carga Procesal vs Tiempo de Resolución (Por Departamento)', fontsize=14, fontweight='bold')
            plt.xlabel('Número de Expedientes (Carga)')
            plt.ylabel('Tiempo Promedio de Resolución (Años)')
            plt.grid(True, alpha=0.2)
            plt.tight_layout()
            plt.savefig(output_dir / 'cluster_carga_tiempo.png')
            plt.close()

def main():
    # Rutas de entrada
    original_data_path = Path("datasets/raw/dataset.csv")
    processed_data_path = Path("datasets/processed/processed_records.csv")

    # Rutas de salida (Nuevas rutas solicitadas)
    eda_original_dir = EVIDENCE_DIR / "eda" / "original"
    eda_processed_dir = EVIDENCE_DIR / "eda" / "processed"
    
    # Loggers
    logger_orig = setup_logger("eda-original", eda_original_dir / "analysis.log")
    logger_proc = setup_logger("eda-processed", eda_processed_dir / "analysis.log")

    # --- Análisis Dataset Original ---
    if original_data_path.exists():
        print(f"Iniciando EDA Original: {original_data_path}")
        run_streaming_eda(original_data_path, eda_original_dir, logger_orig)
        run_pandas_eda(original_data_path, eda_original_dir, logger_orig)
    else:
        print(f"Saltando EDA Original (No encontrado en {original_data_path})")

    # --- Análisis Dataset Procesado ---
    if processed_data_path.exists():
        print(f"Iniciando EDA Procesado: {processed_data_path}")
        run_streaming_eda(processed_data_path, eda_processed_dir, logger_proc)
        run_pandas_eda(processed_data_path, eda_processed_dir, logger_proc)
    else:
        print(f"Saltando EDA Procesado (No encontrado en {processed_data_path})")

    print(f"\nProceso completado.")
    print(f"Resultados Original: {eda_original_dir}")
    print(f"Resultados Procesado: {eda_processed_dir}")

if __name__ == "__main__":
    main()