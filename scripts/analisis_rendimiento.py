import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

def main():
    base_dir = os.path.join("..", "evidence", "metrics_results")
    output_dir = base_dir
    
    print("Cargando métricas para análisis avanzado...")
    try:
        df_sec = pd.read_csv(os.path.join(base_dir, 'metricas_secuencial.csv'))
        df_conc = pd.read_csv(os.path.join(base_dir, 'metricas_concurrente.csv'))
    except FileNotFoundError:
        print(f"Error: No se encontraron archivos en {base_dir}")
        return

    sns.set_theme(style="whitegrid", palette="muted")
    
    # 1. CÁLCULO DE MEDIA RECORTADA (Trimmed Mean) para mitigar Cold Start
    def trimmed_mean(data, proportion=0.1):
        q_low = data.quantile(proportion)
        q_high = data.quantile(1 - proportion)
        return data[(data >= q_low) & (data <= q_high)].mean()

    mean_sec = trimmed_mean(df_sec['tiempo_segundos'])
    print(f"Media Recortada Secuencial: {mean_sec:.4f} s")

    # Agrupar concurrentes por número de workers
    df_grouped = df_conc.groupby('goroutines')['tiempo_segundos'].agg(['mean', 'std']).reset_index()
    # Aplicar media recortada por grupo
    df_grouped['trimmed_mean'] = df_conc.groupby('goroutines')['tiempo_segundos'].apply(lambda x: trimmed_mean(x)).values

    # 2. GRÁFICO 1: TIEMPOS DE EJECUCIÓN CON MEDIA RECORTADA
    plt.figure(figsize=(12, 6))
    plt.plot(df_grouped['goroutines'], df_grouped['trimmed_mean'], marker='o', label='Media Recortada (Concurrente)', linewidth=2.5, color='#e63946')
    plt.axhline(mean_sec, color='#1d3557', linestyle='--', label=f'Media Recortada (Secuencial: {mean_sec:.2f}s)', linewidth=2)
    plt.title('Comparativa de Tiempos: Secuencial vs Concurrente (12 Niveles de Escala)', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Workers (Goroutines)')
    plt.ylabel('Tiempo de Ejecución (Segundos)')
    plt.legend()
    plt.savefig(os.path.join(output_dir, 'tiempos_media_recortada.png'), dpi=300)

    # 3. CÁLCULO DE SPEEDUP Y EFICIENCIA PARALELA
    df_grouped['speedup'] = mean_sec / df_grouped['trimmed_mean']
    df_grouped['efficiency'] = df_grouped['speedup'] / df_grouped['goroutines']

    # GRÁFICO 2: EFICIENCIA PARALELA
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_grouped, x='goroutines', y='efficiency', marker='s', color='#2a9d8f', linewidth=2.5)
    plt.fill_between(df_grouped['goroutines'], df_grouped['efficiency'], alpha=0.2, color='#2a9d8f')
    plt.axhline(1.0, color='red', linestyle=':', label='Límite Ideal (1.0)')
    plt.title('Curva de Eficiencia Paralela ($E = Speedup / n$)', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Workers (Goroutines)')
    plt.ylabel('Eficiencia (0.0 - 1.0)')
    plt.ylim(0, 1.1)
    plt.legend()
    plt.savefig(os.path.join(output_dir, 'eficiencia_paralela.png'), dpi=300)

    # 4. MODELADO DE LA LEY DE AMDAHL
    # Estimamos la fracción serial 's' usando el punto de mayor workers
    # s = (1/S - 1/n) / (1 - 1/n)
    n_max = df_grouped['goroutines'].max()
    S_max = df_grouped.loc[df_grouped['goroutines'] == n_max, 'speedup'].values[0]
    serial_fraction = ( (1/S_max) - (1/n_max) ) / (1 - (1/n_max))
    parallel_fraction = 1 - serial_fraction
    theoretical_limit = 1 / serial_fraction if serial_fraction > 0 else float('inf')

    # GRÁFICO 3: LEY DE AMDAHL (PROYECCIÓN TEÓRICA)
    n_range = np.linspace(1, 256, 100)
    theoretical_speedup = 1 / (serial_fraction + (parallel_fraction / n_range))
    
    plt.figure(figsize=(12, 6))
    plt.plot(n_range, theoretical_speedup, label=f'Límite Teórico Amdahl (s={serial_fraction:.2%})', color='#457b9d', linestyle='--')
    plt.scatter(df_grouped['goroutines'], df_grouped['speedup'], color='#e63946', label='Speedup Observado')
    plt.title('Modelado de la Ley de Amdahl y Límite de Aceleración', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Workers')
    plt.ylabel('Speedup (Aceleración)')
    plt.legend()
    plt.savefig(os.path.join(output_dir, 'ley_amdahl.png'), dpi=300)

    # GUARDAR LOG DE ANÁLISIS
    log_path = os.path.join("..", "evidence", "metrics_results", "analisis_resultados.log")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== REPORTE TÉCNICO DE RENDIMIENTO ===\n\n")
        f.write(f"Fracción Serial Estimada (s): {serial_fraction:.4f} ({serial_fraction:.2%})\n")
        f.write(f"Fracción Paralela Estimada (p): {parallel_fraction:.4f} ({parallel_fraction:.2%})\n")
        f.write(f"Límite Teórico de Speedup (Amdahl): {theoretical_limit:.2f}x\n")
        f.write("-" * 40 + "\n")
        f.write("Configuración Óptima Detectada:\n")
        best_row = df_grouped.loc[df_grouped['efficiency'].idxmax()]
        f.write(f"Mejor Eficiencia: {best_row['efficiency']:.4f} con {int(best_row['goroutines'])} workers\n")
        f.write(f"Velocidad Máxima Alcanzada: {df_grouped['speedup'].max():.2f}x respecto al secuencial\n")

    print(f"Análisis completado. Gráficos generados en {output_dir}")
    print(f"Log de análisis guardado en {log_path}")

if __name__ == "__main__":
    main()
