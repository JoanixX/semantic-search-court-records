import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def main():
    base_dir = os.path.join("evidence", "metrics_results")
    output_dir = base_dir
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        df_sec = pd.read_csv(os.path.join(base_dir, 'metricas_secuencial.csv'))
        df_conc = pd.read_csv(os.path.join(base_dir, 'metricas_concurrente.csv'))
        res_path = os.path.join(base_dir, 'system_resources.csv')
        
    except FileNotFoundError:
        print(f"Error: No se encontraron los archivos en {base_dir}")
        return

    # Cálculos base (Media Recortada)
    tiempos_ordenados = df_sec['tiempo_segundos'].sort_values()
    media_secuencial = tiempos_ordenados.iloc[1:-1].mean()
    sns.set_theme(style="whitegrid")

    # ==========================================
    # GRÁFICA 1: Variabilidad del Modelo Secuencial (Boxplot y Swarmplot)
    # ==========================================
    plt.figure(figsize=(8, 6))
    sns.boxplot(y=df_sec['tiempo_segundos'], color='lightgray', width=0.3)
    sns.swarmplot(y=df_sec['tiempo_segundos'], color='red', size=10)
    plt.axhline(media_secuencial, color='blue', linestyle='--', label=f'Media Recortada ({media_secuencial:.2f} s)')
    plt.title('Inestabilidad del Tiempo de Ejecución (Modelo Secuencial)', fontsize=14, fontweight='bold')
    plt.ylabel('Tiempo de Ejecución (Segundos)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'variabilidad_secuencial.png'))
    print(f"G1 guardado en {output_dir}")

    # ==========================================
    # GRÁFICA 2: Decaimiento Exponencial del Tiempo (Curva de Rendimiento)
    # ==========================================
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_conc, x='goroutines', y='tiempo_segundos', marker='o', markersize=8, color='darkred', linewidth=2.5)
    plt.title('Impacto de la Concurrencia en la Reducción del Tiempo (Decaimiento)', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Workers (Goroutines)')
    plt.ylabel('Tiempo de Ejecución (Segundos)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'decaimiento_tiempo.png'))
    print(f"G2 guardado en {output_dir}")
    
    # ==========================================
    # GRÁFICA 3: Curva de Speedup (Factor de Aceleración)
    # ==========================================
    # Speedup = T_secuencial_base / T_concurrente
    df_conc['speedup'] = media_secuencial / df_conc['tiempo_segundos']
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=df_conc, x='goroutines', y='speedup', marker='s', color='green', linewidth=2.5)
    plt.fill_between(df_conc['goroutines'], df_conc['speedup'], alpha=0.2, color='green')
    plt.title('Curva de Speedup: Factor de Aceleración del Sistema', fontsize=14, fontweight='bold')
    plt.xlabel('Número de Workers (Goroutines)')
    plt.ylabel('Speedup (X veces más rápido)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'curva_speedup.png'))
    print(f"G3 guardado en {output_dir}")

    # ==========================================
    # GRÁFICA 4: Análisis de Ganancia Marginal (Rendimientos Decrecientes)
    # ==========================================
    df_conc_grouped = df_con_grouped = df_conc.groupby('goroutines')['tiempo_segundos'].mean().reset_index()
    df_conc_grouped['ahorro_tiempo'] = df_conc_grouped['tiempo_segundos'].shift(1) - df_conc_grouped['tiempo_segundos']
    df_conc_grouped['ahorro_tiempo'] = df_conc_grouped['ahorro_tiempo'].fillna(0)

    plt.figure(figsize=(10, 6))
    sns.barplot(x=df_conc_grouped['goroutines'].astype(str), y=df_conc_grouped['ahorro_tiempo'], palette='viridis', hue=df_conc_grouped['goroutines'].astype(str), legend=False)
    plt.title('Análisis de Rendimientos Decrecientes (Ganancia Marginal)', fontsize=14, fontweight='bold')
    plt.xlabel('Incremento de Workers (Goroutines)')
    plt.ylabel('Segundos Ahorrados respecto al paso anterior')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'rendimientos_decrecientes.png'))
    print(f"G4 guardado en {output_dir}")

    # ==========================================
    # GRÁFICA 5: Monitor de Recursos (Uso de CPU y RAM)
    # ==========================================
    if os.path.exists(res_path):
        df_res = pd.read_csv(res_path)
        fig, ax1 = plt.subplots(figsize=(12, 6))
        ax2 = ax1.twinx()
        
        ax1.plot(df_res.index, df_res['cpu_percent'], color='red', alpha=0.6, label='CPU (%)')
        ax2.plot(df_res.index, df_res['ram_percent'], color='blue', alpha=0.6, label='RAM (%)')
        
        ax1.set_xlabel('Tiempo (Muestras)')
        ax1.set_ylabel('CPU %', color='red')
        ax2.set_ylabel('RAM %', color='blue')
        plt.title('Uso de Recursos del Sistema (Evidencia de Computación)', fontsize=14, fontweight='bold')
        fig.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'uso_recursos.png'))
        print(f"G5 guardado en {output_dir}")

if __name__ == "__main__":
    main()