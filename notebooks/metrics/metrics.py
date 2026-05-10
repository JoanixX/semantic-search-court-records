import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="darkgrid")

# 1. Cargar métricas exportadas por los scripts de Go
df_sec = pd.read_csv('metricas_secuencial.csv')
df_conc = pd.read_csv('metricas_concurrente.csv')

# Media recortada del modelo secuencial (para el cálculo de Speedup)
tiempo_base_secuencial = df_sec['tiempo_segundos'].mean()

# 2. Calcular el Speedup: T_Secuencial / T_Concurrente
df_conc['speedup'] = tiempo_base_secuencial / df_conc['tiempo_segundos']

# ==========================================
# GRÁFICA 1: Escalabilidad (Tiempo vs Goroutines)
# ==========================================
plt.figure(figsize=(12, 6))
sns.lineplot(data=df_conc, x='goroutines', y='tiempo_segundos', color='darkblue', linewidth=2, label='Concurrente')
plt.axhline(tiempo_base_secuencial, color='red', linestyle='--', label='Secuencial (Baseline)')

plt.title('Disminución del Tiempo de Ejecución vs Número de Goroutines', fontsize=14, fontweight='bold')
plt.xlabel('Número de Workers (Goroutines)')
plt.ylabel('Tiempo de Ejecución (Segundos)')
plt.legend()
plt.tight_layout()
plt.savefig('grafica_tiempos.png')
plt.show()

# ==========================================
# GRÁFICA 2: Curva de Speedup (Ley de Amdahl)
# ==========================================
plt.figure(figsize=(12, 6))
sns.lineplot(data=df_conc, x='goroutines', y='speedup', color='green', linewidth=2)

plt.title('Curva de Speedup: Rendimiento del Pipeline Concurrente', fontsize=14, fontweight='bold')
plt.xlabel('Número de Workers (Goroutines)')
plt.ylabel('Speedup (X Veces más rápido)')
plt.axvline(8, color='orange', linestyle=':', label='Ideal de núcleos físicos (Ej: 8 Cores)')
plt.legend()
plt.tight_layout()
plt.savefig('grafica_speedup.png')
plt.show()