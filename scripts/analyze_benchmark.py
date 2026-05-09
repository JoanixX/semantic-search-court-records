import pandas as pd
import matplotlib.pyplot as plt
import subprocess
import io
import os
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
EVIDENCE_DIR = ROOT_DIR / "evidence"

def run_benchmark(records=20000, runs=3):
    cmd = ["go", "run", "./cmd/benchmark", "-records", str(records), "-runs", str(runs), "-delay-ms", "2"]
    print(f"Ejecutando benchmark: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT_DIR), capture_output=True, text=True)
    if result.returncode != 0:
        print("Error en benchmark Go:")
        print(result.stderr)
        return None
    return result.stdout

def analyze_and_plot(csv_data):
    df = pd.read_csv(io.StringIO(csv_data))
    
    # Split logs
    seq_df = df[df['Type'] == 'Sequential']
    conc_df = df[df['Type'] == 'Concurrent']
    
    seq_log_path = EVIDENCE_DIR / "benchmark_sequential.log"
    conc_log_path = EVIDENCE_DIR / "benchmark_concurrent.log"
    
    seq_df.to_csv(seq_log_path, index=False)
    conc_df.to_csv(conc_log_path, index=False)
    
    print(f"Logs guardados en {seq_log_path} y {conc_log_path}")
    
    # Calculate means
    means = df.groupby(['Type', 'Workers'])['Duration'].mean().reset_index()
    
    # Plotting
    plt.figure(figsize=(12, 6))
    
    # Plot 1: Execution Time vs Workers
    plt.subplot(1, 2, 1)
    conc_means = means[means['Type'] == 'Concurrent']
    seq_mean_val = means[means['Type'] == 'Sequential']['Duration'].values[0]
    
    plt.plot(conc_means['Workers'], conc_means['Duration'], marker='o', label='Concurrent')
    plt.axhline(y=seq_mean_val, color='r', linestyle='--', label='Sequential Mean')
    plt.xlabel('Number of Workers')
    plt.ylabel('Duration (s)')
    plt.title('Execution Time vs Workers')
    plt.legend()
    plt.grid(True)
    
    # Plot 2: Speedup
    plt.subplot(1, 2, 2)
    conc_means['Speedup'] = seq_mean_val / conc_means['Duration']
    plt.plot(conc_means['Workers'], conc_means['Speedup'], marker='s', color='green')
    plt.plot(conc_means['Workers'], conc_means['Workers'], color='gray', linestyle=':', label='Ideal Speedup')
    plt.xlabel('Number of Workers')
    plt.ylabel('Speedup (x)')
    plt.title('Speedup vs Workers')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plot_path = EVIDENCE_DIR / "benchmark_plots.png"
    plt.savefig(plot_path)
    print(f"Gráficos guardados en {plot_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=20000)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    
    output = run_benchmark(args.records, args.runs)
    if output:
        analyze_and_plot(output)
