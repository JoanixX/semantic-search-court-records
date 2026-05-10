import psutil
import time
import csv
from pathlib import Path
import argparse

def monitor(duration_seconds, output_path, interval=0.5):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'cpu_percent', 'ram_percent', 'ram_used_gb', 'disk_usage_percent'])
        
        start_time = time.time()
        print(f"Monitoreando recursos en {output_path}...")
        
        try:
            while time.time() - start_time < duration_seconds:
                cpu = psutil.cpu_percent(interval=None)
                ram = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                writer.writerow([
                    time.strftime('%Y-%m-%d %H:%M:%S'),
                    cpu,
                    ram.percent,
                    round(ram.used / (1024**3), 2),
                    disk.percent
                ])
                time.sleep(interval)
        except KeyboardInterrupt:
            print("Monitoreo detenido manualmente.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor de recursos del sistema (Task Manager Style)")
    parser.add_argument("--duration", type=int, default=60, help="Duración del monitoreo en segundos")
    parser.add_argument("--output", default="evidence/metrics_results/system_resources.csv", help="Archivo CSV de salida")
    args = parser.parse_args()
    
    monitor(args.duration, Path(args.output))
