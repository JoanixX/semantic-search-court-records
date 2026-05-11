from __future__ import annotations
import argparse
import os
import subprocess
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, ensure_dir, setup_logger, write_kv_report

def append_section(path: Path, section: str, command: list[str], result: subprocess.CompletedProcess[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = [
        f"=== {section} ===",
        f"Command: {' '.join(command)}",
        f"Return code: {result.returncode}",
        "--- STDOUT ---",
        (result.stdout or "").rstrip(),
        "--- STDERR ---",
        (result.stderr or "").rstrip(),
        "",
    ]
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(block) + "\n")

def run_command(command: list[str], capture_path: Path, logger, section: str) -> None:
    logger.info("Ejecutando: %s", " ".join(command))
    result = subprocess.run(command, cwd=str(Path(__file__).resolve().parents[1]), capture_output=True, text=True)
    append_section(capture_path, section, command, result)
    if result.returncode != 0:
        logger.error("Comando fallido con codigo %d", result.returncode)
        raise SystemExit(result.returncode)
    logger.info("Comando completado correctamente")

def run_go_command(command: list[str], capture_path: Path, logger, section: str) -> None:
    env = dict(os.environ)
    env["GOCACHE"] = str((Path(__file__).resolve().parents[1] / ".gocache").resolve())
    Path(env["GOCACHE"]).mkdir(parents=True, exist_ok=True)
    logger.info("Ejecutando: %s", " ".join(command))
    result = subprocess.run(command, cwd=str(Path(__file__).resolve().parents[1]), capture_output=True, text=True, env=env)
    append_section(capture_path, section, command, result)
    if result.returncode != 0:
        logger.error("Comando Go fallido con codigo %d", result.returncode)
        raise SystemExit(result.returncode)
    logger.info("Comando Go completado correctamente")

def main() -> int:
    parser = argparse.ArgumentParser(description="Flujo de ejecución paso a paso (10 pasos)")
    parser.add_argument("--workers", type=int, default=12, help="Workers para el pipeline Go")
    parser.add_argument("--chunks", type=int, default=1000, help="Tamaño de chunks para el pipeline Go")
    args = parser.parse_args()

    ensure_dir(EVIDENCE_DIR)
    workflow_log = EVIDENCE_DIR / "workflow_full.log"
    logger = setup_logger("workflow", workflow_log)
    
    logger.info("=== INICIANDO FLUJO DE 10 PASOS ===")

    # Paso 1: Dataset normal (Ya existe en datasets/raw/dataset.csv)
    raw_data = Path("datasets/raw/dataset.csv")
    if not raw_data.exists():
        logger.error("Paso 1 fallido: No se encontró datasets/raw/dataset.csv")
        return 1
    logger.info("Paso 1: Dataset original detectado")

    # Paso 2: EDA Inicial
    logger.info("Paso 2: Ejecutando EDA inicial...")
    run_command([sys.executable, "scripts/analisis_eda.py", "--input", str(raw_data), "--output-dir", "evidence/eda/original"], workflow_log, logger, "EDA ORIGINAL")

    # Paso 3: Augment Dataset (Scraper)
    logger.info("Paso 3: Ejecutando augment_dataset.py...")
    run_command([sys.executable, "scrapers/augment_dataset.py"], workflow_log, logger, "AUGMENT DATASET")

    # Paso 4: Monitor Resources (Aviso)
    logger.info("Paso 4: RECORDATORIO - Asegúrese de tener monitor_resources.py ejecutándose en una terminal separada.")

    # Paso 5: Pruebas de rendimiento (Go)
    logger.info("Paso 5: Ejecutando pruebas de algoritmos (pruebas/)...")
    run_go_command(["go", "test", "./internal/pruebas/...", "-v"], workflow_log, logger, "PRUEBAS RENDIMIENTO GO")

    # Paso 6: Análisis de Rendimiento (Speedup, Amdahl)
    logger.info("Paso 6: Analizando métricas de rendimiento...")
    run_command([sys.executable, "scripts/analisis_rendimiento.py"], workflow_log, logger, "ANALISIS RENDIMIENTO")

    # Paso 7: Metrics (Justificación y gráficas)
    logger.info("Paso 7: Generando gráficas de métricas finales...")
    run_command([sys.executable, "scripts/metrics.py"], workflow_log, logger, "METRICS")

    # Paso 8: Pipeline Go
    logger.info("Paso 8: Ejecutando Pipeline Go principal...")
    # El comando solicitado es go run ./internal/pipeline
    run_go_command(["go", "run", "./internal/pipeline", "-workers", str(args.workers), "-chunks", str(args.chunks)], workflow_log, logger, "PIPELINE PRINCIPAL")

    # Paso 9: EDA Procesado
    logger.info("Paso 9: Ejecutando EDA sobre el dataset procesado...")
    processed_data = Path("datasets/processed/processed_records.csv")
    run_command([sys.executable, "scripts/analisis_eda.py", "--input", str(processed_data), "--output-dir", "evidence/eda/processed"], workflow_log, logger, "EDA PROCESADO")

    # Paso 10: Feature Engineering Final
    logger.info("Paso 10: Generando features finales...")
    run_command([sys.executable, "scripts/eda_features.py", "--input", str(processed_data)], workflow_log, logger, "FEATURES FINALES")

    logger.info("=== FLUJO DE 10 PASOS COMPLETADO EXITOSAMENTE ===")
    
    write_kv_report(
        EVIDENCE_DIR / "workflow_10_steps_summary.txt",
        "Resumen del Flujo Paso a Paso",
        [
            ("Estado", "Completado"),
            ("Pasos", "10/10"),
            ("Workers usados", args.workers),
            ("Chunks usados", args.chunks),
            ("Log final", str(workflow_log))
        ],
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())