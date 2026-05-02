"""Orquestador principal del flujo de trabajo.

Orden requerido:
1. Tests obligatorios.
2. EDA inicial del dataset original.
3. Scraper para complementar hasta 1M+.
4. Fusion y validacion del dataset.
5. EDA de features.
6. Ejecucion Go con trazabilidad y logs.
"""

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
    parser = argparse.ArgumentParser(description="Flujo completo del proyecto con gate de pruebas")
    parser.add_argument("--target-total", type=int, default=1_000_000, help="umbral total requerido")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--delay-ms", type=int, default=1)
    parser.add_argument("--benchmark-runs", type=int, default=3)
    parser.add_argument("--benchmark-records", type=int, default=10000)
    args = parser.parse_args()

    ensure_dir(EVIDENCE_DIR)
    workflow_log = EVIDENCE_DIR / "workflow.log"
    logger = setup_logger("workflow", workflow_log)
    logger.info("Inicio del flujo controlado")

    tests_log = EVIDENCE_DIR / "tests.log"
    analysis_log = EVIDENCE_DIR / "analysis.log"
    prep_log = EVIDENCE_DIR / "prep.log"
    go_log = EVIDENCE_DIR / "go.log"

    for log_path in [tests_log, analysis_log, prep_log, go_log, workflow_log]:
        log_path.write_text("", encoding="utf-8")

    run_command([sys.executable, "-m", "unittest", "discover", "-s", "tests/python", "-p", "test_*.py"], tests_log, logger, "PYTHON TESTS")
    run_go_command(["go", "test", "./tests/unit", "./tests/integration"], tests_log, logger, "GO TESTS")

    run_command([sys.executable, "scripts/eda_original.py"], analysis_log, logger, "ORIGINAL EDA")
    run_command([sys.executable, "scrapers/augment_dataset.py", "--target-total", str(args.target_total)], prep_log, logger, "SCRAPER")
    run_command([sys.executable, "scripts/merge_datasets.py"], prep_log, logger, "MERGE")
    run_command([sys.executable, "scripts/validate_dataset.py", "--target", str(args.target_total)], prep_log, logger, "VALIDATION")
    run_command([sys.executable, "scripts/eda_features.py"], analysis_log, logger, "FEATURE EDA")

    go_env = ["-csv", "datasets/processed/combined_official_dataset.csv", "-workers", str(args.workers), "-delay-ms", str(args.delay_ms), "-log-every", "5000"]
    run_go_command(["go", "run", "./cmd/pipeline", *go_env], go_log, logger, "PIPELINE")
    run_go_command(["go", "run", "./cmd/benchmark", "-records", str(args.benchmark_records), "-runs", str(args.benchmark_runs), "-delay-ms", "2"], go_log, logger, "BENCHMARK")

    write_kv_report(
        EVIDENCE_DIR / "workflow_summary.txt",
        "Resumen del flujo controlado",
        [
            ("Tests", "ok"),
            ("EDA original", "ok"),
            ("Scraper", "ok"),
            ("Merge", "ok"),
            ("Validacion", "ok"),
            ("Features", "ok"),
            ("Go pipeline", "ok"),
            ("Go benchmark", "ok"),
        ],
    )
    logger.info("Flujo completado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
