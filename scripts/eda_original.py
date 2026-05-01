"""Analisis exploratorio inicial del dataset original.

Este paso solo analiza el dataset base y genera evidencias iniciales:
- nulos
- distribuciones simples
- graficos basicos
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import Counter
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, GRAPHICS_DIR, make_png_bar_chart, ensure_dir, setup_logger, write_kv_report, write_text_table


def analyze_original_dataset(csv_path: Path, logger: logging.Logger) -> None:
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
            if row_count % 50000 == 0:
                logger.info("Filas analizadas: %d", row_count)

    top_nulls = null_counter.most_common(10)
    top_process = process_counter.most_common(10)
    top_years = year_counter.most_common(10)

    ensure_dir(EVIDENCE_DIR)
    ensure_dir(GRAPHICS_DIR / "original")

    write_kv_report(
        EVIDENCE_DIR / "original_eda_summary.txt",
        "Resumen del dataset original",
        [
            ("Archivo", str(csv_path)),
            ("Filas", row_count),
            ("Columnas", len(fieldnames)),
            ("Columnas con nulos detectados", len(null_counter)),
        ],
    )

    write_text_table(
        EVIDENCE_DIR / "original_nulls_table.txt",
        "Top 10 columnas con nulos",
        ["Columna", "Nulos"],
        top_nulls,
    )
    write_text_table(
        EVIDENCE_DIR / "original_process_table.txt",
        "Top 10 procesos",
        ["Proceso", "Frecuencia"],
        top_process,
    )
    write_text_table(
        EVIDENCE_DIR / "original_year_table.txt",
        "Top 10 anios",
        ["Anio", "Frecuencia"],
        top_years,
    )

    make_png_bar_chart(
        GRAPHICS_DIR / "original" / "nulls.png",
        "Nulos por columna",
        [label[:12] for label, _ in top_nulls],
        [value for _, value in top_nulls],
    )
    make_png_bar_chart(
        GRAPHICS_DIR / "original" / "processes.png",
        "Distribucion de procesos",
        [label[:12] for label, _ in top_process],
        [value for _, value in top_process],
    )
    make_png_bar_chart(
        GRAPHICS_DIR / "original" / "years.png",
        "Distribucion por anio",
        [label for label, _ in top_years],
        [value for _, value in top_years],
    )

    logger.info("Analisis original completado: %d filas", row_count)


def main() -> None:
    parser = argparse.ArgumentParser(description="EDA inicial del dataset original")
    parser.add_argument("--input", default="datasets/raw/dataset.csv", help="CSV original a analizar")
    args = parser.parse_args()

    logger = setup_logger("eda-original", EVIDENCE_DIR / "analysis.log")
    csv_path = Path(args.input)
    logger.info("Iniciando EDA original para %s", csv_path)
    analyze_original_dataset(csv_path, logger)


if __name__ == "__main__":
    main()
