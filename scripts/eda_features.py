"""EDA posterior a la validacion del umbral, con feature engineering."""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from collections import Counter
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, GRAPHICS_DIR, ensure_dir, make_png_bar_chart, safe_bucket, setup_logger, write_kv_report, write_text_table


DNI_RE = re.compile(r"\b\d{8}\b")


def derive_features(row: dict[str, str]) -> dict[str, str]:
    text = (row.get("RESUMEN_SENTENCIA") or row.get("TextoLegal") or row.get("TEXT") or "").strip()
    words = text.split()
    dni_count = len(DNI_RE.findall(text))
    year = ""
    date = (row.get("FEC_INGRESO") or "").strip()
    if len(date) >= 4 and date[:4].isdigit():
        year = date[:4]

    enriched = dict(row)
    enriched["feature_text_length"] = str(len(text))
    enriched["feature_word_count"] = str(len(words))
    enriched["feature_dni_count"] = str(dni_count)
    enriched["feature_has_dni"] = "1" if dni_count > 0 else "0"
    enriched["feature_year"] = year
    return enriched


def feature_eda(input_csv: Path, output_csv: Path, logger: logging.Logger) -> None:
    ensure_dir(output_csv.parent)
    ensure_dir(GRAPHICS_DIR / "features")

    length_buckets = Counter()
    word_buckets = Counter()
    has_dni_counter = Counter()
    year_counter = Counter()
    total = 0

    with input_csv.open("r", encoding="utf-8", errors="ignore", newline="") as in_handle, output_csv.open("w", encoding="utf-8", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        fieldnames = (reader.fieldnames or []) + ["feature_text_length", "feature_word_count", "feature_dni_count", "feature_has_dni", "feature_year"]
        writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            enriched = derive_features(row)
            writer.writerow(enriched)

            text = (row.get("RESUMEN_SENTENCIA") or row.get("TextoLegal") or row.get("TEXT") or "").strip()
            length_buckets[safe_bucket(len(text), 100)] += 1
            word_buckets[safe_bucket(len(text.split()), 25)] += 1
            has_dni_counter[enriched["feature_has_dni"]] += 1
            if enriched["feature_year"]:
                year_counter[enriched["feature_year"]] += 1

            if total % 100000 == 0:
                logger.info("Features procesadas: %d", total)

    top_lengths = length_buckets.most_common(10)
    top_words = word_buckets.most_common(10)
    top_years = year_counter.most_common(10)
    has_dni_rows = sorted(has_dni_counter.items())

    write_kv_report(
        EVIDENCE_DIR / "feature_eda_summary.txt",
        "Resumen de feature engineering",
        [
            ("Archivo de entrada", str(input_csv)),
            ("Archivo de salida", str(output_csv)),
            ("Filas procesadas", total),
            ("Buckets de longitud", len(length_buckets)),
            ("Buckets de palabras", len(word_buckets)),
        ],
    )
    write_text_table(
        EVIDENCE_DIR / "feature_length_table.txt",
        "Buckets de longitud de texto",
        ["Bucket", "Frecuencia"],
        top_lengths,
    )
    write_text_table(
        EVIDENCE_DIR / "feature_word_table.txt",
        "Buckets de palabras",
        ["Bucket", "Frecuencia"],
        top_words,
    )
    write_text_table(
        EVIDENCE_DIR / "feature_has_dni_table.txt",
        "Presencia de DNI",
        ["Valor", "Frecuencia"],
        has_dni_rows,
    )
    write_text_table(
        EVIDENCE_DIR / "feature_year_table.txt",
        "Top 10 anios en features",
        ["Anio", "Frecuencia"],
        top_years,
    )

    make_png_bar_chart(
        GRAPHICS_DIR / "features" / "text_length.png",
        "Buckets de longitud",
        [label for label, _ in top_lengths],
        [value for _, value in top_lengths],
    )
    make_png_bar_chart(
        GRAPHICS_DIR / "features" / "word_count.png",
        "Buckets de palabras",
        [label for label, _ in top_words],
        [value for _, value in top_words],
    )
    make_png_bar_chart(
        GRAPHICS_DIR / "features" / "year.png",
        "Top anios de features",
        [label for label, _ in top_years],
        [value for _, value in top_years],
    )

    logger.info("Feature engineering completado: %d filas", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="EDA de features sobre el dataset validado")
    parser.add_argument("--input", default="datasets/processed/combined_official_dataset.csv", help="CSV combinado validado")
    parser.add_argument("--output", default="datasets/processed/expedientes_tc_features.csv", help="CSV enriquecido")
    args = parser.parse_args()

    logger = setup_logger("features", EVIDENCE_DIR / "analysis.log")
    feature_eda(Path(args.input), Path(args.output), logger)


if __name__ == "__main__":
    main()
