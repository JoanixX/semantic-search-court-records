"""Valida que el dataset combinado alcance el umbral requerido."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, count_csv_rows, setup_logger, write_kv_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida el total de registros")
    parser.add_argument("--input", default="datasets/processed/combined_official_dataset.csv", help="CSV combinado")
    parser.add_argument("--target", type=int, default=1_000_000, help="umbral minimo")
    args = parser.parse_args()

    logger = setup_logger("validate", EVIDENCE_DIR / "prep.log")
    csv_path = Path(args.input)
    total = count_csv_rows(csv_path) if csv_path.exists() else 0
    status = "cumple" if total >= args.target else "no cumple"
    write_kv_report(
        EVIDENCE_DIR / "validation_summary.txt",
        "Validacion del dataset",
        [
            ("Archivo", str(csv_path)),
            ("Registros", total),
            ("Objetivo", args.target),
            ("Estado", status),
        ],
    )
    logger.info("Validacion: %s", status)
    logger.info("Total=%d Objetivo=%d", total, args.target)
    return 0 if total >= args.target else 1


if __name__ == "__main__":
    raise SystemExit(main())
