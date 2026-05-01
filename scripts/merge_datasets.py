"""Une dataset original y complementado de forma trazable y sin duplicados."""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.common import EVIDENCE_DIR, ensure_dir, setup_logger


def row_key(row: dict[str, str], fieldnames: list[str]) -> str:
    payload = "|".join((row.get(name) or "") for name in fieldnames)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def merge_csvs(original_csv: Path, complement_csv: Path, output_csv: Path, logger: logging.Logger) -> int:
    ensure_dir(output_csv.parent)
    seen: set[str] = set()
    written = 0

    headers: list[str] = []
    for source in [original_csv, complement_csv]:
        if not source.exists():
            continue
        with source.open("r", encoding="utf-8", errors="ignore", newline="") as in_handle:
            reader = csv.DictReader(in_handle)
            for name in reader.fieldnames or []:
                if name not in headers:
                    headers.append(name)

    with output_csv.open("w", encoding="utf-8", newline="") as out_handle:
        writer = csv.DictWriter(out_handle, fieldnames=headers)
        writer.writeheader()

        for source in [original_csv, complement_csv]:
            if not source.exists():
                logger.info("Fuente ausente, se omite: %s", source)
                continue

            with source.open("r", encoding="utf-8", errors="ignore", newline="") as in_handle:
                reader = csv.DictReader(in_handle)
                for row in reader:
                    key = row_key(row, headers)
                    if key in seen:
                        continue
                    seen.add(key)
                    writer.writerow({name: row.get(name, "") for name in headers})
                    written += 1
                    if written % 100000 == 0:
                        logger.info("Filas fusionadas: %d", written)

    logger.info("Fusion completada: %d filas", written)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description="Fusiona dataset original y complementado")
    parser.add_argument("--original", default="datasets/raw/dataset.csv", help="CSV original")
    parser.add_argument("--complement", default="datasets/processed/expedientes_tc_complemento.csv", help="CSV complementado")
    parser.add_argument("--output", default="datasets/processed/expedientes_tc_combined.csv", help="CSV combinado")
    args = parser.parse_args()

    logger = setup_logger("merge", EVIDENCE_DIR / "prep.log")
    merge_csvs(Path(args.original), Path(args.complement), Path(args.output), logger)


if __name__ == "__main__":
    main()
