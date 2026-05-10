from __future__ import annotations
import argparse
import csv
import logging
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
from scripts.common import EVIDENCE_DIR, GRAPHICS_DIR, ensure_dir, make_png_bar_chart, setup_logger, write_kv_report, write_text_table

def derive_features(row: dict[str, str]) -> dict[str, str]:
    # 1. TEXTO_PARA_EMBEDDING: Unificación Semántica
    embedding_text = " ".join([
        row.get("CDES_TIPOPROCESO", ""),
        row.get("MATERIA", ""),
        row.get("SUB_MATERIA", ""),
        row.get("ESPECIFICA", ""),
        row.get("RESUMEN_SENTENCIA", "")
    ]).strip()

    # 2. RIESGO_PII_NIVEL: Seguridad / Anonimización
    demandante = row.get("TIPO_DEMANDANTE", "").upper().strip()
    riesgo = 0
    if "NATURAL" in demandante:
        riesgo = 3
    elif "JURIDICA" in demandante or "JURÍDICA" in demandante:
        riesgo = 1
    elif demandante and demandante != "--" and demandante != "N/A":
        riesgo = 2 # Otras entidades (Estado, etc)

    # 3. DURACION_RESOLUCION_DIAS: Eficiencia Procesal
    duracion_dias = -1
    fec_ingreso = row.get("FEC_INGRESO", "").strip()
    pub_pagweb = row.get("PUB_PAGWEB", "").strip()
    
    # Soporta YYYY-MM-DD (len 10) y YYYYMMDD (len 8)
    def parse_date(date_str):
        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
            return datetime.strptime(date_str, "%Y-%m-%d")
        if len(date_str) == 8 and date_str.isdigit():
            return datetime.strptime(date_str, "%Y%m%d")
        return None

    try:
        d_ini = parse_date(fec_ingreso)
        d_fin = parse_date(pub_pagweb)
        if d_ini and d_fin:
            diff = (d_fin - d_ini).days
            if diff >= 0:
                duracion_dias = diff
    except Exception:
        pass

    # 4. NECESITA_HIDRATACION: Control de Pipeline (Scraper trigger)
    # Se marca como 1 si falta el resumen o si la fecha de publicación es inválida/link
    resumen = row.get("RESUMEN_SENTENCIA", "").strip()
    hidratacion = 0
    if not resumen or resumen == "--" or resumen == "N/A":
        hidratacion = 1
    elif "http" in pub_pagweb.lower() or ".pdf" in pub_pagweb.lower():
        hidratacion = 1
    elif pub_pagweb == "N/A" or pub_pagweb == "--":
        hidratacion = 1

    # 5. UBICACION_GEOCLUSTER: Agrupamiento Jurisdiccional
    geocluster = f"{row.get('DEPARTAMENTO', 'N/A')} | {row.get('DISTRITO', 'N/A')}"

    enriched = dict(row)
    enriched["TEXTO_PARA_EMBEDDING"] = embedding_text
    enriched["RIESGO_PII_NIVEL"] = str(riesgo)
    enriched["DURACION_RESOLUCION_DIAS"] = str(duracion_dias) if duracion_dias >= 0 else "N/A"
    enriched["NECESITA_HIDRATACION"] = str(hidratacion)
    enriched["UBICACION_GEOCLUSTER"] = geocluster
    return enriched

def feature_eda(input_csv: Path, output_csv: Path, logger: logging.Logger) -> None:
    ensure_dir(GRAPHICS_DIR)

    riesgo_counter = Counter()
    hidratacion_counter = Counter()
    geocluster_counter = Counter()
    duracion_buckets = Counter()
    total = 0

    with input_csv.open("r", encoding="utf-8", errors="ignore", newline="") as in_handle, output_csv.open("w", encoding="utf-8", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        new_fields = ["TEXTO_PARA_EMBEDDING", "RIESGO_PII_NIVEL", "DURACION_RESOLUCION_DIAS", "NECESITA_HIDRATACION", "UBICACION_GEOCLUSTER"]
        fieldnames = (reader.fieldnames or []) + new_fields
        writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            enriched = derive_features(row)
            writer.writerow(enriched)

            riesgo_counter[enriched["RIESGO_PII_NIVEL"]] += 1
            hidratacion_counter[enriched["NECESITA_HIDRATACION"]] += 1
            geocluster_counter[enriched["UBICACION_GEOCLUSTER"]] += 1
            
            if enriched["DURACION_RESOLUCION_DIAS"] != "N/A":
                dias = int(enriched["DURACION_RESOLUCION_DIAS"])
                bucket = "0-30" if dias <= 30 else "31-365" if dias <= 365 else "365+"
                duracion_buckets[bucket] += 1

            if total % 100000 == 0:
                logger.info("Features procesadas: %d", total)

    top_geoclusters = geocluster_counter.most_common(10)
    
    write_kv_report(
        GRAPHICS_DIR / "feature_eda_summary.txt",
        "Resumen de Feature Engineering (Trabajo Parcial)",
        [
            ("Archivo de entrada", str(input_csv)),
            ("Archivo de salida", str(output_csv)),
            ("Filas procesadas", total),
        ],
    )
    
    write_text_table(
        GRAPHICS_DIR / "riesgo_pii_table.txt",
        "Distribucion de Riesgo PII",
        ["Nivel", "Frecuencia"],
        sorted(riesgo_counter.items()),
    )
    
    write_text_table(
        GRAPHICS_DIR / "duracion_resolucion_table.txt",
        "Duracion de Resolucion (Buckets)",
        ["Bucket (Dias)", "Frecuencia"],
        sorted(duracion_buckets.items()),
    )

    make_png_bar_chart(
        GRAPHICS_DIR / "riesgo_pii.png",
        "Niveles de Riesgo PII",
        [label for label, _ in sorted(riesgo_counter.items())],
        [value for _, value in sorted(riesgo_counter.items())],
    )
    
    make_png_bar_chart(
        GRAPHICS_DIR / "geoclusters.png",
        "Top 10 Ubicacion Geocluster",
        [label for label, _ in top_geoclusters],
        [value for _, value in top_geoclusters],
    )

    make_png_bar_chart(
        GRAPHICS_DIR / "duracion_resolucion.png",
        "Duracion de Resolucion (Dias)",
        [label for label, _ in sorted(duracion_buckets.items())],
        [value for _, value in sorted(duracion_buckets.items())],
    )

    make_png_bar_chart(
        GRAPHICS_DIR / "necesita_hidratacion.png",
        "Necesidad de Hidratacion (Trigger Scraper)",
        ["No Necesita", "Necesita (1)"] if "1" in hidratacion_counter else [label for label, _ in sorted(hidratacion_counter.items())],
        [value for _, value in sorted(hidratacion_counter.items())],
    )

    logger.info("Feature engineering completado: %d filas", total)

def main() -> None:
    parser = argparse.ArgumentParser(description="EDA de features sobre el dataset unificado")
    parser.add_argument("--input", default="datasets/processed/processed_records.csv", help="CSV unificado")
    parser.add_argument("--output", default="datasets/processed/processed_records_features.csv", help="CSV enriquecido")
    args = parser.parse_args()

    logger = setup_logger("features", GRAPHICS_DIR / "analysis.log")
    feature_eda(Path(args.input), Path(args.output), logger)

if __name__ == "__main__":
    main()