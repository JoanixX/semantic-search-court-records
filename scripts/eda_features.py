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
    resumen = row.get("RESUMEN_SENTENCIA", "").strip()
    embedding_text = " ".join([
        row.get("CDES_TIPOPROCESO", ""),
        row.get("MATERIA", ""),
        row.get("SUB_MATERIA", ""),
        row.get("ESPECIFICA", ""),
        resumen
    ]).strip()

    # 2. RIESGO_PII_NIVEL: Seguridad / Anonimización (MEJORADO)
    demandante = row.get("TIPO_DEMANDANTE", "").upper().strip()
    demandado = row.get("TIPO_DEMANDADO", "").upper().strip()
    
    # Buscamos patrones de DNI o nombres que podrían haber escapado (simulado con regex básico)
    has_pii_patterns = bool(re.search(r'\d{8}', resumen)) # Ejemplo: DNI de 8 dígitos
    
    riesgo = 0
    if "NATURAL" in demandante or "NATURAL" in demandado:
        riesgo = 3 # Alto riesgo: Involucra personas naturales
    elif has_pii_patterns:
        riesgo = 2 # Riesgo medio: Posibles datos sensibles en texto
    elif "JURIDICA" in demandante or "JURÍDICA" in demandante:
        riesgo = 1 # Bajo riesgo: Entidades jurídicas
    else:
        riesgo = 1 # Por defecto bajo si no hay info clara

    # 3. DURACION_RESOLUCION_DIAS: Eficiencia Procesal
    duracion_dias = -1
    fec_ingreso = row.get("FEC_INGRESO", "").strip()
    pub_pagweb = row.get("PUB_PAGWEB", "").strip()
    
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

    # 4. NECESITA_HIDRATACION: Control de Pipeline (MEJORADO)
    # Se marca como 1 si el resumen es demasiado corto (<100 chars) o tiene palabras clave de "sin datos"
    hidratacion = 0
    if not resumen or resumen in {"--", "N/A", "null", "SIN RESUMEN", "NO ESPECIFICA"}:
        hidratacion = 1
    elif len(resumen) < 100: # Umbral de calidad
        hidratacion = 1
    elif "http" in pub_pagweb.lower() or ".pdf" in pub_pagweb.lower():
        hidratacion = 1

    # 5. UBICACION_GEOCLUSTER: Agrupamiento Jurisdiccional
    geocluster = f"{row.get('DEPARTAMENTO', 'N/A')} | {row.get('DISTRITO', 'N/A')}"

    # --- NUEVAS FEATURES ---
    
    # 6. COMPLEJIDAD_CASO: Basado en materia y longitud de sentencia
    longitud = len(resumen)
    materias_complejas = {"PENAL", "CIVIL", "CONSTITUCIONAL"}
    materia = row.get("MATERIA", "").upper()
    
    score = 1
    if materia in materias_complejas: score += 2
    if longitud > 500: score += 1
    if longitud > 2000: score += 1
    
    # 7. PRIORIDAD_ATENCION: Basado en antigüedad si no se ha resuelto o tomó mucho tiempo
    prioridad = "BAJA"
    if duracion_dias > 365 * 2: # Mas de 2 años
        prioridad = "ALTA"
    elif duracion_dias > 365: # Mas de 1 año
        prioridad = "MEDIA"

    # 8. SENTENCIA_EXTENSA: Flag booleano
    extensa = "1" if longitud > 1500 else "0"

    enriched = dict(row)
    enriched["TEXTO_PARA_EMBEDDING"] = embedding_text
    enriched["RIESGO_PII_NIVEL"] = str(riesgo)
    enriched["DURACION_RESOLUCION_DIAS"] = str(duracion_dias) if duracion_dias >= 0 else "N/A"
    enriched["NECESITA_HIDRATACION"] = str(hidratacion)
    enriched["UBICACION_GEOCLUSTER"] = geocluster
    enriched["COMPLEJIDAD_CASO"] = str(score)
    enriched["PRIORIDAD_ATENCION"] = prioridad
    enriched["SENTENCIA_EXTENSA"] = extensa
    return enriched

def feature_eda(input_csv: Path, output_csv: Path, logger: logging.Logger) -> None:
    output_dir = EVIDENCE_DIR / "features"
    ensure_dir(output_dir)

    riesgo_counter = Counter()
    hidratacion_counter = Counter()
    geocluster_counter = Counter()
    duracion_buckets = Counter()
    complejidad_counter = Counter()
    prioridad_counter = Counter()
    total = 0

    with input_csv.open("r", encoding="utf-8", errors="ignore", newline="") as in_handle, output_csv.open("w", encoding="utf-8", newline="") as out_handle:
        reader = csv.DictReader(in_handle)
        new_fields = [
            "TEXTO_PARA_EMBEDDING", "RIESGO_PII_NIVEL", "DURACION_RESOLUCION_DIAS", 
            "NECESITA_HIDRATACION", "UBICACION_GEOCLUSTER", "COMPLEJIDAD_CASO",
            "PRIORIDAD_ATENCION", "SENTENCIA_EXTENSA"
        ]
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
            complejidad_counter[enriched["COMPLEJIDAD_CASO"]] += 1
            prioridad_counter[enriched["PRIORIDAD_ATENCION"]] += 1
            
            if enriched["DURACION_RESOLUCION_DIAS"] != "N/A":
                dias = int(enriched["DURACION_RESOLUCION_DIAS"])
                bucket = "0-30" if dias <= 30 else "31-365" if dias <= 365 else "365+"
                duracion_buckets[bucket] += 1

            if total % 100000 == 0:
                logger.info("Features procesadas: %d", total)

    top_geoclusters = geocluster_counter.most_common(15)
    
    write_kv_report(
        output_dir / "feature_eda_summary.txt",
        "Resumen de Feature Engineering (Final)",
        [
            ("Archivo de entrada", str(input_csv)),
            ("Archivo de salida", str(output_csv)),
            ("Filas procesadas", total),
        ],
    )
    
    write_text_table(
        output_dir / "hidratacion_table.txt",
        "Distribucion de Necesidad de Hidratacion",
        ["Nivel", "Frecuencia"],
        sorted(hidratacion_counter.items()),
    )

    write_text_table(
        output_dir / "geoclusters_table.txt",
        "Distribucion de Geocluster",
        ["Nivel", "Frecuencia"],
        sorted(geocluster_counter.items()),
    )

    write_text_table(
        output_dir / "riesgo_pii_table.txt",
        "Distribucion de Riesgo PII",
        ["Nivel", "Frecuencia"],
        sorted(riesgo_counter.items()),
    )
    
    write_text_table(
        output_dir / "duracion_resolucion_table.txt",
        "Duracion de Resolucion (Buckets)",
        ["Bucket (Dias)", "Frecuencia"],
        sorted(duracion_buckets.items()),
    )

    write_text_table(
        output_dir / "complejidad_table.txt",
        "Distribucion de Complejidad de Caso",
        ["Nivel (1-5)", "Frecuencia"],
        sorted(complejidad_counter.items()),
    )

    write_text_table(
        output_dir / "prioridad_table.txt",
        "Distribucion de Prioridad de Atencion",
        ["Prioridad", "Frecuencia"],
        sorted(prioridad_counter.items()),
    )

    make_png_bar_chart(
        output_dir / "riesgo_pii.png",
        "Niveles de Riesgo PII (Mejorado)",
        [label for label, _ in sorted(riesgo_counter.items())],
        [value for _, value in sorted(riesgo_counter.items())],
    )
    
    make_png_bar_chart(
        output_dir / "geoclusters.png",
        "Top 15 Ubicacion Geocluster",
        [label for label, _ in top_geoclusters],
        [value for _, value in top_geoclusters],
    )

    make_png_bar_chart(
        output_dir / "complejidad.png",
        "Distribucion de Complejidad",
        [label for label, _ in sorted(complejidad_counter.items())],
        [value for _, value in sorted(complejidad_counter.items())],
    )

    make_png_bar_chart(
        output_dir / "necesita_hidratacion.png",
        "Necesidad de Hidratacion (Criterio Estricto)",
        ["No Necesita", "Necesita (1)"] if "1" in hidratacion_counter else [label for label, _ in sorted(hidratacion_counter.items())],
        [value for _, value in sorted(hidratacion_counter.items())],
    )

    logger.info("Feature engineering completado: %d filas", total)

def main() -> None:
    parser = argparse.ArgumentParser(description="EDA de features sobre el dataset unificado")
    parser.add_argument("--input", default="datasets/processed/processed_records.csv", help="CSV unificado")
    parser.add_argument("--output", default="datasets/processed/processed_records_features.csv", help="CSV enriquecido")
    args = parser.parse_args()

    output_dir = EVIDENCE_DIR / "features"
    ensure_dir(output_dir)
    
    logger = setup_logger("features", output_dir / "analysis.log")
    feature_eda(Path(args.input), Path(args.output), logger)

if __name__ == "__main__":
    main()