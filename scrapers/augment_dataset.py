from __future__ import annotations
import argparse
import csv
import logging
import sys
import re
from pathlib import Path
from typing import Iterable

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from scrapers.base_utils import (
    TARGET_COLUMNS, HarvestedRow, HarvestSummary, DEFAULT_MAX_SHEETS_PER_WORKBOOK,
    DEFAULT_MAX_ROWS_PER_SHEET, MAX_TEXT_SCAN_CHARS, _normalize_token,
    _normalize_field_map
)
from scrapers.crawler_engine import (
    _load_seed_urls, crawl_official_sources, finalize_summary,
    zero_yield_sources, productive_sources, _extract_expediente,
    _field_sample_for_expediente, _has_dataset_schema_fields,
    _process_code_from_expediente, _extract_date, _extract_city,
    _extract_sala, _extract_fallo, _summarize
)

def _build_row(
    url: str,
    text: str,
    source_kind: str,
    html_links: list[str],
    fields: dict[str, str] | None = None,
) -> HarvestedRow | None:
    expediente = _extract_expediente(text)
    field_map = _normalize_field_map(fields or {})
    if not expediente and field_map:
        expediente = _extract_expediente(_field_sample_for_expediente(field_map))
    is_structured_dataset_row = source_kind in {"csv", "xlsx", "json"} and _has_dataset_schema_fields(field_map)
    if not expediente and not is_structured_dataset_row:
        return None

    process_type = _process_code_from_expediente(expediente)
    date = _extract_date(text)
    city = _extract_city(text)
    sala = _extract_sala(text)
    fallo = _extract_fallo(text)
    upper_text = text[:MAX_TEXT_SCAN_CHARS].upper()
    if "AUTO DEL TRIBUNAL CONSTITUCIONAL" in upper_text:
        type_res = "AUTO"
    elif "SENTENCIA DEL TRIBUNAL CONSTITUCIONAL" in upper_text:
        type_res = "SENTENCIA"
    else:
        type_res = ""

    values = {column: "" for column in TARGET_COLUMNS}
    for target_column in TARGET_COLUMNS:
        token = _normalize_token(target_column)
        if token in field_map and field_map[token]:
            values[target_column] = field_map[token]
    if not date and field_map:
        for candidate in ("FECINGRESO", "FECHAINGRESO", "FECHADEINGRESO", "FECHA", "INGRESO", "MES"):
            if candidate in field_map and field_map[candidate]:
                date = field_map[candidate]
                break
    if not city and field_map:
        for candidate in ("PROCEDENCIA", "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "SEDE", "CORTE"):
            if candidate in field_map and field_map[candidate]:
                city = field_map[candidate]
                break
    if not sala and field_map:
        for candidate in ("SALA", "SALAORIGEN", "ORGANO", "PLENO", "INSTANCIA", "COLEGIADO", "JUZGADO"):
            if candidate in field_map and field_map[candidate]:
                sala = field_map[candidate]
                break
    if not fallo and field_map:
        for candidate in ("FALLO", "RESULTADO", "DECISION", "DECISIONFINAL", "ACTOPROCESAL", "SENTENCIA"):
            if candidate in field_map and field_map[candidate]:
                fallo = field_map[candidate]
                break
    if not process_type and field_map:
        for candidate in ("CDESTIPOPROCESO", "TIPOPROCESO", "PROCESO", "MATERIA", "MOTIVOINGRESO", "DELITO"):
            if candidate in field_map and field_map[candidate]:
                process_type = field_map[candidate]
                break
    if not values["FEC_INGRESO"]:
        values["FEC_INGRESO"] = date
    if not values["PROCEDENCIA"]:
        values["PROCEDENCIA"] = city
    if not values["CDES_TIPOPROCESO"]:
        values["CDES_TIPOPROCESO"] = process_type
    if not values["SALA_ORIGEN"]:
        values["SALA_ORIGEN"] = sala
    if not values["SALA"]:
        values["SALA"] = sala
    if not values["FEC_VISTA"]:
        values["FEC_VISTA"] = date
    if not values["MATERIA"]:
        values["MATERIA"] = process_type
    if not values["PUB_PAGWEB"]:
        values["PUB_PAGWEB"] = date
    if not values["PUB_PERUANO"]:
        values["PUB_PERUANO"] = ""
    if not values["TIPO_RESOLUCION"]:
        values["TIPO_RESOLUCION"] = type_res
    if not values["FALLO"]:
        values["FALLO"] = fallo
    if not values["DEPARTAMENTO"]:
        values["DEPARTAMENTO"] = city
    if not values["RESUMEN_SENTENCIA"]:
        values["RESUMEN_SENTENCIA"] = _summarize(text)
    return HarvestedRow(values=values, source_url=url, source_kind=source_kind, expediente=expediente)

def write_harvest_csv(rows: Iterable[HarvestedRow], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TARGET_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.values)

def main() -> int:
    parser = argparse.ArgumentParser(description="Harvest official TC documents and build a real dataset complement")
    parser.add_argument("--manifest", default="datasets/raw/official_sources.txt", help="archivo con seeds oficiales")
    parser.add_argument("--output-csv", default="datasets/raw/official_tc_harvest.csv", help="CSV de salida")
    parser.add_argument("--target-total", type=int, default=1_000_000, help="objetivo minimo de filas")
    parser.add_argument("--max-pages", type=int, default=1000, help="maximo de paginas a visitar")
    parser.add_argument("--timeout", type=int, default=20, help="timeout por request")
    parser.add_argument("--log-file", default="evidence/scraper/prep.log", help="bitacora de preprocesamiento")
    parser.add_argument("--proxy", default="", help="proxy HTTP/HTTPS permitido por la red, por ejemplo http://host:puerto")
    parser.add_argument("--no-proxy-env", action="store_true", help="ignora variables HTTP_PROXY/HTTPS_PROXY del entorno")
    parser.add_argument("--max-sheets-per-workbook", type=int, default=DEFAULT_MAX_SHEETS_PER_WORKBOOK, help="maximo de hojas por XLSX/ODS")
    parser.add_argument("--max-rows-per-sheet", type=int, default=DEFAULT_MAX_ROWS_PER_SHEET, help="maximo de filas por hoja tabular")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    def resolve_repo_path(value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else repo_root / path

    log_path = resolve_repo_path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("official-scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("[%(name)s] %(asctime)s %(levelname)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.propagate = False

    manifest_path = resolve_repo_path(args.manifest)
    seeds = _load_seed_urls(manifest_path if manifest_path.exists() else None)
    logger.info("Seeds oficiales cargadas: %d", len(seeds))
    logger.info("Iniciando crawler oficial con %d paginas maximas", args.max_pages)

    rows, summary = crawl_official_sources(
        seeds,
        args.max_pages,
        args.timeout,
        logger,
        target_total=args.target_total,
        proxy=args.proxy or None,
        trust_env_proxy=not args.no_proxy_env,
        max_sheets=args.max_sheets_per_workbook,
        max_rows_per_sheet=args.max_rows_per_sheet,
    )
    write_harvest_csv(rows, resolve_repo_path(args.output_csv))
    
    warning = finalize_summary(summary, args.target_total)
    if summary.visited_pages >= args.max_pages and summary.harvested_rows < args.target_total:
        page_warning = (
            f"Se alcanzo el limite de exploracion de {args.max_pages} paginas antes de llegar "
            f"a {args.target_total} registros. Aumenta --max-pages o agrega fuentes directas."
        )
        summary.warnings.append(page_warning)
        logger.warning(page_warning)
    if warning:
        logger.warning(warning)
    else:
        logger.info("Se alcanzo el objetivo minimo de registros reales.")

    discarded_sources = zero_yield_sources(summary)
    productive = productive_sources(summary)
    if discarded_sources:
        logger.info("Fuentes descartadas por rendimiento cero: %d", len(discarded_sources))

    report_path = repo_root / "evidence" / "scraper" / "official_harvest_summary.txt"
    warning_lines = [f"- {warning}" for warning in summary.warnings] if summary.warnings else ["- none"]
    report_path.write_text(
        "\n".join(
            [
                "Resumen de harvest oficial",
                "",
                f"Seeds: {summary.seeds}",
                f"Visited pages: {summary.visited_pages}",
                f"Harvested rows: {summary.harvested_rows}",
                f"HTML docs: {summary.html_documents}",
                f"PDF docs: {summary.pdf_documents}",
                f"Tabular docs: {summary.tabular_documents}",
                f"Archive docs: {summary.archive_documents}",
                f"Text docs: {summary.text_documents}",
                f"Skipped non-doc pages: {summary.skipped_non_documents}",
                f"Missing to target: {summary.missing_to_target}",
                f"Zero-yield sources: {len(discarded_sources)}",
                f"Productive sources: {len(productive)}",
                "Warnings:",
                *warning_lines,
                "",
                "Productive sources:",
                *([f"- {count}: {source}" for source, count in productive[:30]] if productive else ["- none"]),
                "",
                "Discarded sources:",
                *([f"- {source}" for source in discarded_sources] if discarded_sources else ["- none"]),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())