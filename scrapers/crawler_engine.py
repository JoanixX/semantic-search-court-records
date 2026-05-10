from __future__ import annotations
import logging
import re
import requests
from collections import deque
from pathlib import Path
from typing import Iterable
from html.parser import HTMLParser
from html import unescape
from urllib.parse import urljoin
from scrapers.base_utils import (
    MONTHS, MAX_TEXT_SCAN_CHARS, PROCESS_CODE_MAP, MAX_FIELD_VALUE_SCAN_CHARS,
    MAX_FIELD_VALUES_FOR_EXPEDIENTE, TARGET_COLUMNS, _normalize_token,
    HarvestedRow, HarvestSummary, _normalize_field_map,
    _normalize_url, _is_official_url, _suffix_from_url, _looks_like_document,
    USER_AGENT, DEFAULT_MAX_SHEETS_PER_WORKBOOK, DEFAULT_MAX_ROWS_PER_SHEET,
    DEFAULT_SEEDS
)
from scrapers.extraction_logic import (
    _extract_documents_from_payload, _is_probably_zip
)

class _HTMLCollector(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[str] = []
        self.parts: list[str] = []
        self._skip_depth = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs_dict = dict(attrs)
        if tag in {"script", "style"}:
            self._skip_depth += 1
        if tag == "title":
            self._in_title = True
        if tag == "a":
            href = attrs_dict.get("href")
            if href:
                self.links.append(urljoin(self.base_url, href))

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        text = data.strip()
        if text:
            self.parts.append(unescape(text))

def _extract_date(text: str) -> str:
    patterns = [
        r"En\s+[A-Za-zÁÉÍÓÚÑáéíóúñ]+,\s+a\s+los\s+(\d{1,2})\s+d[ií]as?\s+del\s+mes\s+de\s+([A-Za-zÁÉÍÓÚÑáéíóúñ]+)\s+de\s+(\d{4})",
        r"(\d{1,2})\s+de\s+([A-Za-zÁÉÍÓÚÑáéíóúñ]+)\s+de\s+(\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            day = int(match.group(1))
            month = MONTHS.get(match.group(2).lower(), 0)
            year = int(match.group(3))
            if month:
                return f"{year:04d}-{month:02d}-{day:02d}"
    return ""

def _extract_expediente(text: str) -> str:
    text = text[:MAX_TEXT_SCAN_CHARS]
    patterns = [
        r"EXP\.\s*N\.?[°º]?\s*([0-9]{3,5}-[0-9]{4}-[A-Z]{2,4})",
        r"EXPEDIENTE\s+([0-9]{3,5}-[0-9]{4}-[A-Z]{2,4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).upper().replace("/TC", "")
    fallback = re.search(r"([0-9]{3,5}-[0-9]{4}-[A-Z]{2,4}(?:/TC)?)", text, flags=re.IGNORECASE)
    if fallback:
        return fallback.group(1).upper().replace("/TC", "")
    return ""

def _process_code_from_expediente(expediente: str) -> str:
    if not expediente:
        return ""
    match = re.search(r"-([A-Z]{2,4})(?:/TC)?$", expediente.upper())
    if not match:
        return ""
    code = match.group(1)
    return PROCESS_CODE_MAP.get(code, code)

def _extract_sala(text: str) -> str:
    match = re.search(r"^(Sala [A-Za-zÁÉÍÓÚÑáéíóúñ ]+|PLENO)", text.strip(), flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"(Sala [A-Za-zÁÉÍÓÚÑáéíóúñ ]+|Pleno)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return ""

def _extract_fallo(text: str) -> str:
    relevant = text
    for marker in ["HA RESUELTO", "RESUELVE", "DECIDE"]:
        idx = relevant.upper().find(marker)
        if idx >= 0:
            relevant = relevant[idx:]
            break
    match = re.search(r"(IMPROCEDENTE|INFUNDAD[OA]|FUNDAD[OA]|NUL[OA]|RECHAZAD[OA]|INADMISIBLE)", relevant, flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""

def _extract_city(text: str) -> str:
    match = re.search(r"TC\s+([A-ZÁÉÍÓÚÑ]{3,})\s+", text)
    if match:
        return match.group(1).upper()
    match = re.search(r"^(LIMA|CUSCO|AREQUIPA|PIURA|TRUJILLO|PUNO|CALLAO|ICA|JUNIN|HUANUCO|LORETO|TACNA)", text.strip(), flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""

def _extract_pdf_link(html_links: list[str]) -> str:
    for link in html_links:
        if link.lower().endswith(".pdf"):
            return link
    return ""

def _summarize(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    return cleaned[:1200]

def _first_field_value(field_map: dict[str, str], candidates: Iterable[str]) -> str:
    for candidate in candidates:
        value = field_map.get(candidate, "").strip()
        if value:
            return value[:MAX_FIELD_VALUE_SCAN_CHARS]
    return ""

def _field_sample_for_expediente(field_map: dict[str, str]) -> str:
    preferred = _first_field_value(
        field_map,
        (
            "EXPEDIENTE",
            "NEXPEDIENTE",
            "NUMEXPEDIENTE",
            "NUMEROEXPEDIENTE",
            "CODIGOEXPEDIENTE",
            "EXP",
        ),
    )
    if preferred:
        return preferred

    parts: list[str] = []
    for value in field_map.values():
        if not value:
            continue
        parts.append(value[:MAX_FIELD_VALUE_SCAN_CHARS])
        if len(parts) >= MAX_FIELD_VALUES_FOR_EXPEDIENTE:
            break
    return " ".join(parts)[:MAX_TEXT_SCAN_CHARS]

def _has_dataset_schema_fields(field_map: dict[str, str], minimum: int = 3) -> bool:
    present = 0
    for column in TARGET_COLUMNS:
        if field_map.get(_normalize_token(column), "").strip():
            present += 1
        if present >= minimum:
            return True
    return False

def _load_seed_urls(manifest_path: Path | None) -> list[str]:
    if manifest_path and manifest_path.exists():
        urls = []
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(_normalize_url(line))
        return urls
    return list(DEFAULT_SEEDS)

def _follow_link(link: str) -> bool:
    link = link.lower()
    relevant_terms = (
        "tribunal-constitucional",
        "tc.gob.pe",
        "corte-superior",
        "csjpiura",
        "expedientes-judiciales",
        "expediente",
        "demandas",
        "sentencias",
        "justicia",
        "judicial",
        "alimentos",
        "violencia",
        "nlpt",
        "pcalp",
        "requisitorias",
        "notificaciones",
    )
    is_relevant_open_data = "datosabiertos.gob.pe" in link and any(term in link for term in relevant_terms)
    return (
        "tc.gob.pe/jurisprudencia" in link
        or link.endswith(".pdf")
        or link.endswith(".html")
        or link.endswith(".zip")
        or link.endswith(".csv")
        or link.endswith(".tsv")
        or link.endswith(".xlsx")
        or link.endswith(".xlsm")
        or link.endswith(".xltx")
        or link.endswith(".xltm")
        or link.endswith(".docx")
        or link.endswith(".json")
        or "/api/" in link
        or "action=" in link
        or (is_relevant_open_data and "datosabiertos.gob.pe/dataset" in link)
        or (is_relevant_open_data and "datosabiertos.gob.pe/node" in link)
        or (is_relevant_open_data and "datosabiertos.gob.pe/search" in link)
    )

def crawl_official_sources(
    seed_urls: Iterable[str],
    max_pages: int,
    timeout: int,
    logger: logging.Logger,
    target_total: int | None = None,
    proxy: str | None = None,
    trust_env_proxy: bool = True,
    max_sheets: int = DEFAULT_MAX_SHEETS_PER_WORKBOOK,
    max_rows_per_sheet: int = DEFAULT_MAX_ROWS_PER_SHEET,
) -> tuple[list[HarvestedRow], HarvestSummary]:
    from scrapers.augment_dataset import _build_row
    session = requests.Session()
    session.trust_env = trust_env_proxy
    session.headers.update({"User-Agent": USER_AGENT})
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    queue = deque(_normalize_url(url) for url in seed_urls if url)
    seen: set[str] = set()
       
    harvested: list[HarvestedRow] = []
    summary = HarvestSummary(seeds=len(queue))

    while queue and summary.visited_pages < max_pages:
        current = queue.popleft()
        if current in seen or not _is_official_url(current):
            continue
        seen.add(current)
        
        summary.visited_pages += 1
        source_key = current
        before_count = summary.harvested_rows

        try:
            response = session.get(current, timeout=timeout)
            response.raise_for_status()
        except Exception as exc:
            logger.warning("No se pudo obtener %s: %s", current, exc)
            summary.warnings.append(f"fetch failed: {current}")
            summary.source_yields[source_key] = summary.source_yields.get(source_key, 0)
            continue

        content_type = response.headers.get("content-type", "").lower()
        final_url = response.url
        documents = _extract_documents_from_payload(
            final_url,
            response.content,
            content_type,
            logger,
            max_sheets=max_sheets,
            max_rows_per_sheet=max_rows_per_sheet,
        )
        if not documents:
            summary.skipped_non_documents += 1
            if "html" in content_type or _suffix_from_url(final_url) in {".html", ".htm"}:
                logger.warning("HTML invalido o no parseable en %s", final_url)
                summary.warnings.append(f"html parse failed: {final_url}")
            else:
                logger.warning("No se pudo extraer contenido util de %s", final_url)
                summary.warnings.append(f"unsupported content skipped: {final_url}")
            continue

        if any(document.source_kind == "pdf" for document in documents):
            summary.pdf_documents += sum(1 for document in documents if document.source_kind == "pdf")
        if any(document.source_kind == "html" for document in documents):
            summary.html_documents += sum(1 for document in documents if document.source_kind == "html")
        if any(document.source_kind in {"csv", "xlsx"} for document in documents):
            summary.tabular_documents += sum(1 for document in documents if document.source_kind in {"csv", "xlsx"})
        if any(document.source_kind == "json" for document in documents):
            summary.text_documents += sum(1 for document in documents if document.source_kind == "json")
        if any(document.source_kind == "text" for document in documents):
            summary.text_documents += sum(1 for document in documents if document.source_kind == "text")
        if _is_probably_zip(content_type, response.content, final_url):
            summary.archive_documents += 1

        for document in documents:
            for link in document.html_links:
                link = _normalize_url(link)
                if _is_official_url(link) and _follow_link(link) and link not in seen:
                    queue.append(link)
            if document.source_kind == "html":
                if not _looks_like_document(document.text):
                    summary.skipped_non_documents += 1
                    continue

            row = _build_row(final_url, document.text, document.source_kind, document.html_links, document.fields)
            if row:
                harvested.append(row)
                summary.harvested_rows += 1
                if target_total is not None and summary.harvested_rows >= target_total:
                    gained = summary.harvested_rows - before_count
                    summary.source_yields[source_key] = summary.source_yields.get(source_key, 0) + gained
                    return harvested, summary

        gained = summary.harvested_rows - before_count
        summary.source_yields[source_key] = summary.source_yields.get(source_key, 0) + gained

    return harvested, summary

def finalize_summary(summary: HarvestSummary, target_total: int) -> str | None:
    if summary.harvested_rows >= target_total:
        summary.missing_to_target = 0
        return None

    summary.missing_to_target = target_total - summary.harvested_rows
    warning = (
        f"Solo se obtuvieron {summary.harvested_rows} registros oficiales reales; "
        f"faltan {summary.missing_to_target} para llegar a {target_total}."
    )
    summary.warnings.append(warning)
    return warning

def zero_yield_sources(summary: HarvestSummary) -> list[str]:
    return sorted(source for source, yielded in summary.source_yields.items() if yielded <= 0)

def productive_sources(summary: HarvestSummary) -> list[tuple[str, int]]:
    return sorted(
        ((source, yielded) for source, yielded in summary.source_yields.items() if yielded > 0),
        key=lambda item: item[1],
        reverse=True,
    )
