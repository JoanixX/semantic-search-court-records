from __future__ import annotations

import argparse
import csv
import json
import io
import logging
import posixpath
import re
import unicodedata
import zipfile
from collections import deque
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from pypdf import PdfReader


TARGET_COLUMNS = [
    "FEC_INGRESO",
    "PROCEDENCIA",
    "CDES_TIPOPROCESO",
    "SALA_ORIGEN",
    "TIPO_DEMANDANTE",
    "TIPO_DEMANDADO",
    "SALA",
    "FEC_VISTA",
    "MATERIA",
    "SUB_MATERIA",
    "ESPECIFICA",
    "PUB_PAGWEB",
    "PUB_PERUANO",
    "TIPO_RESOLUCION",
    "FALLO",
    "FEC_DEVPJ",
    "FEC_DEVPJ_1",
    "DEPARTAMENTO",
    "PROVINCIA",
    "DISTRITO",
    "RESUMEN_SENTENCIA",
]

DEFAULT_SEEDS = [
    "https://www.tc.gob.pe/",
    "https://www.tc.gob.pe/jurisprudencia/",
    "https://www.tc.gob.pe/jurisprudencia/2024/01118-2024-HC.html",
    "https://www.tc.gob.pe/jurisprudencia/2025/00332-2023-AA%20Resolucion.html",
    "https://www.datosabiertos.gob.pe/dataset/expedientes-ingresados-al-tribunal-constitucional-desde-1992-2025-tribunal-constitucional-tc",
]

OFFICIAL_DOMAINS = {"tc.gob.pe", "www.tc.gob.pe", "datosabiertos.gob.pe", "www.datosabiertos.gob.pe"}
USER_AGENT = "semantic-search-court-records/1.0 (+openai; official-doc-harvester)"
MAX_TEXT_SCAN_CHARS = 50_000
MAX_FIELD_VALUE_SCAN_CHARS = 2_000
MAX_FIELD_VALUES_FOR_EXPEDIENTE = 24
DEFAULT_MAX_SHEETS_PER_WORKBOOK = 4
DEFAULT_MAX_ROWS_PER_SHEET = 250_000

PROCESS_CODE_MAP = {
    "AA": "AMPARO",
    "PA": "AMPARO",
    "HC": "HABEAS CORPUS",
    "PHC": "HABEAS CORPUS",
    "AD": "INCONSTITUCIONALIDAD",
    "PC": "CUMPLIMIENTO",
    "Q": "QUEJA",
    "RJ": "RECURSO DE AGRAVIO",
    "EL": "ELECCION",
}

MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "setiembre": 9,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


@dataclass(frozen=True)
class HarvestedRow:
    values: dict[str, str]
    source_url: str
    source_kind: str
    expediente: str


@dataclass
class HarvestSummary:
    seeds: int = 0
    visited_pages: int = 0
    harvested_rows: int = 0
    pdf_documents: int = 0
    html_documents: int = 0
    tabular_documents: int = 0
    archive_documents: int = 0
    text_documents: int = 0
    skipped_non_documents: int = 0
    missing_to_target: int = 0
    warnings: list[str] = None
    source_yields: dict[str, int] = None

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []
        if self.source_yields is None:
            self.source_yields = {}


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


def _normalize_url(url: str) -> str:
    return url.strip().replace("\u200b", "")


def _is_official_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host in OFFICIAL_DOMAINS


def _looks_like_document(text: str) -> bool:
    upper = text.upper()
    return "EXP. N." in upper or "EXP N." in upper or "SENTENCIA DEL TRIBUNAL CONSTITUCIONAL" in upper or "AUTO DEL TRIBUNAL CONSTITUCIONAL" in upper


@lru_cache(maxsize=4096)
def _normalize_token(value: str) -> str:
    snippet = value[:128] if len(value) > 128 else value
    normalized = unicodedata.normalize("NFKD", snippet)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^A-Z0-9]+", "", normalized.upper())[:64]


def _suffix_from_url(url: str) -> str:
    return Path(urlparse(url).path).suffix.lower()


def _is_media_resource(url: str, content_type: str) -> bool:
    suffix = _suffix_from_url(url)
    declared = content_type.lower()
    media_suffixes = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tif", ".tiff", ".mp4", ".mov", ".avi", ".mp3", ".wav", ".ogg"}
    media_types = ("image/", "video/", "audio/")
    return suffix in media_suffixes or any(declared.startswith(prefix) for prefix in media_types)


def _is_probably_html(content_type: str, content: bytes) -> bool:
    declared = content_type.lower()
    if "pdf" in declared:
        return False
    if any(token in declared for token in ("text/html", "application/xhtml+xml", "text/xml", "application/xml", "text/plain")):
        return True
    sample = content[:4096]
    if not sample:
        return False
    if b"\x00" in sample:
        return False
    text_sample = sample.decode("utf-8", errors="ignore").lower()
    return "<html" in text_sample or "<!doctype" in text_sample or "<body" in text_sample or "<head" in text_sample


def _is_probably_zip(content_type: str, content: bytes, url: str) -> bool:
    declared = content_type.lower()
    if "zip" in declared:
        return True
    suffix = _suffix_from_url(url)
    if suffix in {".zip", ".docx", ".xlsx", ".xlsm", ".xltx", ".xltm", ".ods", ".odt"}:
        return True
    return content.startswith(b"PK\x03\x04")


def _is_probably_csv(content_type: str, url: str) -> bool:
    declared = content_type.lower()
    if "csv" in declared or "tsv" in declared:
        return True
    return _suffix_from_url(url) in {".csv", ".tsv"}


def _is_probably_excel(content_type: str, url: str) -> bool:
    declared = content_type.lower()
    if "spreadsheetml" in declared or "excel" in declared:
        return True
    return _suffix_from_url(url) in {".xlsx", ".xlsm", ".xltx", ".xltm"}


def _is_probably_text(content_type: str, content: bytes, url: str) -> bool:
    declared = content_type.lower()
    if any(token in declared for token in ("text/plain", "application/json", "application/ld+json", "application/xml", "text/xml", "text/rtf")):
        return True
    suffix = _suffix_from_url(url)
    if suffix in {".txt", ".json", ".xml", ".md", ".rtf", ".html", ".htm"}:
        return True
    sample = content[:4096]
    if not sample or b"\x00" in sample:
        return False
    text_sample = sample.decode("utf-8", errors="ignore").strip()
    return bool(text_sample)


def _is_probably_json(content_type: str, url: str, content: bytes) -> bool:
    declared = content_type.lower()
    if "json" in declared:
        return True
    return _suffix_from_url(url) == ".json" or _decode_bytes(content).lstrip().startswith(("{", "["))


def _extract_html_text(html: str, base_url: str) -> tuple[str, list[str]]:
    parser = _HTMLCollector(base_url)
    try:
        parser.feed(html)
    except (AssertionError, ValueError, TypeError) as exc:
        raise ValueError(f"HTML invalido o no parseable en {base_url}") from exc
    text = " ".join(parser.parts)
    return re.sub(r"\s+", " ", text).strip(), parser.links


def _extract_pdf_text(data: bytes) -> str:
    reader = PdfReader(io.BytesIO(data))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


@dataclass(frozen=True)
class ExtractedDocument:
    text: str
    source_kind: str
    html_links: list[str]
    fields: dict[str, str]
    source_label: str


def _decode_bytes(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _looks_like_endpoint(value: str) -> bool:
    lowered = value.strip().lower()
    return lowered.startswith(("http://", "https://")) and any(
        lowered.endswith(suffix)
        for suffix in (".json", ".csv", ".tsv", ".xlsx", ".xlsm", ".xltx", ".xltm", ".zip", ".pdf", ".html", ".htm", ".docx")
    )


def _flatten_json(value, prefix: str = "") -> tuple[list[str], list[str]]:
    parts: list[str] = []
    links: list[str] = []

    def visit(node, path: str) -> None:
        if isinstance(node, dict):
            for key, item in node.items():
                next_path = f"{path}.{key}" if path else str(key)
                visit(item, next_path)
        elif isinstance(node, list):
            for index, item in enumerate(node):
                next_path = f"{path}[{index}]" if path else f"[{index}]"
                visit(item, next_path)
        elif isinstance(node, str):
            text = node.strip()
            if not text:
                return
            if _looks_like_endpoint(text):
                links.append(text)
            label = prefix or path
            if label:
                parts.append(f"{label}: {text}")
            else:
                parts.append(text)
        elif node is not None:
            label = prefix or path
            rendered = str(node).strip()
            if rendered:
                if label:
                    parts.append(f"{label}: {rendered}")
                else:
                    parts.append(rendered)

    visit(value, prefix)
    return parts, links


def _normalize_field_map(fields: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for index, (key, value) in enumerate(fields.items()):
        if index >= 256:
            break
        if value is None:
            continue
        key_text = str(key)
        if not key_text or len(key_text) > 256:
            continue
        normalized[_normalize_token(key_text)] = str(value).strip()
    return normalized


def _row_text_from_fields(fields: dict[str, str]) -> str:
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        value_text = str(value).strip()
        if value_text:
            parts.append(f"{key}: {value_text}")
    return " | ".join(parts)


def _extract_text_from_xml_bytes(data: bytes) -> str:
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return _decode_bytes(data)
    parts = [part.strip() for part in root.itertext() if part and part.strip()]
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _json_documents_from_bytes(data: bytes, source_label: str) -> list[ExtractedDocument]:
    raw_text = _decode_bytes(data)
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return [
            ExtractedDocument(
                text=re.sub(r"\s+", " ", raw_text).strip(),
                source_kind="json",
                html_links=[],
                fields={},
                source_label=source_label,
            )
        ]

    documents: list[ExtractedDocument] = []
    if isinstance(payload, dict):
        parts, links = _flatten_json(payload)
        documents.append(
            ExtractedDocument(
                text=re.sub(r"\s+", " ", " | ".join(parts)).strip(),
                source_kind="json",
                html_links=links,
                fields={k: str(v) for k, v in payload.items() if isinstance(v, (str, int, float))},
                source_label=source_label,
            )
        )
        for key in ("data", "results", "items", "records", "dataset", "documents", "resources"):
            value = payload.get(key)
            if isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, dict):
                        item_parts, item_links = _flatten_json(item, key)
                        documents.append(
                            ExtractedDocument(
                                text=re.sub(r"\s+", " ", " | ".join(item_parts)).strip(),
                                source_kind="json",
                                html_links=item_links,
                                fields={k: str(v) for k, v in item.items() if isinstance(v, (str, int, float))},
                                source_label=f"{source_label}#{key}[{index}]",
                            )
                        )
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            if isinstance(item, dict):
                item_parts, item_links = _flatten_json(item, f"[{index}]")
                documents.append(
                    ExtractedDocument(
                        text=re.sub(r"\s+", " ", " | ".join(item_parts)).strip(),
                        source_kind="json",
                        html_links=item_links,
                        fields={k: str(v) for k, v in item.items() if isinstance(v, (str, int, float))},
                        source_label=f"{source_label}#{index}",
                    )
                )
            else:
                rendered = str(item).strip()
                if rendered:
                    documents.append(
                        ExtractedDocument(
                            text=rendered,
                            source_kind="json",
                            html_links=[],
                            fields={},
                            source_label=f"{source_label}#{index}",
                        )
                    )
    return documents


def _csv_rows_from_bytes(data: bytes) -> list[dict[str, str]]:
    text = _decode_bytes(data)
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows: list[dict[str, str]] = []
    for row in reader:
        clean_row = {str(key).strip(): (value.strip() if isinstance(value, str) else "" if value is None else str(value).strip()) for key, value in row.items() if key is not None}
        if any(clean_row.values()):
            rows.append(clean_row)
    return rows


def _xlsx_shared_strings(zf: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    strings: list[str] = []
    for item in root.iter():
        if item.tag.endswith("}si"):
            text = "".join(part for part in item.itertext())
            strings.append(re.sub(r"\s+", " ", text).strip())
    return strings


def _xlsx_sheet_members(zf: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook = "xl/workbook.xml"
    rels = "xl/_rels/workbook.xml.rels"
    if workbook not in zf.namelist():
        return []

    sheet_targets: dict[str, str] = {}
    if rels in zf.namelist():
        rel_root = ET.fromstring(zf.read(rels))
        for rel in rel_root.iter():
            if not rel.tag.endswith("}Relationship"):
                continue
            rel_id = rel.attrib.get("Id", "")
            target = rel.attrib.get("Target", "")
            if rel_id and target:
                sheet_targets[rel_id] = target

    root = ET.fromstring(zf.read(workbook))
    sheets: list[tuple[str, str]] = []
    for sheet in root.iter():
        if not sheet.tag.endswith("}sheet"):
            continue
        name = sheet.attrib.get("name", "Sheet")
        rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
        target = sheet_targets.get(rel_id, "")
        if target:
            target = posixpath.normpath(posixpath.join("xl", target.lstrip("/")))
            sheets.append((name, target))
    if sheets:
        return sheets

    return [
        (name, member)
        for member in sorted(zf.namelist())
        if member.startswith("xl/worksheets/sheet") and member.endswith(".xml")
        for name in [Path(member).stem]
    ]


def _xlsx_column_index(cell_ref: str, fallback: int) -> int:
    match = re.match(r"([A-Z]+)", cell_ref.upper())
    if not match:
        return fallback
    index = 0
    for char in match.group(1):
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def _xlsx_rows_from_bytes(
    data: bytes,
    max_sheets: int = DEFAULT_MAX_SHEETS_PER_WORKBOOK,
    max_rows_per_sheet: int = DEFAULT_MAX_ROWS_PER_SHEET,
) -> list[dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        shared_strings = _xlsx_shared_strings(zf)
        rows: list[dict[str, str]] = []
        for sheet_index, (sheet_name, member) in enumerate(_xlsx_sheet_members(zf)):
            if sheet_index >= max_sheets:
                break
            try:
                sheet_handle = zf.open(member)
            except KeyError:
                continue
            headers: list[str] = []
            header_tokens: list[str] = []
            row_count = 0
            try:
                for _, row in ET.iterparse(sheet_handle, events=("end",)):
                    if not row.tag.endswith("}row"):
                        continue
                    values: list[str] = []
                    for fallback_idx, cell in enumerate(row):
                        if not cell.tag.endswith("}c"):
                            continue
                        column_index = _xlsx_column_index(cell.attrib.get("r", ""), fallback_idx)
                        while len(values) <= column_index:
                            values.append("")
                        cell_type = cell.attrib.get("t", "")
                        value = ""
                        inline_text = ""
                        for part in cell:
                            if part.tag.endswith("}v"):
                                value = (part.text or "").strip()
                            elif part.tag.endswith("}is"):
                                inline_text = " ".join(text.strip() for text in part.itertext() if text and text.strip())
                        if cell_type == "s" and value.isdigit():
                            index = int(value)
                            value = shared_strings[index] if index < len(shared_strings) else ""
                        elif cell_type == "inlineStr" and inline_text:
                            value = inline_text
                        values[column_index] = re.sub(r"\s+", " ", value).strip()
                    if not any(values):
                        row.clear()
                        continue
                    if not headers:
                        headers = values
                        header_tokens = [_normalize_token(header) for header in headers]
                        row.clear()
                        continue
                    row_count += 1
                    record: dict[str, str] = {"__SHEET__": sheet_name, "__ROW__": str(row_count + 1)}
                    for idx, value in enumerate(values):
                        header = headers[idx] if idx < len(headers) else f"COL{idx + 1}"
                        token = header_tokens[idx] if idx < len(header_tokens) else _normalize_token(header)
                        if not token:
                            token = f"COL{idx + 1}"
                        record[header] = value
                        record[token] = value
                    if any(value for key, value in record.items() if not key.startswith("__")):
                        rows.append(record)
                    row.clear()
                    if row_count >= max_rows_per_sheet:
                        break
            finally:
                sheet_handle.close()
        return rows


def _extract_documents_from_zip(
    data: bytes,
    source_label: str,
    logger: logging.Logger,
    depth: int = 0,
    max_sheets: int = DEFAULT_MAX_SHEETS_PER_WORKBOOK,
    max_rows_per_sheet: int = DEFAULT_MAX_ROWS_PER_SHEET,
) -> list[ExtractedDocument]:
    if depth > 3:
        logger.warning("Se omitio un ZIP anidado por profundidad maxima en %s", source_label)
        return []
    documents: list[ExtractedDocument] = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if name.endswith("/"):
                    continue
                suffix = Path(name).suffix.lower()
                if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".mp4", ".mov", ".avi", ".mp3", ".wav", ".ogg"}:
                    continue
                member_bytes = zf.read(name)
                member_label = f"{source_label}!/{name}"
                if suffix in {".zip", ".docx", ".ods", ".odt"}:
                    documents.extend(
                        _extract_documents_from_zip(
                            member_bytes,
                            member_label,
                            logger,
                            depth + 1,
                            max_sheets=max_sheets,
                            max_rows_per_sheet=max_rows_per_sheet,
                        )
                    )
                    continue
                if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
                    for row in _xlsx_rows_from_bytes(member_bytes, max_sheets=max_sheets, max_rows_per_sheet=max_rows_per_sheet):
                        documents.append(
                            ExtractedDocument(
                                text=_row_text_from_fields(row),
                                source_kind="xlsx",
                                html_links=[],
                                fields=row,
                                source_label=member_label,
                            )
                        )
                    continue
                if suffix in {".csv", ".tsv"}:
                    for row in _csv_rows_from_bytes(member_bytes):
                        documents.append(
                            ExtractedDocument(
                                text=_row_text_from_fields(row),
                                source_kind="csv",
                                html_links=[],
                                fields=row,
                                source_label=member_label,
                            )
                        )
                    continue
                if suffix in {".html", ".htm"}:
                    try:
                        text, links = _extract_html_text(_decode_bytes(member_bytes), member_label)
                    except ValueError:
                        continue
                    documents.append(
                        ExtractedDocument(
                            text=text,
                            source_kind="html",
                            html_links=links,
                            fields={},
                            source_label=member_label,
                        )
                    )
                    continue
                if suffix in {".json"}:
                    documents.extend(_json_documents_from_bytes(member_bytes, member_label))
                    continue
                if suffix in {".xml", ".txt", ".json", ".md", ".rtf", ""}:
                    text = _extract_text_from_xml_bytes(member_bytes) if suffix == ".xml" else _decode_bytes(member_bytes)
                    documents.append(
                        ExtractedDocument(
                            text=re.sub(r"\s+", " ", text).strip(),
                            source_kind="text",
                            html_links=[],
                            fields={},
                            source_label=member_label,
                        )
                    )
                    continue
                if suffix == ".pdf":
                    try:
                        text = _extract_pdf_text(member_bytes)
                    except Exception as exc:
                        logger.warning("No se pudo leer PDF en ZIP %s: %s", member_label, exc)
                        continue
                    documents.append(
                        ExtractedDocument(
                            text=text,
                            source_kind="pdf",
                            html_links=[],
                            fields={},
                            source_label=member_label,
                        )
                    )
                    continue
    except zipfile.BadZipFile:
        logger.warning("ZIP invalido o corrupto en %s", source_label)
    return documents


def _extract_documents_from_payload(
    url: str,
    content: bytes,
    content_type: str,
    logger: logging.Logger,
    max_sheets: int = DEFAULT_MAX_SHEETS_PER_WORKBOOK,
    max_rows_per_sheet: int = DEFAULT_MAX_ROWS_PER_SHEET,
) -> list[ExtractedDocument]:
    if _is_media_resource(url, content_type):
        return []

    suffix = _suffix_from_url(url)
    declared = content_type.lower()
    source_label = url

    if "pdf" in declared or suffix == ".pdf" or content.startswith(b"%PDF"):
        try:
            return [
                ExtractedDocument(
                    text=_extract_pdf_text(content),
                    source_kind="pdf",
                    html_links=[],
                    fields={},
                    source_label=source_label,
                )
            ]
        except Exception as exc:
            logger.warning("No se pudo leer PDF %s: %s", url, exc)
            return []

    if _is_probably_excel(content_type, url):
        try:
            rows = _xlsx_rows_from_bytes(content, max_sheets=max_sheets, max_rows_per_sheet=max_rows_per_sheet)
        except zipfile.BadZipFile:
            logger.warning("Excel invalido o corrupto en %s", url)
            return []
        except Exception as exc:
            logger.warning("No se pudo leer Excel %s: %s", url, exc)
            return []
        return [
            ExtractedDocument(
                text=_row_text_from_fields(row),
                source_kind="xlsx",
                html_links=[],
                fields=row,
                source_label=source_label,
            )
            for row in rows
        ]

    if _is_probably_csv(content_type, url):
        rows = _csv_rows_from_bytes(content)
        return [
            ExtractedDocument(
                text=_row_text_from_fields(row),
                source_kind="csv",
                html_links=[],
                fields=row,
                source_label=source_label,
            )
            for row in rows
        ]

    if _is_probably_zip(content_type, content, url):
        return _extract_documents_from_zip(
            content,
            source_label,
            logger,
            max_sheets=max_sheets,
            max_rows_per_sheet=max_rows_per_sheet,
        )

    if _is_probably_json(content_type, url, content):
        return _json_documents_from_bytes(content, source_label)

    if "html" in declared or _is_probably_html(content_type, content):
        try:
            text, links = _extract_html_text(_decode_bytes(content), source_label)
        except ValueError as exc:
            logger.warning("%s", exc)
            return []
        return [
            ExtractedDocument(
                text=text,
                source_kind="html",
                html_links=links,
                fields={},
                source_label=source_label,
            )
        ]

    if _is_probably_text(content_type, content, url):
        text = _extract_text_from_xml_bytes(content) if suffix == ".xml" else _decode_bytes(content)
        return [
            ExtractedDocument(
                text=re.sub(r"\s+", " ", text).strip(),
                source_kind="text",
                html_links=[],
                fields={},
                source_label=source_label,
            )
        ]

    return []


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


def write_harvest_csv(rows: Iterable[HarvestedRow], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TARGET_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.values)


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Harvest official TC documents and build a real dataset complement")
    parser.add_argument("--manifest", default="datasets/raw/official_sources.txt", help="archivo con seeds oficiales")
    parser.add_argument("--output-csv", default="datasets/processed/official_tc_harvest.csv", help="CSV de salida")
    parser.add_argument("--target-total", type=int, default=1_000_000, help="objetivo minimo de filas")
    parser.add_argument("--max-pages", type=int, default=1000, help="maximo de paginas a visitar")
    parser.add_argument("--timeout", type=int, default=20, help="timeout por request")
    parser.add_argument("--log-file", default="evidence/prep.log", help="bitacora de preprocesamiento")
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

    report_path = repo_root / "evidence" / "official_harvest_summary.txt"
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
