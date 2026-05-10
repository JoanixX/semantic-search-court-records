from __future__ import annotations
import argparse
import csv
import json
import io
import logging
import posixpath
import re
import time
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
    from html.parser import HTMLParser
    from html import unescape
    from urllib.parse import urljoin
    class _LocalHTMLCollector(HTMLParser):
        def __init__(self, base_url: str) -> None:
            super().__init__()
            self.base_url = base_url
            self.links: list[str] = []
            self.parts: list[str] = []
            self._skip_depth = 0
            self._in_title = False
        def handle_starttag(self, tag: str, attrs) -> None:
            attrs_dict = dict(attrs)
            if tag in {"script", "style"}: self._skip_depth += 1
            if tag == "title": self._in_title = True
            if tag == "a":
                href = attrs_dict.get("href")
                if href: self.links.append(urljoin(self.base_url, href))
        def handle_endtag(self, tag: str) -> None:
            if tag in {"script", "style"} and self._skip_depth: self._skip_depth -= 1
            if tag == "title": self._in_title = False
        def handle_data(self, data: str) -> None:
            if self._skip_depth: return
            text = data.strip()
            if text: self.parts.append(unescape(text))
    parser = _LocalHTMLCollector(base_url)
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
        try: return data.decode(encoding)
        except UnicodeDecodeError: continue
    return data.decode("utf-8", errors="ignore")

def _looks_like_endpoint(value: str) -> bool:
    lowered = value.strip().lower()
    return lowered.startswith(("http://", "https://")) and any(lowered.endswith(suffix) for suffix in (".json", ".csv", ".tsv", ".xlsx", ".xlsm", ".xltx", ".xltm", ".zip", ".pdf", ".html", ".htm", ".docx"))

def _normalize_field_map(fields: dict[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for index, (key, value) in enumerate(fields.items()):
        if index >= 256: break
        if value is None: continue
        key_text = str(key)
        if not key_text or len(key_text) > 256: continue
        normalized[_normalize_token(key_text)] = str(value).strip()
    return normalized

def _row_text_from_fields(fields: dict[str, str]) -> str:
    parts = []
    for key, value in fields.items():
        if value is None: continue
        value_text = str(value).strip()
        if value_text: parts.append(f"{key}: {value_text}")
    return " | ".join(parts)

def _extract_text_from_xml_bytes(data: bytes) -> str:
    try: root = ET.fromstring(data)
    except ET.ParseError: return _decode_bytes(data)
    parts = [part.strip() for part in root.itertext() if part and part.strip()]
    return re.sub(r"\s+", " ", " ".join(parts)).strip()
