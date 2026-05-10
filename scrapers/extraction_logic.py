from __future__ import annotations
import csv
import io
import logging
import posixpath
import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET
from scrapers.base_utils import (
    ExtractedDocument, _decode_bytes, _normalize_token,
    DEFAULT_MAX_SHEETS_PER_WORKBOOK, DEFAULT_MAX_ROWS_PER_SHEET,
    _row_text_from_fields, _extract_text_from_xml_bytes, _extract_pdf_text,
    _extract_html_text, _is_media_resource, _suffix_from_url, _is_probably_excel,
    _is_probably_csv, _is_probably_zip, _is_probably_json, _is_probably_html,
    _is_probably_text
)
from scrapers.json_logic import _json_documents_from_bytes

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
