from __future__ import annotations
import json
import re
from scrapers.base_utils import (
    ExtractedDocument, _decode_bytes, _looks_like_endpoint
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
