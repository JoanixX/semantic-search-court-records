"""Funciones compartidas para orquestacion, trazabilidad y graficos PNG."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = REPO_ROOT / "evidence"
GRAPHICS_DIR = EVIDENCE_DIR / "graphics"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def setup_logger(name: str, log_path: Path) -> logging.Logger:
    ensure_dir(log_path.parent)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("[%(name)s] %(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def iter_csv_dicts(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def write_text_table(path: Path, title: str, headers: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    ensure_dir(path.parent)
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    lines = [title, ""]
    header_line = " | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * widths[i] for i in range(len(headers)))
    lines.append(header_line)
    lines.append(separator)
    for row in rows:
        lines.append(" | ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_kv_report(path: Path, title: str, items: Sequence[tuple[str, object]]) -> None:
    lines = [title, ""]
    for key, value in items:
        lines.append(f"{key}: {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_png_bar_chart(path: Path, title: str, labels: Sequence[str], values: Sequence[float]) -> None:
    ensure_dir(path.parent)
    from PIL import Image, ImageDraw, ImageFont

    width = 1100
    height = 650
    margin_left = 100
    margin_right = 50
    margin_top = 80
    margin_bottom = 120
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom
    max_value = max(values) if values else 1
    if max_value <= 0:
        max_value = 1
    bar_width = chart_width / max(1, len(values))

    image = Image.new("RGB", (width, height), "#0f172a")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()

    draw.text((width // 2 - 120, 24), title, fill="#e2e8f0", font=font)
    draw.line((margin_left, margin_top, margin_left, margin_top + chart_height), fill="#94a3b8", width=2)
    draw.line((margin_left, margin_top + chart_height, margin_left + chart_width, margin_top + chart_height), fill="#94a3b8", width=2)

    ticks = 5
    for i in range(ticks + 1):
        value = max_value * i / ticks
        y = margin_top + chart_height - (value / max_value) * chart_height
        draw.line((margin_left - 6, y, margin_left, y), fill="#94a3b8", width=1)
        draw.text((margin_left - 85, y - 6), str(int(value)), fill="#cbd5e1", font=font)

    for index, (label, value) in enumerate(zip(labels, values)):
        bar_height = (value / max_value) * chart_height
        x = margin_left + index * bar_width + 8
        y = margin_top + chart_height - bar_height
        w = max(8, bar_width - 16)
        draw.rectangle((x, y, x + w, margin_top + chart_height), fill="#38bdf8")
        draw.text((x, y - 14), str(int(value)), fill="#e2e8f0", font=font)
        draw.text((x, margin_top + chart_height + 8), label[:12], fill="#e2e8f0", font=font)

    image.save(path, format="PNG")


def safe_bucket(value: int, bucket_size: int) -> str:
    start = (value // bucket_size) * bucket_size
    end = start + bucket_size - 1
    return f"{start}-{end}"

