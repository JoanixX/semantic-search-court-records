import io
import json
import zipfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pypdf import PdfReader

from scrapers.augment_dataset import (
    HarvestSummary,
    _build_row,
    _extract_documents_from_payload,
    _extract_html_text,
    _extract_pdf_text,
    _is_probably_html,
    crawl_official_sources,
    finalize_summary,
    zero_yield_sources,
)


def build_minimal_pdf_bytes(text: str) -> bytes:
    def obj(num: int, body: str) -> bytes:
        return f"{num} 0 obj\n{body}\nendobj\n".encode("latin1")

    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    stream = f"BT /F1 18 Tf 72 720 Td ({escaped}) Tj ET"
    header = b"%PDF-1.4\n"
    objects = [
        obj(1, "<< /Type /Catalog /Pages 2 0 R >>"),
        obj(2, "<< /Type /Pages /Kids [3 0 R] /Count 1 >>"),
        obj(
            3,
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R "
            "/Resources << /Font << /F1 5 0 R >> >> >>",
        ),
        obj(4, f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream"),
        obj(5, "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
    ]

    offsets = [0]
    position = len(header)
    for object_bytes in objects:
        offsets.append(position)
        position += len(object_bytes)

    xref_start = position
    xref_lines = ["xref", "0 6", "0000000000 65535 f "]
    for offset in offsets[1:]:
        xref_lines.append(f"{offset:010d} 00000 n ")
    trailer = "trailer << /Size 6 /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n".format(start=xref_start)
    body = header + b"".join(objects) + ("\n".join(xref_lines) + "\n" + trailer).encode("latin1")
    return body


def build_minimal_xlsx_bytes() -> bytes:
    buffer = io.BytesIO()
    shared_strings = [
        "EXPEDIENTE",
        "PROCEDENCIA",
        "FEC_INGRESO",
        "FALLO",
        "01118-2024-PHC/TC",
        "CUSCO",
        "2024-12-12",
        "IMPROCEDENTE",
    ]
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>
"""
    shared_strings_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="8" uniqueCount="8">
{items}
</sst>
""".format(
        items="\n".join(f"  <si><t>{value}</t></si>" for value in shared_strings)
    )
    sheet = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="s"><v>0</v></c>
      <c r="B1" t="s"><v>1</v></c>
      <c r="C1" t="s"><v>2</v></c>
      <c r="D1" t="s"><v>3</v></c>
    </row>
    <row r="2">
      <c r="A2" t="s"><v>4</v></c>
      <c r="B2" t="s"><v>5</v></c>
      <c r="C2" t="s"><v>6</v></c>
      <c r="D2" t="s"><v>7</v></c>
    </row>
  </sheetData>
</worksheet>
"""
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", rels)
        zf.writestr("xl/sharedStrings.xml", shared_strings_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet)
    return buffer.getvalue()


def build_zip_with_csv_bytes() -> bytes:
    buffer = io.BytesIO()
    csv_bytes = "EXPEDIENTE,PROCEDENCIA,FALLO\n00999-2024-PA/TC,LIMA,FUNDADA\n".encode("utf-8")
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("folder/resultados.csv", csv_bytes)
    return buffer.getvalue()


def build_json_payload_bytes() -> bytes:
    payload = {
        "dataset": "tribunal constitucional",
        "download_url": "https://www.datosabiertos.gob.pe/sites/default/files/dataset1_20250509.xlsx",
        "resources": [
            {
                "format": "csv",
                "url": "https://www.datosabiertos.gob.pe/sites/default/files/dataset1_20250509.csv",
            }
        ],
        "records": [
            {
                "EXPEDIENTE": "01118-2024-PHC/TC",
                "PROCEDENCIA": "CUSCO",
                "FALLO": "IMPROCEDENTE",
            }
        ],
    }
    return json.dumps(payload).encode("utf-8")


class FakeResponse:
    def __init__(self, url: str, *, text: str = "", content: bytes = b"", content_type: str = "text/html") -> None:
        self.url = url
        self.text = text
        self.content = content or text.encode("utf-8")
        self.headers = {"content-type": content_type}

    def raise_for_status(self) -> None:
        return None


class OfficialScraperTests(unittest.TestCase):
    def test_probably_html_filters_binary_payloads(self):
        self.assertFalse(_is_probably_html("application/octet-stream", b"\x00\x01\x02\x03"))
        self.assertTrue(_is_probably_html("text/html; charset=utf-8", b"<html><body>Hola</body></html>"))

    def test_extract_html_text_and_build_row(self):
        html = """
        <html>
          <head><title>Sentencia</title></head>
          <body>
            <h1>Sala Segunda. Sentencia 1740/2024</h1>
            <p>EXP. N.° 01118-2024-PHC/TC</p>
            <p>CUSCO</p>
            <p>En Lima, a los 12 días del mes de diciembre de 2024</p>
            <p>HA RESUELTO: Declarar IMPROCEDENTE la demanda.</p>
          </body>
        </html>
        """
        text, links = _extract_html_text(html, "https://www.tc.gob.pe/jurisprudencia/2024/01118-2024-HC.html")
        row = _build_row("https://www.tc.gob.pe/jurisprudencia/2024/01118-2024-HC.html", text, "html", links)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.expediente, "01118-2024-PHC")
        self.assertEqual(row.values["CDES_TIPOPROCESO"], "HABEAS CORPUS")
        self.assertEqual(row.values["PROCEDENCIA"], "CUSCO")
        self.assertEqual(row.values["FEC_VISTA"], "2024-12-12")
        self.assertEqual(row.values["FALLO"], "IMPROCEDENTE")
        self.assertEqual(row.values["PUB_PAGWEB"], "2024-12-12")

    def test_extract_pdf_text_from_minimal_pdf(self):
        pdf_bytes = build_minimal_pdf_bytes("EXP. N.° 00999-2024-PA/TC\nLIMA\nHA RESUELTO: FUNDADA la demanda.")
        text = _extract_pdf_text(pdf_bytes)
        self.assertIn("00999-2024-PA/TC", text)
        self.assertIn("FUNDADA", text)

    def test_extract_zip_with_csv_payload(self):
        zip_bytes = build_zip_with_csv_bytes()
        documents = _extract_documents_from_payload(
            "https://www.tc.gob.pe/datos/resultados.zip",
            zip_bytes,
            "application/zip",
            __import__("logging").getLogger("test"),
        )
        self.assertEqual(len(documents), 1)
        self.assertIn("00999-2024-PA/TC", documents[0].text)
        self.assertEqual(documents[0].fields["EXPEDIENTE"], "00999-2024-PA/TC")

    def test_extract_xlsx_payload(self):
        xlsx_bytes = build_minimal_xlsx_bytes()
        documents = _extract_documents_from_payload(
            "https://www.tc.gob.pe/datos/resultados.xlsx",
            xlsx_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            __import__("logging").getLogger("test"),
        )
        self.assertEqual(len(documents), 1)
        self.assertIn("01118-2024-PHC/TC", documents[0].text)
        self.assertEqual(documents[0].fields["EXPEDIENTE"], "01118-2024-PHC/TC")

    def test_build_row_accepts_dataset_schema_without_expediente(self):
        fields = {
            "FEC_INGRESO": "2026-02-23",
            "PROCEDENCIA": "LIMA",
            "CDES_TIPOPROCESO": "AMPARO",
            "FALLO": "FUNDADA",
        }
        row = _build_row("https://www.datosabiertos.gob.pe/dataset.csv", "", "csv", [], fields)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.expediente, "")
        self.assertEqual(row.values["PROCEDENCIA"], "LIMA")
        self.assertEqual(row.values["CDES_TIPOPROCESO"], "AMPARO")
        self.assertEqual(row.values["FALLO"], "FUNDADA")

    def test_extract_json_payload_and_endpoints(self):
        json_bytes = build_json_payload_bytes()
        documents = _extract_documents_from_payload(
            "https://www.datosabiertos.gob.pe/api/dataset.json",
            json_bytes,
            "application/json",
            __import__("logging").getLogger("test"),
        )
        self.assertGreaterEqual(len(documents), 1)
        self.assertTrue(any("dataset1_20250509.xlsx" in link for link in documents[0].html_links))
        self.assertTrue(any("01118-2024-PHC/TC" in doc.text for doc in documents))

    def test_crawl_official_sources_warns_when_target_not_met(self):
        html = """
        <html>
          <body>
            <h1>Sal Segunda. Sentencia 1740/2024</h1>
            <p>EXP. N.° 01118-2024-PHC/TC</p>
            <p>CUSCO</p>
            <p>En Lima, a los 12 días del mes de diciembre de 2024</p>
            <p>HA RESUELTO: Declarar IMPROCEDENTE la demanda.</p>
          </body>
        </html>
        """

        def fake_get(self, url, timeout=20):
            if url.endswith(".pdf"):
                pdf_bytes = build_minimal_pdf_bytes("EXP. N.° 00999-2024-PA/TC\nLIMA\nHA RESUELTO: FUNDADA la demanda.")
                return FakeResponse(url, content=pdf_bytes, content_type="application/pdf")
            return FakeResponse(url, text=html)

        with patch("requests.Session.get", new=fake_get):
            rows, summary = crawl_official_sources(
                ["https://www.tc.gob.pe/jurisprudencia/2024/01118-2024-HC.html"],
                max_pages=1,
                timeout=5,
                logger=__import__("logging").getLogger("test"),
            )

        self.assertEqual(len(rows), 1)
        warning = finalize_summary(summary, target_total=2)
        self.assertIsNotNone(warning)
        self.assertIn("faltan 1", warning)
        self.assertEqual(summary.harvested_rows, 1)

    def test_crawl_official_sources_skips_malformed_html(self):
        malformed = "<![\x005z\x00R\x00\x00 broken html"

        def fake_get(self, url, timeout=20):
            return FakeResponse(url, text=malformed, content=malformed.encode("utf-8", errors="ignore"), content_type="text/html")

        with patch("requests.Session.get", new=fake_get):
            rows, summary = crawl_official_sources(
                ["https://www.tc.gob.pe/jurisprudencia/2024/01118-2024-HC.html"],
                max_pages=1,
                timeout=5,
                logger=__import__("logging").getLogger("test"),
            )

        self.assertEqual(rows, [])
        self.assertGreaterEqual(summary.skipped_non_documents, 1)
        self.assertTrue(any("html parse failed" in warning for warning in summary.warnings))

    def test_crawl_official_sources_reads_xlsx_and_csv_zip(self):
        zip_bytes = build_zip_with_csv_bytes()
        xlsx_bytes = build_minimal_xlsx_bytes()

        def fake_get(self, url, timeout=20):
            if url.endswith(".zip"):
                return FakeResponse(url, content=zip_bytes, content_type="application/zip")
            if url.endswith(".xlsx"):
                return FakeResponse(
                    url,
                    content=xlsx_bytes,
                    content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            return FakeResponse(url, text="<html><body><p>EXP. N.Â° 01118-2024-PHC/TC</p></body></html>")

        with patch("requests.Session.get", new=fake_get):
            rows, summary = crawl_official_sources(
                [
                    "https://www.tc.gob.pe/datos/resultados.zip",
                    "https://www.tc.gob.pe/datos/resultados.xlsx",
                ],
                max_pages=2,
                timeout=5,
                logger=__import__("logging").getLogger("test"),
            )

        self.assertGreaterEqual(len(rows), 2)
        self.assertGreaterEqual(summary.tabular_documents, 2)
        self.assertTrue(any(row.expediente for row in rows))

    def test_crawl_official_sources_reads_json_endpoints(self):
        json_bytes = build_json_payload_bytes()

        def fake_get(self, url, timeout=20):
            if url.endswith(".json"):
                return FakeResponse(url, content=json_bytes, content_type="application/json")
            return FakeResponse(url, text="<html><body><p>EXP. N.Â° 01118-2024-PHC/TC</p></body></html>")

        with patch("requests.Session.get", new=fake_get):
            rows, summary = crawl_official_sources(
                ["https://www.datosabiertos.gob.pe/api/dataset.json"],
                max_pages=2,
                timeout=5,
                logger=__import__("logging").getLogger("test"),
            )

        self.assertGreaterEqual(len(rows), 1)
        self.assertGreaterEqual(summary.text_documents, 1)

    def test_zero_yield_sources_reports_empty_sources(self):
        summary = HarvestSummary()
        summary.source_yields = {
            "https://www.tc.gob.pe/a": 0,
            "https://www.tc.gob.pe/b": 3,
        }
        discarded = zero_yield_sources(summary)
        self.assertEqual(discarded, ["https://www.tc.gob.pe/a"])


if __name__ == "__main__":
    unittest.main()
