import csv
import tempfile
from pathlib import Path
import unittest

from scrapers.augment_dataset import augment_dataset, discover_documents, load_existing_keys


class AugmentDatasetTests(unittest.TestCase):
    def test_discover_documents_reads_txt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            docs = root / "docs"
            docs.mkdir()
            (docs / "a.txt").write_text("Primera sentencia. Segunda parte.", encoding="utf-8")

            found = discover_documents(docs)
            self.assertEqual(len(found), 2)
            self.assertTrue(any("Primera sentencia" in chunk.text for chunk in found))

    def test_augment_dataset_creates_only_missing_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            existing = root / "existing.csv"
            source_dir = root / "docs"
            output = root / "out.csv"
            source_dir.mkdir()
            (source_dir / "source.txt").write_text("Texto base para complementar.", encoding="utf-8")

            with existing.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=[
                    "FEC_INGRESO", "PROCEDENCIA", "CDES_TIPOPROCESO", "SALA_ORIGEN",
                    "TIPO_DEMANDANTE", "TIPO_DEMANDADO", "SALA", "FEC_VISTA", "MATERIA",
                    "SUB_MATERIA", "ESPECIFICA", "PUB_PAGWEB", "PUB_PERUANO", "TIPO_RESOLUCION",
                    "FALLO", "FEC_DEVPJ", "FEC_DEVPJ_1", "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
                    "RESUMEN_SENTENCIA",
                ])
                writer.writeheader()
                writer.writerow({
                    "FEC_INGRESO": "2026-01-01",
                    "PROCEDENCIA": "LIMA",
                    "CDES_TIPOPROCESO": "AMPARO",
                    "SALA_ORIGEN": "SALA 1",
                    "TIPO_DEMANDANTE": "NATURAL",
                    "TIPO_DEMANDADO": "JURIDICA",
                    "SALA": "PLENO",
                    "FEC_VISTA": "2026-01-15",
                    "MATERIA": "CONSTITUCIONAL",
                    "SUB_MATERIA": "DERECHOS",
                    "ESPECIFICA": "NO ESPECIFICADO",
                    "PUB_PAGWEB": "local://seed/1",
                    "PUB_PERUANO": "seed",
                    "TIPO_RESOLUCION": "SENTENCIA",
                    "FALLO": "FUNDADO",
                    "FEC_DEVPJ": "2026-01-16",
                    "FEC_DEVPJ_1": "2026-01-17",
                    "DEPARTAMENTO": "LIMA",
                    "PROVINCIA": "LIMA",
                    "DISTRITO": "LIMA",
                    "RESUMEN_SENTENCIA": "texto existente",
                })

            created = augment_dataset(existing, source_dir, output, target_total=3)
            self.assertEqual(created, 2)

            with output.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertTrue(all(row["RESUMEN_SENTENCIA"] for row in rows))

    def test_load_existing_keys_handles_missing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing.csv"
            self.assertEqual(load_existing_keys(missing), set())


if __name__ == "__main__":
    unittest.main()

