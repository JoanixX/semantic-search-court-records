# Semantic Search Court Records

Proyecto para el curso de Programacion Concurrente y Distribuida.

La base del trabajo se enfoca en un pipeline concurrente en Go para limpieza y anonimizacion de expedientes judiciales, mas un scraper en Python para complementar el dataset sin sobrescribir lo ya cargado.

## Flujo recomendado

El orden correcto del proyecto es:

1. Ejecutar tests obligatorios.
2. Analizar el dataset original y generar solo analisis basico.
3. Ejecutar el scraper para complementar datos hasta superar 1M.
4. Fusionar y validar el dataset combinado.
5. Si el umbral se cumple, ejecutar feature engineering y su EDA.
6. Finalmente correr el pipeline Go y el benchmark Go con logs y tablas.

El orquestador principal es:

```bash
python scripts/run_workflow.py
```

## Entry points reales

Hay dos entrypoints Go reales y eso es intencional:

- `cmd/pipeline/main.go` procesa el dataset real.
- `cmd/benchmark/main.go` ejecuta la simulacion secuencial vs concurrente.

Separarlos evita mezclar carga real con carga de medicion y mantiene mas clara la trazabilidad del informe. Los `main.go` dentro de `notebooks/` quedaron como artefactos historicos y estan excluidos del build con `//go:build ignore`.

## Como ejecutar por partes

1. Tests:

```bash
go test ./tests/unit ./tests/integration
python -m unittest discover -s tests/python -p "test_*.py"
```

2. EDA original:

```bash
python scripts/eda_original.py --input datasets/raw/dataset.csv
```

3. Scraper:

```bash
python scrapers/augment_dataset.py --target-total 1000000
```

4. Fusion y validacion:

```bash
python scripts/merge_datasets.py
python scripts/validate_dataset.py --target 1000000
```

5. Feature engineering:

```bash
python scripts/eda_features.py
```

6. Go real:

```bash
go run ./cmd/pipeline -csv datasets/raw/expedientes_tc_masivo.csv -workers 8 -delay-ms 1 -log-every 5000
go run ./cmd/benchmark -records 10000 -runs 3 -delay-ms 2
```

## Logs y trazabilidad

El flujo genera cuatro logs agrupados con secciones internas:

- `evidence/tests.log`
- `evidence/analysis.log`
- `evidence/prep.log`
- `evidence/go.log`

Ademas, `evidence/workflow.log` conserva la bitacora maestra del orquestador.

## Documentacion

Toda la documentacion complementaria esta centralizada en [docs/README.md](docs/README.md).

## Estructura actual

- `cmd/pipeline/`: ejecucion principal del pipeline concurrente.
- `cmd/benchmark/`: comparacion secuencial vs concurrente.
- `internal/expedientes/`: logica reusable del procesamiento.
- `scrapers/`: complementacion de datos y pruebas de scraping.
- `scripts/`: orquestacion, EDA, validacion y fusion.
- `tests/unit/`: pruebas unitarias de Go.
- `tests/integration/`: pruebas de integracion de Go.
- `tests/python/`: pruebas de Python.
- `evidence/`: salidas reproducibles para el informe.
- `datasets/raw/`: datos de entrada.
- `datasets/processed/`: datos complementados o derivados.

## Evidencia actual

- Dataset combinado actual: 199,387 registros.
- Brecha respecto a 1,000,000: 800,613 registros.
- El scraper `scrapers/augment_dataset.py` genera solo registros nuevos y evita duplicar claves existentes.
