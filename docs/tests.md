# Pruebas Unitarias E Integracion

## Go

Pruebas disponibles:

- `tests/unit/anonymizer_test.go`
- `tests/unit/loader_test.go`
- `tests/integration/processor_test.go`

Estas pruebas forman parte del gate inicial del flujo. Si fallan, el orquestador no avanza.

Ejecucion:

```bash
go test ./tests/unit ./tests/integration
```

## Python

Pruebas disponibles:

- `tests/python/test_official_scraper.py`

Ejecucion:

```bash
python -m unittest discover -s tests/python -p "test_*.py"
```
