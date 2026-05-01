# Scraper Y Complementacion De Datos

## Objetivo

El scraper `scrapers/augment_dataset.py` complementa el dataset sin duplicar registros ya existentes.

## Que hace

- lee documentos en `datasets/raw/scraper_sources/`.
- extrae texto de `.txt`, `.md`, `.html` y `.pdf`.
- genera filas nuevas hasta alcanzar el objetivo definido.
- evita claves repetidas usando hash SHA-256.

## Como ejecutar

```bash
python scrapers/augment_dataset.py --target-total 1000000
```

## Pruebas

```bash
python -m unittest discover -s tests/python -p "test_*.py"
```

El scraper corre despues del EDA inicial y antes de la validacion del umbral.
