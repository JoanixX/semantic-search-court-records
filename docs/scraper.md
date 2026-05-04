# Scraper Y Complementacion De Datos

## Objetivo

El scraper `scrapers/augment_dataset.py` extrae documentos reales desde fuentes oficiales del gobierno peruano y construye un complemento de dataset sin inventar campos.

## Que hace

- parte de seeds oficiales definidos en `datasets/raw/official_sources.txt`.
- crawlea dominios oficiales del TC y de Datos Abiertos.
- extrae texto desde HTML, PDF, CSV, ZIP, XLSX y JSON.
- descubre endpoints y descargas reales dentro de HTML, JSON y archivos anidados.
- llena solo los campos que el documento realmente contiene.
- avisa si el numero de registros reales obtenidos no alcanza el objetivo.
- salta contenido binario o HTML malformado y deja la traza en `evidence/prep.log` y en `evidence/official_harvest_summary.txt`.
- registra las semillas con rendimiento cero para descartarlas en el siguiente intento y priorizar las que si generan filas.
- procesa XLSX grandes de forma incremental y limita filas por hoja para evitar que un archivo pesado detenga todo el crawl.
- corta la ejecucion cuando alcanza `--target-total`, por lo que las pruebas pequenas no recorren fuentes innecesarias.

El complemento oficial se escribe en `datasets/processed/official_tc_harvest.csv` y luego se fusiona en el dataset procesado combinado.

## Como ejecutar

```bash
python scrapers/augment_dataset.py --target-total 1000000
```

Para una corrida completa con archivos tabulares grandes:

```bash
python scrapers/augment_dataset.py --target-total 1000000 --timeout 200 --max-pages 1000 --max-rows-per-sheet 250000 --no-proxy-env
```

Para pruebas con descargas directas oficiales y mayor volumen, se puede limitar la exploracion inicial a fuentes tabulares:

```bash
python scrapers/augment_dataset.py --target-total 1000000 --timeout 120 --max-pages 6 --max-rows-per-sheet 500000 --no-proxy-env
```

Si falta poco para el millon, se debe aumentar `--max-pages` para permitir fuentes judiciales adicionales o repetir cuando Datos Abiertos no responda con errores temporales `502`.

Si el entorno tiene variables de proxy rotas, se puede ignorar el proxy del sistema:

```bash
python scrapers/augment_dataset.py --target-total 1000000 --no-proxy-env
```

Si la red institucional exige un proxy autorizado:

```bash
python scrapers/augment_dataset.py --target-total 1000000 --proxy http://host:puerto
```

Para pruebas cortas:

```bash
python scrapers/augment_dataset.py --target-total 10 --timeout 8 --no-proxy-env
```

## Pruebas

```bash
python -m unittest discover -s tests/python -p "test_*.py"
```

El scraper corre despues del EDA inicial y antes de la validacion del umbral. Si no hay suficientes documentos oficiales, el reporte de `evidence/official_harvest_summary.txt` lo deja claro.
