# Flujo De Ejecucion

Este es el orden correcto del proyecto:

1. Ejecutar tests obligatorios.
2. Analizar el dataset original y generar solo analisis basico.
3. Ejecutar el scraper para complementar datos hasta superar 1M.
4. Fusionar y validar el dataset combinado.
5. Si el umbral se cumple, ejecutar feature engineering y su EDA.
6. Finalmente correr el pipeline Go y el benchmark Go con logs y tablas.

## Orquestador Principal

El archivo recomendado para automatizar todo el flujo es:

```bash
python scripts/run_workflow.py
```

Paramentros utiles:

- `--target-total 1000000`
- `--workers 8`
- `--delay-ms 1`
- `--benchmark-runs 3`
- `--benchmark-records 10000`

## Trazabilidad

Cada etapa escribe:

- un log agrupado por fase en `evidence/`
- una salida de resumen en texto
- graficos PNG en `evidence/graphics/`
