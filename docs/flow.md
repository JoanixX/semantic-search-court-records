# Flujo de Ejecución Detallado

El proyecto se rige por un flujo de trabajo lineal de 10 pasos, diseñado para garantizar la integridad de los datos y la validez de las métricas de rendimiento.

## Orquestador Principal

El archivo central para automatizar el ciclo completo es:

```bash
python scripts/run_workflow.py
```

## Fases del Flujo

1. **Paso 1: Detección de Dataset**: Localización de `datasets/raw/dataset.csv`.
2. **Paso 2: EDA Inicial**: Análisis de calidad y distribución original.
3. **Paso 3: Aumento de Datos (Scraper)**: Ejecución de `scrapers/augment_dataset.py` para alcanzar el volumen objetivo (>1M).
4. **Paso 4: Monitoreo de Recursos**: Activación del agente de captura de CPU/RAM.
5. **Paso 5: Benchmarking Go**: Ejecución de algoritmos secuenciales y concurrentes en `internal/pruebas/`.
6. **Paso 6: Análisis de Rendimiento**: Cálculo estadístico de Speedup y Eficiencia.
7. **Paso 7: Generación de Métricas**: Creación de visualizaciones comparativas de hardware.
8. **Paso 8: Pipeline Principal Go**: Ejecución de la limpieza y anonimización masiva en `internal/pipeline/`.
9. **Paso 9: EDA Procesado**: Verificación visual de la calidad tras el procesamiento.
10. **Paso 10: Ingeniería de Características**: Generación de las 8 dimensiones analíticas finales.

## Parámetros Útiles del Orquestador

- `--workers <int>`: Configura el pool de goroutines para los pasos de Go.
- `--chunks <int>`: Ajusta el tamaño del buffer de los canales para optimizar el uso de memoria.

## Salidas por Fase

Cada etapa del flujo genera:
- Una sección dedicada en `evidence/workflow_full.log`.
- Un resumen ejecutivo en `evidence/workflow_10_steps_summary.txt`.
- Artefactos específicos (Gráficas, Tablas o Logs) en sus carpetas correspondientes dentro de `evidence/`.
