# Evidencias Reproducibles

Las salidas del proyecto se consolidan en el directorio `evidence/`, permitiendo auditar cada fase del proceso.

## Registro de Ejecución (Logs)

- **`evidence/workflow_full.log`**: Bitácora maestra con la salida de los 10 pasos.
- **`evidence/pipeline_logs/`**: Logs específicos del procesamiento Go (limpieza y validación).
- **`evidence/monitor_resources.log`**: Trazabilidad del consumo de hardware (CPU/RAM).

## Análisis Exploratorio (EDA)

Ubicación: `evidence/eda/`
- **`original/`**: Reportes y gráficas del dataset crudo.
- **`processed/`**: Reportes y gráficas tras la limpieza y anonimización.

## Ingeniería de Características

Ubicación: `evidence/features/`
- Gráficas y tablas de las 8 características enriquecidas (PII Risk, Complexity, Priority, etc.).
- Resumen estadístico de las nuevas dimensiones del dataset.

## Rendimiento y Métricas

- **`evidence/metrics/`**: Gráficas de Speedup, Eficiencia y comparación de recursos.
- **`evidence/workflow_10_steps_summary.txt`**: Resumen ejecutivo de la última corrida exitosa.

## Cómo regenerar evidencias

Para ejecutar el ciclo completo de generación de evidencias:

```bash
python scripts/run_workflow.py --workers 12
```

Si se desea regenerar solo la limpieza Go:

```bash
go run ./internal/pipeline -input datasets/raw/dataset.csv -workers 8
```

Para regenerar solo el análisis de rendimiento:

```bash
python scripts/analisis_rendimiento.py
```
