# Semantic Search Court Records

Proyecto para el curso de Programacion Concurrente y Distribuida.

El sistema implementa un pipeline concurrente en Go para la limpieza y anonimización de expedientes judiciales, orquestado con herramientas de análisis de datos en Python para generar evidencias de rendimiento y calidad.

## Flujo de Ejecución (10 Pasos)

El proyecto utiliza un orquestador centralizado que ejecuta el pipeline completo:

```bash
python scripts/run_workflow.py --workers 12 --chunks 1000
```

Este flujo automatizado realiza las siguientes fases:
1. **Detección de Datos**: Verifica la existencia del dataset original (`datasets/raw/dataset.csv`).
2. **EDA Inicial**: Genera el análisis exploratorio del dataset crudo.
3. **Cosecha de Datos**: Ejecuta el scraper para aumentar el volumen de registros.
4. **Monitoreo**: (Manual) Recordatorio para activar el monitor de recursos.
5. **Pruebas de Algoritmos**: Ejecuta `go test` sobre los algoritmos secuencial y concurrente.
6. **Análisis de Rendimiento**: Calcula Speedup, Eficiencia y Ley de Amdahl.
7. **Métricas de Hardware**: Genera gráficas de uso de CPU y RAM.
8. **Pipeline Principal**: Ejecuta el procesamiento concurrente real sobre el dataset masivo.
9. **EDA Procesado**: Genera el análisis exploratorio sobre los resultados limpios.
10. **Feature Engineering**: Genera las 8 características analíticas enriquecidas.

## Estructura del Proyecto

- `internal/pipeline/`: Código fuente del pipeline concurrente principal.
- `internal/pruebas/`: Algoritmos de benchmarking (secuencial vs concurrente).
- `internal/expedientes/`: Lógica compartida de limpieza, validación y tipos.
- `scrapers/`: Módulos para la obtención de datos oficiales (verbatim).
- `scripts/`: Scripts de Python para EDA, métricas y orquestación.
- `evidence/`: Directorio central de reportes, logs y gráficas.
- `datasets/`: Datos crudos y procesados.

## Entry Points Principales

- **Orquestador**: `scripts/run_workflow.py`
- **Pipeline Go**: `internal/pipeline/main.go`
- **Benchmark Go**: `internal/pruebas/main.go`

## Ejecución Manual de Componentes

### Tests
```bash
go test ./internal/pruebas/... -v
python -m unittest discover -s tests/python -p "test_*.py"
```

### Pipeline Individual
```bash
go run ./internal/pipeline -input datasets/raw/dataset.csv -workers 8 -chunk 100 -cost 1ms
```

### Benchmarking Individual
```bash
go run ./internal/pruebas -records 20000 -runs 5
```

## Logs y Trazabilidad

- `evidence/workflow_full.log`: Bitácora completa del orquestador.
- `evidence/pipeline_logs/`: Logs detallados de la ejecución Go.
- `evidence/eda/`: Gráficas de análisis exploratorio (original vs procesado).
- `evidence/features/`: Reportes y gráficas de ingeniería de características.

## Resultados de Rendimiento (Hito PC2)

| Métrica | Valor |
|---------|-------|
| Registros | 20,000 |
| Workers | 8 |
| Tiempo Secuencial | 50.44s |
| Tiempo Concurrente | 6.27s |
| **Speedup** | **8.05x** |
| **Eficiencia** | **87.57%** |

