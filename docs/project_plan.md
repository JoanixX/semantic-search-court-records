# Plan de Trabajo: Semantic Search & Court Records Processing

## 1. Introducción y Objetivos
Este proyecto tiene como objetivo principal la optimización del procesamiento de registros judiciales peruanos (Tribunal Constitucional) mediante la comparación de paradigmas de programación: **Secuencial vs Concurrente (Worker Pool)**. Se busca anonimizar datos sensibles (DNI) y limpiar el dataset para su posterior uso en búsqueda semántica.

### Objetivos Específicos:
- **Automatización**: Implementar un scraper modular verbatim para cosechar >1M de registros.
- **Eficiencia**: Reducir el tiempo de limpieza y anonimización mediante concurrencia en Go.
- **Validación**: Asegurar la integridad del dataset mediante validaciones automáticas y EDA (Exploratory Data Analysis).
- **Métricas Reales**: Cuantificar el uso de recursos (CPU, RAM, Disco) y tiempos de ejecución.

## 2. Identificación del Código
El sistema se divide en tres capas principales:

### A. Cosecha y Preparación (Python)
- `scrapers/`: Módulos divididos para base_utils, extracción, crawling y orquestación. No resumen nada, copian la lógica original verbatim.
- `scripts/analisis_eda.py`: Genera gráficas de evidencia para el dataset inicial, limpieza, creación de features y métricas finales.
- `scripts/run_workflow.py`: Orquestador que asegura el orden lógico del pipeline.

### B. Algoritmos de Procesamiento (Go)
- `internal/algoritmos/secuencial.go`: Procesa el CSV registro por registro. Debido a su alta latencia, se limita a **25 corridas** para evitar tiempos de espera excesivos en entornos de evaluación.
- `internal/algoritmos/concurrente.go`: Utiliza un patrón de **Worker Pool** para paralelizar la anonimización, distribuyendo la carga en múltiples cores de CPU. Se ejecutan **100 corridas** para obtener una distribución estadística robusta.

### C. Visualización y Evidencia
- Se generan tablas y gráficas comparativas en `evidence/`.
- Se omiten etiquetas subjetivas ("Baja", "Media"); toda la comparación es numérica y porcentual.

## 3. Plan de Trabajo Detallado
1. **Fase 1: Ingesta**: Ejecución del scraper para alcanzar el umbral de registros requeridos.
2. **Fase 2: EDA Inicial**: Análisis de la calidad de los datos cosechados.
3. **Fase 3: Procesamiento Go**:
    - Ejecución secuencial (25 iteraciones).
    - Ejecución concurrente (100 iteraciones).
    - Captura de recursos del sistema (CPU/RAM) durante ambas fases.
4. **Fase 4: Consolidación**: Unión de reportes y generación de gráficas comparativas finales.
5. **Fase 5: Validación**: Verificación de que el dataset anonimizado mantiene la estructura original.

## 4. Resultados Esperados
Se espera demostrar que el algoritmo concurrente (Worker Pool) reduce el tiempo de procesamiento en al menos un **60-80%** en procesadores multi-core, manteniendo un uso de recursos estable pero más intensivo en comparación con el modelo secuencial. El resultado final será un dataset de registros judiciales limpio, anonimizado y listo para indexación semántica.
