# Plan de Trabajo: Semantic Search & Court Records Processing

## 1. Introducción y Objetivos
Este proyecto tiene como objetivo principal la optimización del procesamiento de registros judiciales peruanos (Tribunal Constitucional) mediante la comparación de paradigmas de programación: **Secuencial vs Concurrente (Worker Pool)**. Se busca anonimizar datos sensibles (DNI) y limpiar el dataset para su posterior uso en búsqueda semántica.

### Objetivos Específicos:
- **Automatización**: Implementar un scraper modular verbatim para cosechar >1M de registros.
- **Eficiencia**: Reducir el tiempo de limpieza y anonimización mediante concurrencia en Go.
- **Validación**: Asegurar la integridad del dataset mediante validaciones automáticas y EDA (Exploratory Data Analysis).
- **Métricas Reales**: Cuantificar el uso de recursos (CPU, RAM, Disco) y tiempos de ejecución.

## 2. Identificación de Componentes

### A. Preparación y Análisis (Python)
- `scrapers/`: Módulos modulares para la extracción y aumento del dataset.
- `scripts/run_workflow.py`: Orquestador maestro de 10 pasos.
- `scripts/analisis_eda.py` y `scripts/eda_features.py`: Generación de evidencias visuales y enriquecimiento de datos.

### B. Motor de Procesamiento (Go)
- `internal/pruebas/`: Algoritmos comparativos de benchmarking (Secuencial vs Concurrente).
- `internal/pipeline/`: Pipeline principal de producción optimizado para alta concurrencia.
- `internal/expedientes/`: Lógica nuclear reutilizable de limpieza y anonimización.

### C. Visualización de Resultados
- `scripts/analisis_rendimiento.py`: Genera métricas de Speedup y Eficiencia.
- `scripts/metrics.py`: Genera gráficas comparativas de uso de hardware.

## 3. Plan de Trabajo Detallado
1. **Fase 1: Ingesta Masiva**: Ejecución del scraper para superar el umbral de 1M de registros oficiales.
2. **Fase 2: Análisis Exploratorio Inicial**: Validación de la calidad del dataset crudo.
3. **Fase 3: Benchmarking y Validación Teórica**:
    - Ejecución de pruebas controladas en `internal/pruebas/`.
    - Cálculo de Speedup y verificación de la Ley de Amdahl.
    - Captura de uso de recursos del sistema en tiempo real.
4. **Fase 4: Procesamiento de Producción**:
    - Ejecución del pipeline principal sobre el dataset masivo unificado.
    - Aplicación de anonimización a escala industrial.
5. **Fase 5: Consolidación y Features**:
    - Generación de 8 nuevas características (PII Risk, Case Complexity, etc.).
    - Auditoría final de calidad mediante EDA del dataset procesado.

## 4. Resultados Esperados
Demostrar una mejora de rendimiento de al menos un **80%** (Speedup > 5x) en sistemas multicore, garantizando que el dataset final sea seguro (anonimizado), íntegro (sin nulos) y enriquecido para tareas de búsqueda semántica avanzada.
