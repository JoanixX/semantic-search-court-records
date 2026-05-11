# Arquitectura Y Justificación

El proyecto está diseñado bajo un enfoque modular, separando la lógica de negocio (procesamiento de expedientes) de la infraestructura de ejecución y las pruebas de rendimiento.

## Estructura de Módulos

- **`internal/pipeline/`**: Entrypoint principal del sistema. Orquesta la lectura de datasets masivos y aplica el pipeline de limpieza mediante concurrencia.
- **`internal/pruebas/`**: Módulo dedicado a la validación de algoritmos y benchmarking controlado (Secuencial vs Concurrente).
- **`internal/expedientes/`**: Contiene la lógica nuclear (Limpiador, Validador, Tipos) compartida por los entrypoints.

## Orquestación del Flujo

El sistema utiliza un orquestador en Python (`scripts/run_workflow.py`) que garantiza un flujo de 10 pasos lógicos, desde la ingesta de datos hasta la generación de características enriquecidas (Feature Engineering). Esto asegura que cada fase tenga las dependencias de datos necesarias antes de ejecutarse.

## Modelo de Concurrencia

El pipeline utiliza el patrón **Producer-Worker-Consumer**:

- **Productor**: Lee el archivo CSV y distribuye los registros en un canal con buffer (`chunkSize`).
- **Workers**: Pool de goroutines que procesan y anonimizan registros en paralelo.
- **Consumidor**: Recibe los resultados y escribe el archivo final de forma sincronizada.

Tecnologías de coordinación:
- `channels` para la distribución de carga y gestión de presión (backpressure).
- `sync.WaitGroup` para la sincronización de fin de etapa.
- `sync/atomic` para contadores de progreso globales sin bloqueos.
- `context` (opcional) para la propagación de cancelaciones en ejecuciones largas.

