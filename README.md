# Limpieza Paralela de Expedientes Judiciales para Búsqueda Semántica

Este proyecto implementa un pipeline concurrente en Go para limpiar, normalizar y anonimizar más de un millón de expedientes judiciales del Poder Judicial del Perú. El sistema procesa los documentos en paralelo usando worker pools, mutexes y canales para garantizar eficiencia, seguridad y escalabilidad.

Incluye:

- Limpieza masiva concurrente
- Anonimización de datos sensibles
- Preparación para búsqueda semántica (embeddings)
- Comparación secuencial vs concurrente
- Modelado en Promela y verificación con Spin

Este repositorio corresponde al entregable del curso CC65 – Programación Concurrente y Distribuida (2026-1).
