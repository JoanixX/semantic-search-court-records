# Benchmark Secuencial Vs Concurrente

## Propósito

El benchmark reside en `internal/pruebas/` y tiene como objetivo comparar el desempeño del algoritmo de procesamiento bajo una carga controlada. Se utiliza para validar empíricamente la Ley de Amdahl y el Speedup teórico del sistema.

## Cómo ejecutar

```bash
go run ./internal/pruebas -records 20000 -runs 5
```

## Parámetros de Configuración

- `-records`: Cantidad de registros sintéticos a generar para la prueba.
- `-runs`: Número de iteraciones para obtener una media estadística fiable.
- `-workers`: Número de hilos concurrentes (por defecto detecta CPUs lógicas).

## Métricas Reportadas

1. **Media Recortada (Secuencial/Concurrente)**: Elimina valores atípicos para mayor precisión.
2. **Speedup**: Factor de aceleración (Tiempo Secuencial / Tiempo Concurrente).
3. **Eficiencia**: Speedup normalizado por el número de workers.
4. **Ley de Amdahl**: Proyección del límite teórico de mejora.

## Referencia Histórica (Hito PC2)

Resultados obtenidos en un entorno controlado de 8 cores:

| Métrica | Resultado |
|---|---:|
| Registros | 20,000 |
| Workers | 8 |
| Tiempo Secuencial | 50.4404 s |
| Tiempo Concurrente | 6.2687 s |
| **Speedup** | **8.05x** |
| **Reducción de Tiempo** | **87.57%** |
