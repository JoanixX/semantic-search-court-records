# Benchmark Secuencial Vs Concurrente

## Qué hace

El benchmark de `cmd/benchmark/main.go` genera datos sintéticos y compara varias corridas del modelo secuencial y concurrente.

## Cómo ejecutar

```bash
go run ./cmd/benchmark -records 10000 -runs 3 -delay-ms 2
```

## Parámetros

- `-records`: cantidad de registros sintéticos.
- `-runs`: número de repeticiones.
- `-delay-ms`: costo simulado por registro.

## Qué reporta

- media recortada para secuencial.
- media recortada para concurrente.
- speedup final.

