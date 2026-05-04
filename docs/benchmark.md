# Benchmark Secuencial Vs Concurrente

## Qué hace

El benchmark de `cmd/benchmark/main.go` genera datos sintéticos y compara varias corridas del modelo secuencial y concurrente.

## Cómo ejecutar

```bash
go run ./cmd/benchmark -records 20000 -runs 3 -delay-ms 2
```

## Parámetros

- `-records`: cantidad de registros sintéticos.
- `-runs`: número de repeticiones.
- `-delay-ms`: costo simulado por registro.

## Qué reporta

- media recortada para secuencial.
- media recortada para concurrente.
- speedup final.

## Resultado PC2 reportado

Segun el informe `CC65-PC2-202601-U202312801.pdf`, el resultado principal de PC2 fue:

| Metrica | Resultado |
|---|---:|
| Registros | 20,000 |
| Workers | 8 |
| Tiempo secuencial | 50.4404 s |
| Tiempo concurrente | 6.2687 s |
| Speedup | 8.05x |
| Reduccion de tiempo | 87.57% |
