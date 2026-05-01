# Pipeline Concurrente

## Qué hace

El pipeline de `cmd/pipeline/main.go` carga el CSV judicial, limpia cada registro, anonimiza DNIs y reporta el tiempo total.

## Cómo ejecutar

```bash
go run ./cmd/pipeline -csv datasets/raw/expedientes_tc_masivo.csv -workers 8 -delay-ms 1 -log-every 5000
```

## Parámetros

- `-csv`: ruta del archivo de entrada.
- `-workers`: cantidad de goroutines worker.
- `-limit`: cantidad opcional de filas a procesar.
- `-delay-ms`: costo simulado por registro para que el speedup sea visible.
- `-log-every`: frecuencia de trazabilidad.

## Qué evidencia genera

- Conteo de filas cargadas.
- Tiempo secuencial.
- Tiempo concurrente.
- Speedup.
- Logs periódicos de progreso.

