# Arquitectura Y Justificación

El proyecto usa dos entrypoints Go reales porque resuelven necesidades distintas:

- `cmd/pipeline/main.go`: procesa el dataset real con limpieza y anonimización.
- `cmd/benchmark/main.go`: ejecuta el escenario controlado para medir secuencial vs concurrente.

Separar ambos flujos evita mezclar carga real con carga de simulación y facilita justificar el speedup en el informe.

## Sobre Los `main.go`

Puede parecer que hay varios archivos `main`, pero en la práctica hay dos que se ejecutan:

- Los de `cmd/` son los binarios válidos del proyecto.
- Los de `notebooks/` quedaron como artefactos históricos y están excluidos del build con `//go:build ignore`.

Esto se hace para conservar el material de exploración del informe sin romper `go test ./...`.

## Concurrencia

El pipeline usa:

- `channels` para distribuir trabajo.
- `sync.WaitGroup` para coordinar finalización.
- `sync/atomic` para el contador global.
- una sección crítica mínima para evitar contención innecesaria.

