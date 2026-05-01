package expedientes

import (
	"fmt"
	"time"
)

// Record representa la unidad de trabajo del pipeline.
// Cada registro conserva solo los campos necesarios para el informe y la limpieza.
type Record struct {
	FecIngreso  string
	Procedencia string
	TipoProceso string
	TextoLegal  string
	Source      string
}

// Result resume el comportamiento del pipeline luego de procesar un lote.
type Result struct {
	TotalRecords int
	Processed    int64
	Duration     time.Duration
	Mode         string
	Workers      int
	Logs         []string
}

// BenchmarkRow guarda el resultado de una corrida para el cálculo de speedup.
type BenchmarkRow struct {
	Workers    int
	Seq        time.Duration
	Concurrent time.Duration
	Speedup    float64
}

// RecordFromCSVRow convierte una fila CSV en un Record validando el tamaño mínimo.
// La validación evita que una fila incompleta rompa el pipeline concurrente.
func RecordFromCSVRow(row []string) (Record, error) {
	if len(row) < 21 {
		return Record{}, fmt.Errorf("fila incompleta: se esperaban al menos 21 columnas, se recibieron %d", len(row))
	}

	return Record{
		FecIngreso:  row[0],
		Procedencia: row[1],
		TipoProceso: row[2],
		TextoLegal:  row[20],
	}, nil
}
