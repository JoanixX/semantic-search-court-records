package expedientes

import (
	"fmt"
	"time"
)

// Record representa la unidad de trabajo del pipeline.
// Cada registro conserva solo los campos necesarios para el informe y la limpieza.
type Record struct {
	FecIngreso       string
	Procedencia      string
	TipoProceso      string
	SalaOrigen       string
	TipoDemandante   string
	TipoDemandado    string
	Sala             string
	FecVista         string
	Materia          string
	SubMateria       string
	Especifica       string
	PubPagWeb        string
	PubPeruano       string
	TipoResolucion   string
	Fallo            string
	FecDevpj         string
	FecDevpj1        string
	Departamento     string
	Provincia        string
	Distrito         string
	ResumenSentencia string
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
func RecordFromCSVRow(row []string) (Record, error) {
	if len(row) < 21 {
		return Record{}, fmt.Errorf("fila incompleta: se esperaban 21 columnas, se recibieron %d", len(row))
	}

	return Record{
		FecIngreso:       row[0],
		Procedencia:      row[1],
		TipoProceso:      row[2],
		SalaOrigen:       row[3],
		TipoDemandante:   row[4],
		TipoDemandado:    row[5],
		Sala:             row[6],
		FecVista:         row[7],
		Materia:          row[8],
		SubMateria:       row[9],
		Especifica:       row[10],
		PubPagWeb:        row[11],
		PubPeruano:       row[12],
		TipoResolucion:   row[13],
		Fallo:            row[14],
		FecDevpj:         row[15],
		FecDevpj1:        row[16],
		Departamento:     row[17],
		Provincia:        row[18],
		Distrito:         row[19],
		ResumenSentencia: row[20],
	}, nil
}
