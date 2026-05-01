package expedientes

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Processor encapsula la configuración de ejecución del pipeline.
// Centralizar esta configuración facilita pruebas, benchmarks y trazabilidad.
type Processor struct {
	Workers       int
	ChunkSize     int
	LogEvery      int
	SimulatedCost time.Duration
	Logger        *log.Logger
}

func (p Processor) logger() *log.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return log.New(os.Stdout, "", log.LstdFlags)
}

// LoadCSVRecords carga el CSV y transforma cada fila en un Record.
// Se omiten filas inválidas en lugar de abortar para mantener el flujo del pipeline.
func LoadCSVRecords(path string, limit int) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		return nil, err
	}

	records := make([]Record, 0, 1024)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		record, err := RecordFromCSVRow(row)
		if err != nil {
			continue
		}
		records = append(records, record)
		if limit > 0 && len(records) >= limit {
			break
		}
	}

	return records, nil
}

func (p Processor) processRecord(record Record) string {
	return CleanAndAnonymize(record.TextoLegal, p.SimulatedCost)
}

// Sequential ejecuta el preprocesamiento con una sola goroutine.
// Sirve como línea base para el cálculo de speedup y para demostrar el costo real.
func (p Processor) Sequential(records []Record) Result {
	start := time.Now()
	logger := p.logger()
	processed := int64(0)
	logs := make([]string, 0, 8)

	for i, record := range records {
		_ = p.processRecord(record)
		processed++
		if p.LogEvery > 0 && (i+1)%p.LogEvery == 0 {
			msg := fmt.Sprintf("[secuencial] procesados=%d", processed)
			logger.Println(msg)
			logs = append(logs, msg)
		}
	}

	return Result{
		TotalRecords: len(records),
		Processed:    processed,
		Duration:     time.Since(start),
		Mode:         "sequential",
		Workers:      1,
		Logs:         logs,
	}
}

// Concurrent ejecuta un Worker Pool con exclusión mutua solo para la contabilidad global.
// La sección crítica es mínima para reducir contención y evitar starvation innecesaria.
func (p Processor) Concurrent(records []Record) Result {
	start := time.Now()
	logger := p.logger()
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	buffer := p.ChunkSize
	if buffer <= 0 {
		buffer = workers * 2
	}

	jobs := make(chan Record, buffer)
	var wg sync.WaitGroup
	var processed int64
	logs := make([]string, 0, 8)
	var logsMu sync.Mutex

	for workerID := 1; workerID <= workers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for record := range jobs {
				_ = p.processRecord(record)
				total := atomic.AddInt64(&processed, 1)
				if p.LogEvery > 0 && total%int64(p.LogEvery) == 0 {
					msg := fmt.Sprintf("[worker=%d] procesados_globales=%d", id, total)
					logger.Println(msg)
					logsMu.Lock()
					logs = append(logs, msg)
					logsMu.Unlock()
				}
			}
		}(workerID)
	}

	for _, record := range records {
		jobs <- record
	}
	close(jobs)
	wg.Wait()

	return Result{
		TotalRecords: len(records),
		Processed:    processed,
		Duration:     time.Since(start),
		Mode:         "concurrent",
		Workers:      workers,
		Logs:         logs,
	}
}

// TrimmedMean calcula la media recortada eliminando el mínimo y el máximo.
// Se usa para reducir el efecto de outliers cuando hay variaciones de entorno.
func TrimmedMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) < 3 {
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}

	clone := append([]float64(nil), values...)
	sort.Float64s(clone)
	trimmed := clone[1 : len(clone)-1]
	var sum float64
	for _, v := range trimmed {
		sum += v
	}
	return sum / float64(len(trimmed))
}

// Speedup calcula la aceleración entre el modelo secuencial y el concurrente.
func Speedup(seq, concurrent time.Duration) float64 {
	if concurrent <= 0 {
		return 0
	}
	return seq.Seconds() / concurrent.Seconds()
}
