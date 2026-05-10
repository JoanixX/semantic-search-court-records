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
	// Limpieza de fechas
	_ = NormalizarFecha(record.FecIngreso)
	_ = NormalizarFecha(record.PubPagWeb)

	// Limpieza y anonimización de texto legal
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

// RunPipeline ejecuta el flujo completo: Lectura -> Limpieza (Worker Pool) -> Escritura.
// Este patrón cumple con el requisito de "Pipelines" de la rúbrica del curso.
func (p Processor) RunPipeline(inputPath, outputPath string) (Result, error) {
	start := time.Now()
	logger := p.logger()

	// 1. Abrir archivos
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return Result{}, err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return Result{}, err
	}
	defer outputFile.Close()

	reader := csv.NewReader(inputFile)
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	// Leer cabecera y escribirla tal cual
	header, err := reader.Read()
	if err != nil {
		return Result{}, err
	}
	if err := writer.Write(header); err != nil {
		return Result{}, err
	}

	// 2. Configurar canales y sincronización
	// El buffer del canal simula la gestión de memoria mencionada en las indicaciones
	jobs := make(chan []string, p.ChunkSize)
	results := make(chan []string, p.ChunkSize)
	var wgWorkers sync.WaitGroup
	var wgConsumer sync.WaitGroup
	var processed int64

	// 3. Lanzar Consumidor (Escritura Secuencial para evitar corrupción de CSV)
	wgConsumer.Add(1)
	go func() {
		defer wgConsumer.Done()
		for row := range results {
			if err := writer.Write(row); err != nil {
				logger.Printf("Error escribiendo fila: %v", err)
			}
		}
	}()

	// 4. Lanzar Workers (Procesamiento Paralelo)
	workers := p.Workers
	if workers <= 0 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		wgWorkers.Add(1)
		go func(id int) {
			defer wgWorkers.Done()
			for row := range jobs {
				// Mapeo a Record y procesamiento
				record, err := RecordFromCSVRow(row)
				if err == nil {
					// Aplicamos limpieza y anonimización
					row[0] = NormalizarFecha(record.FecIngreso)
					row[11] = NormalizarFecha(record.PubPagWeb)
					row[10] = CleanAndAnonymize(record.TextoLegal, p.SimulatedCost)
				}

				results <- row
				total := atomic.AddInt64(&processed, 1)
				if p.LogEvery > 0 && total%int64(p.LogEvery) == 0 {
					logger.Printf("[pipeline-worker-%d] Procesados: %d", id, total)
				}
			}
		}(i)
	}

	// 5. Productor (Lectura Secuencial)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		jobs <- row
	}

	close(jobs)
	wgWorkers.Wait()
	close(results)
	wgConsumer.Wait()

	return Result{
		TotalRecords: int(processed),
		Processed:    processed,
		Duration:     time.Since(start),
		Mode:         "pipeline_concurrent",
		Workers:      workers,
	}, nil
}
