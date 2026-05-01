package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"semantic-search-court-records/internal/expedientes"
)

func main() {
	csvPath := flag.String("csv", "datasets/raw/expedientes_tc_masivo.csv", "ruta del CSV base")
	workers := flag.Int("workers", 8, "cantidad de workers concurrentes")
	limit := flag.Int("limit", 0, "límite opcional de filas a procesar")
	delayMs := flag.Int("delay-ms", 1, "costo simulado por registro en milisegundos")
	logEvery := flag.Int("log-every", 5000, "frecuencia de logging")
	flag.Parse()

	logger := log.New(os.Stdout, "[pipeline] ", log.LstdFlags)
	logger.Println("iniciando pipeline concurrente de limpieza y anonimización")

	records, err := expedientes.LoadCSVRecords(*csvPath, *limit)
	if err != nil {
		logger.Fatalf("no se pudo cargar el CSV: %v", err)
	}

	processor := expedientes.Processor{
		Workers:       *workers,
		ChunkSize:     *workers * 2,
		LogEvery:      *logEvery,
		SimulatedCost: time.Duration(*delayMs) * time.Millisecond,
		Logger:        logger,
	}

	seq := processor.Sequential(records)
	conc := processor.Concurrent(records)
	speedup := expedientes.Speedup(seq.Duration, conc.Duration)

	fmt.Println()
	fmt.Println("==============================================")
	fmt.Println("RESULTADOS FINALES")
	fmt.Println("==============================================")
	fmt.Printf("%-22s %d\n", "Registros cargados:", len(records))
	fmt.Printf("%-22s %s\n", "Secuencial:", seq.Duration)
	fmt.Printf("%-22s %s\n", "Concurrente:", conc.Duration)
	fmt.Printf("%-22s %.2fx\n", "Speedup:", speedup)
	fmt.Printf("%-22s %d\n", "Workers:", processor.Workers)
	fmt.Println("==============================================")
}
