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
	records := flag.Int("records", 10000, "cantidad de registros sintéticos")
	runs := flag.Int("runs", 5, "cantidad de corridas")
	delayMs := flag.Int("delay-ms", 2, "costo simulado por registro en milisegundos")
	flag.Parse()

	logger := log.New(os.Stdout, "[benchmark] ", log.LstdFlags)
	data := expedientes.GenerateSyntheticRecords(*records)
	processor := expedientes.Processor{
		Workers:       8,
		ChunkSize:     128,
		LogEvery:      2000,
		SimulatedCost: time.Duration(*delayMs) * time.Millisecond,
		Logger:        logger,
	}

	var seqTimes []float64
	var concTimes []float64

	for i := 0; i < *runs; i++ {
		seq := processor.Sequential(data)
		seqTimes = append(seqTimes, seq.Duration.Seconds())
	}
	for i := 0; i < *runs; i++ {
		conc := processor.Concurrent(data)
		concTimes = append(concTimes, conc.Duration.Seconds())
	}

	seqMean := expedientes.TrimmedMean(seqTimes)
	concMean := expedientes.TrimmedMean(concTimes)
	speedup := seqMean / concMean

	fmt.Println("Workers | Secuencial(s) | Concurrente(s) | Speedup")
	fmt.Println("---------------------------------------------------")
	fmt.Printf("%7d | %13.4f | %13.4f | %6.2fx\n", processor.Workers, seqMean, concMean, speedup)
}
