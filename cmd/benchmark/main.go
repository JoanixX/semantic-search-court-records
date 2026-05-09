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
	records := flag.Int("records", 20000, "cantidad de registros sintéticos")
	runs := flag.Int("runs", 3, "cantidad de corridas por configuración")
	delayMs := flag.Int("delay-ms", 2, "costo simulado por registro en milisegundos")
	flag.Parse()

	workerCounts := []int{1, 5, 10, 25, 40, 75, 100}
	logger := log.New(os.Stderr, "", 0)

	data := expedientes.GenerateSyntheticRecords(*records)
	
	fmt.Println("Type,Workers,Records,Run,Duration")
	
	// Sequential benchmark
	processorSeq := expedientes.Processor{
		Workers:       1,
		SimulatedCost: time.Duration(*delayMs) * time.Millisecond,
		Logger:        logger,
	}
	for i := 0; i < *runs; i++ {
		res := processorSeq.Sequential(data)
		fmt.Printf("Sequential,1,%d,%d,%.6f\n", *records, i+1, res.Duration.Seconds())
	}

	// Concurrent benchmark
	for _, w := range workerCounts {
		processor := expedientes.Processor{
			Workers:       w,
			ChunkSize:     128,
			SimulatedCost: time.Duration(*delayMs) * time.Millisecond,
			Logger:        logger,
		}
		for i := 0; i < *runs; i++ {
			res := processor.Concurrent(data)
			fmt.Printf("Concurrent,%d,%d,%d,%.6f\n", w, *records, i+1, res.Duration.Seconds())
		}
	}
}
