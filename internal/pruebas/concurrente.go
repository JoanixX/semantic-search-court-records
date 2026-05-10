package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"semantic-search-court-records/internal/expedientes"
	"sync"
	"sync/atomic"
	"time"
)

type ExpedienteTC struct {
	FecIngreso string
	PubPagWeb  string
	Especifica string
}

func workerLimpiador(jobs <-chan ExpedienteTC, wg *sync.WaitGroup, contadorGlobal *int64) {
	defer wg.Done()
	for exp := range jobs {
		// Procesamiento paralelo usando el módulo centralizado
		_ = expedientes.NormalizarFecha(exp.FecIngreso)
		_ = expedientes.NormalizarFecha(exp.PubPagWeb)
		_ = expedientes.CleanAndAnonymize(exp.Especifica, 1*time.Nanosecond)

		nuevo := atomic.AddInt64(contadorGlobal, 1)
		if nuevo%100000 == 0 {
			fmt.Printf("Procesados: %d\n", nuevo)
		}
	}
}

// Función que ejecuta 1 corrida con 'n' workers y 'c' chunk size
func ejecutarCorrida(numWorkers int, chunkSize int, rutaArchivo string) float64 {
	var wg sync.WaitGroup
	var contadorGlobal int64

	archivo, err := os.Open(rutaArchivo)
	if err != nil {
		return 0
	}
	defer archivo.Close()

	lectorCSV := csv.NewReader(archivo)
	_, _ = lectorCSV.Read()

	// El chunkSize (capacidad del canal) es la variable relacionada solicitada
	jobs := make(chan ExpedienteTC, chunkSize)

	inicio := time.Now()

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go workerLimpiador(jobs, &wg, &contadorGlobal)
	}

	for {
		fila, err := lectorCSV.Read()
		if err == io.EOF {
			break
		}
		if err == nil && len(fila) > 11 {
			jobs <- ExpedienteTC{
				FecIngreso: fila[0],
				PubPagWeb:  fila[11],
				Especifica: fila[10],
			}
		}
	}

	close(jobs)
	wg.Wait()

	var memFin runtime.MemStats
	runtime.ReadMemStats(&memFin)

	fmt.Printf(
		"[MEMORIA] Heap usado: %.2f MB | GC: %d\n",
		float64(memFin.Alloc)/(1024*1024),
		memFin.NumGC,
	)

	return time.Since(inicio).Seconds()
}

func EjecutarPruebaConcurrente() {
	rutaArchivoCSV := "../../datasets/processed/processed_records.csv"

	// Al menos 6 puntos diferentes para Worker Pools y Chunks
	puntosWorkers := []int{
		1, 2, 4, 8, 12, 16, 24, 32,
	}

	puntosChunks := []int{
		50, 100, 250, 500, 750, 1000, 1250, 1500,
	}

	fMetricas, err := os.Create("../../evidence/metrics_results/metricas_concurrente.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()
	fMetricas.WriteString("goroutines,chunk_size,tiempo_segundos\n")

	fmt.Println("=== INICIANDO PRUEBAS DE ESCALABILIDAD (10 PUNTOS DE CONTROL) ===")

	// Variaremos el chunk size proporcionalmente para obtener datos de ambas variables.
	for i, workers := range puntosWorkers {
		chunk := puntosChunks[i] // Emparejamos cada worker pool con un chunk size diferente
		fmt.Printf("--- Evaluando Configuración %d: Workers=%d | Chunk=%d ---\n", i+1, workers, chunk)

		for c := 1; c <= 5; c++ {
			tiempo := ejecutarCorrida(workers, chunk, rutaArchivoCSV)
			fmt.Printf("[CONCURRENTE] Cfg %d | Corrida %d | Tiempo: %6.4f s\n", i+1, c, tiempo)
			fMetricas.WriteString(fmt.Sprintf("%d,%d,%.4f\n", workers, chunk, tiempo))
			runtime.GC()
		}
	}
}
