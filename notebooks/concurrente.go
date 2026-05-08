package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

type ExpedienteTC struct {
	TextoLegal string
}

var reDNI = regexp.MustCompile(`\b\d{8}\b`)

func limpiarYAnonimizar(texto string) string {
	time.Sleep(1 * time.Millisecond)
	return reDNI.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

func workerLimpiador(jobs <-chan ExpedienteTC, wg *sync.WaitGroup, contadorGlobal *int64) {
	defer wg.Done()
	for exp := range jobs {
		_ = limpiarYAnonimizar(exp.TextoLegal)
		atomic.AddInt64(contadorGlobal, 1)
	}
}

// Función que ejecuta 1 corrida con 'n' workers
func ejecutarCorrida(numWorkers int, rutaArchivo string) float64 {
	var wg sync.WaitGroup
	var contadorGlobal int64

	archivo, _ := os.Open(rutaArchivo)
	defer archivo.Close()

	lectorCSV := csv.NewReader(archivo)
	_, _ = lectorCSV.Read()

	jobs := make(chan ExpedienteTC, 5000)

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
		if err == nil && len(fila) > 17 {
			jobs <- ExpedienteTC{TextoLegal: fila[17]}
		}
	}

	close(jobs)
	wg.Wait()

	return time.Since(inicio).Seconds()
}

func main() {
	rutaArchivoCSV := "../datasets/processed/processed_records.csv"
	corridas := 150

	fMetricas, err := os.Create("metricas_concurrente.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()
	fMetricas.WriteString("goroutines,tiempo_segundos\n")

	fmt.Println("=== INICIANDO 100 CORRIDAS (TEST DE ESCALABILIDAD) ===")
	
	// Corremos desde 1 hasta 100 goroutines
	for workers := 15; workers <= corridas; workers+=15 {
		tiempo := ejecutarCorrida(workers, rutaArchivoCSV)
		fmt.Printf("Corrida con %3d Workers | Tiempo: %6.4f s\n", workers, tiempo)
		fMetricas.WriteString(fmt.Sprintf("%d,%.4f\n", workers, tiempo))
	}
}