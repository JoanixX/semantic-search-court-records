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
	"flag"
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
	inputPath := flag.String("csv", "../../datasets/processed/processed_records.csv", "Ruta al archivo CSV")
	outputPath := flag.String("output", "metricas_concurrente.csv", "Ruta de salida para el CSV de métricas")
	corridas := flag.Int("runs", 100, "Corridas por cada nivel de workers")
	maxWorkers := flag.Int("max-workers", 150, "Cantidad máxima de workers para el test")
	stepWorkers := flag.Int("step", 15, "Incremento de workers por cada corrida")
	flag.Parse()

	// Validación
	if *inputPath == "" {
		log.Fatal("Error: La flag -input es obligatoria. Proporciona la ruta del dataset.")
	}

	fMetricas, err := os.Create(*outputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()

	// Identificamos los workers y la corrida específica en el mismo CSV
	fMetricas.WriteString("workers,corrida,tiempo_segundos\n")
	fmt.Printf("=== BENCHMARK CONCURRENTE (%d corridas por escalón) ===\n", *corridas)

	for workers := *stepWorkers; workers <= *maxWorkers; workers += *stepWorkers {
		fmt.Printf("\n--- Evaluando con %d Workers ---\n", workers)
		
		for i := 1; i <= *corridas; i++ {
			tiempo := ejecutarCorrida(workers, *inputPath)
			fmt.Printf("Corrida %3d | Tiempo: %6.4f s\n", i, tiempo)
			fMetricas.WriteString(fmt.Sprintf("%d,%d,%.4f\n", workers, i, tiempo))
		}
	}
	fmt.Println("\nResultados concurrentes guardados con éxito.")
}