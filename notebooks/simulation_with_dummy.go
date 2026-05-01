package main

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ==========================================
// ESTRUCTURA DE DATOS
// ==========================================
type Expediente struct {
	ID         int
	URL        string
	Texto      string
	Expediente string
}

// ==========================================
// GENERACIÓN DE DATOS
// ==========================================
func generarDatos(cantidad int) []Expediente {
	datos := make([]Expediente, cantidad)
	for i := 0; i < cantidad; i++ {
		datos[i] = Expediente{
			ID:    i + 1,
			URL:   fmt.Sprintf("https://tc.gob.pe/expediente/%d", i),
			Texto: fmt.Sprintf("Expediente %d: El demandante con DNI 76543210 solicita revisión...", i),
		}
	}
	return datos
}

// ==========================================
// FASE 1: SIMULACIÓN DE DESCARGA
// ==========================================
func descargarContenido(url string) string {
	time.Sleep(20 * time.Millisecond)
	return fmt.Sprintf("Contenido descargado de %s con DNI 76543210", url)
}

// ==========================================
// FASE 2: PROCESAMIENTO (regex global)
// ==========================================
var reDNI = regexp.MustCompile(`[0-9]{8}`)

func limpiarYAnonimizar(texto string) string {
	time.Sleep(2 * time.Millisecond)
	return reDNI.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

// ==========================================
// SECUENCIAL
// ==========================================
func ejecucionSecuencial(datos []Expediente) time.Duration {
	inicio := time.Now()

	for _, exp := range datos {
		contenido := descargarContenido(exp.URL)
		_ = limpiarYAnonimizar(contenido)
	}

	return time.Since(inicio)
}

// ==========================================
// WORKER (misma lógica, mejora: atomic)
// ==========================================
func worker(jobs <-chan Expediente, wg *sync.WaitGroup, contadorGlobal *int64) {
	defer wg.Done()

	for exp := range jobs {
		contenido := descargarContenido(exp.URL)
		_ = limpiarYAnonimizar(contenido)

		atomic.AddInt64(contadorGlobal, 1)
	}
}

// ==========================================
// CONCURRENTE
// ==========================================
func ejecucionConcurrente(datos []Expediente, numWorkers int) time.Duration {
	inicio := time.Now()

	jobs := make(chan Expediente, 100)
	var wg sync.WaitGroup
	var contadorGlobal int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go worker(jobs, &wg, &contadorGlobal)
	}

	for _, exp := range datos {
		jobs <- exp
	}
	close(jobs)

	wg.Wait()

	return time.Since(inicio)
}

// ==========================================
// MEDIA RECORTADA
// ==========================================
func mediaRecortada(valores []float64) float64 {
	if len(valores) < 3 {
		return 0
	}

	sort.Float64s(valores)
	recortado := valores[1 : len(valores)-1]

	var suma float64
	for _, v := range recortado {
		suma += v
	}
	return suma / float64(len(recortado))
}

// ==========================================
// BENCHMARK
// ==========================================
func benchmarkWorkers(datos []Expediente, workersList []int, corridas int) {
	fmt.Println("\n=== MÉTRICAS DE SPEEDUP ===")

	var tiemposSec []float64
	for i := 0; i < corridas; i++ {
		tiemposSec = append(tiemposSec, ejecucionSecuencial(datos).Seconds())
	}
	tSec := mediaRecortada(tiemposSec)

	for _, w := range workersList {
		var tiemposConc []float64

		for i := 0; i < corridas; i++ {
			tiemposConc = append(tiemposConc, ejecucionConcurrente(datos, w).Seconds())
		}

		tConc := mediaRecortada(tiemposConc)
		speedup := tSec / tConc

		fmt.Printf("Workers: %d | Tiempo: %.4fs | Speedup: %.2fx\n", w, tConc, speedup)
	}
}

// ==========================================
// MAIN (igual que el tuyo)
// ==========================================
func main() {
	numRegistros := 10000
	corridas := 5

	datos := generarDatos(numRegistros)

	fmt.Println("=== INICIANDO PRUEBAS ===")
	fmt.Printf("Registros: %d\n", numRegistros)

	benchmarkWorkers(datos, []int{1, 8, 16, 32}, corridas)

	// Ejecutar con:
	// go run -race simulation_with_dummy.go
}