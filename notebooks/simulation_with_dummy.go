//go:build ignore

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
// FASE 1: SIMULACIÓN DE DESCARGA (I/O bound)
// Se usa un sleep variable para simular latencia de red
// ==========================================
func descargarContenido(url string) string {
	// Latencia variable entre 5ms y 15ms
	time.Sleep(time.Duration(5+time.Now().UnixNano()%10) * time.Millisecond)
	return fmt.Sprintf("Contenido descargado de %s con DNI 76543210", url)
}

// ==========================================
// FASE 2: PROCESAMIENTO (CPU bound)
// Regex precompilado + pequeño delay de CPU
// ==========================================
var reDNI = regexp.MustCompile(`[0-9]{8}`)

func limpiarYAnonimizar(texto string) string {
	// Simula carga de CPU ligera
	time.Sleep(1 * time.Millisecond)
	return reDNI.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

// ==========================================
// SECUENCIAL
// ==========================================
func ejecucionSecuencial(datos []Expediente) time.Duration {
	inicio := time.Now()

	for i, exp := range datos {
		contenido := descargarContenido(exp.URL)
		_ = limpiarYAnonimizar(contenido)

		// Log cada 200 registros para ver progreso
		if i%200 == 0 {
			fmt.Printf("[Secuencial] Procesados: %d\n", i)
		}
	}

	return time.Since(inicio)
}

// ==========================================
// WORKER
// Procesa tareas concurrentemente
// ==========================================
func worker(id int, jobs <-chan Expediente, wg *sync.WaitGroup, contadorGlobal *int64) {
	defer wg.Done()

	for exp := range jobs {
		contenido := descargarContenido(exp.URL)
		_ = limpiarYAnonimizar(contenido)

		total := atomic.AddInt64(contadorGlobal, 1)

		// Log cada 300 operaciones globales
		if total%300 == 0 {
			fmt.Printf("[Worker %d] Total procesados: %d\n", id, total)
		}
	}
}

// ==========================================
// CONCURRENTE (Worker Pool)
// ==========================================
func ejecucionConcurrente(datos []Expediente, numWorkers int) time.Duration {
	inicio := time.Now()

	jobs := make(chan Expediente, 100)
	var wg sync.WaitGroup
	var contadorGlobal int64

	// Lanzamiento de workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, &wg, &contadorGlobal)
	}

	// Envío de trabajos
	for _, exp := range datos {
		jobs <- exp
	}
	close(jobs)

	wg.Wait()

	return time.Since(inicio)
}

// ==========================================
// MEDIA RECORTADA (elimina outliers)
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
// BENCHMARK CON LOGS
// ==========================================
func benchmarkWorkers(datos []Expediente, workersList []int, corridas int) {
	fmt.Println("\n=== MÉTRICAS DE SPEEDUP ===")

	// ========================
	// Secuencial
	// ========================
	var tiemposSec []float64
	for i := 0; i < corridas; i++ {
		fmt.Printf("\n[Secuencial] Corrida %d...\n", i+1)
		tiempo := ejecucionSecuencial(datos).Seconds()
		fmt.Printf("[Secuencial] Tiempo: %.4fs\n", tiempo)
		tiemposSec = append(tiemposSec, tiempo)
	}

	tSec := mediaRecortada(tiemposSec)
	fmt.Printf("\nTiempo Secuencial (media recortada): %.4fs\n\n", tSec)

	// ========================
	// Concurrente
	// ========================
	fmt.Println("Workers | Tiempo (s) | Speedup")
	fmt.Println("--------------------------------")

	for _, w := range workersList {
		var tiemposConc []float64

		for i := 0; i < corridas; i++ {
			fmt.Printf("\n[Concurrente] Workers=%d Corrida %d...\n", w, i+1)
			tiempo := ejecucionConcurrente(datos, w).Seconds()
			fmt.Printf("[Concurrente] Workers=%d Tiempo: %.4fs\n", w, tiempo)
			tiemposConc = append(tiemposConc, tiempo)
		}

		tConc := mediaRecortada(tiemposConc)
		speedup := tSec / tConc

		fmt.Printf("%7d | %10.4f | %6.2fx\n", w, tConc, speedup)
	}
}

// ==========================================
// MAIN
// ==========================================
func main() {
	numRegistros := 1000 // Tamaño moderado para pruebas rápidas
	corridas := 3        // Repeticiones para estabilidad estadística

	datos := generarDatos(numRegistros)

	fmt.Println("=== INICIANDO PRUEBAS ===")
	fmt.Printf("Registros: %d\n", numRegistros)

	// Comparación con distintos niveles de concurrencia
	benchmarkWorkers(datos, []int{1, 4, 8, 16}, corridas)

	// Ejecutar con:
	// go run -race simulation_with_dummy.go
}
