package main

import (
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Expediente simula el registro judicial extraído del TC
type Expediente struct { 
	ID    int
	Texto string
}

// SIMULACIÓN DE CARGA: Generamos 10000 expedientes dummy
func generarDatos(cantidad int) []Expediente {
	datos := make([]Expediente, cantidad)
	for i := 0; i < cantidad; i++ {
		datos[i] = Expediente{
			ID:    i + 1,
			Texto: fmt.Sprintf("Expediente %d: El demandante con DNI 76543210 solicita revisión...", i),
		}
	}
	return datos
}

// TAREA INTENSIVA (NLP / Regex): Anonimización de datos sensibles
func limpiarYAnonimizar(texto string) string {
	// Simulamos el tiempo que toma el procesamiento NLP por CPU (ej. 2 milisegundos por registro)
	time.Sleep(2 * time.Millisecond)
	re := regexp.MustCompile(`[0-9]{8}`)
	return re.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

// ==========================================
// 1. IMPLEMENTACIÓN SECUENCIAL
// ==========================================
func ejecucionSecuencial(datos []Expediente) time.Duration {
	inicio := time.Now()
	anonimizadosTotal := 0

	for _, exp := range datos {
		_ = limpiarYAnonimizar(exp.Texto)
		anonimizadosTotal++
	}

	return time.Since(inicio)
}

// ==========================================
// 2. IMPLEMENTACIÓN CONCURRENTE (Worker Pool)
// ==========================================
func worker(id int, jobs <-chan Expediente, wg *sync.WaitGroup, mu *sync.Mutex, contadorGlobal *int) {
	defer wg.Done()
	for exp := range jobs {
		// Procesamiento fuera de la sección crítica (para no bloquear otros hilos)
		_ = limpiarYAnonimizar(exp.Texto)

		// SECCIÓN CRÍTICA: Actualizamos el contador global de manera segura
		mu.Lock()
		*contadorGlobal++
		mu.Unlock()
	}
}

func ejecucionConcurrente(datos []Expediente, numWorkers int) time.Duration {
	inicio := time.Now()

	jobs := make(chan Expediente, len(datos))
	var wg sync.WaitGroup
	var mu sync.Mutex
	anonimizadosTotal := 0

	// 1. Despliegue del Worker Pool
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, &wg, &mu, &anonimizadosTotal)
	}

	// 2. Enviar trabajos al canal
	for _, exp := range datos {
		jobs <- exp
	}
	close(jobs) // Cerramos el canal para indicar que no hay más datos

	// 3. Esperar sincronización
	wg.Wait()

	return time.Since(inicio)
}

// ==========================================
// MAIN: PRUEBAS MULTIPLES Y RESULTADOS
// ==========================================
func main() {
	numRegistros := 10000
	numWorkers := 8
	corridas := 5
	datos := generarDatos(numRegistros)

	var tiemposSec []float64
	var tiemposConc []float64

	fmt.Println("=== INICIANDO PRUEBAS DE RENDIMIENTO (NLP ANONIMIZACIÓN) ===")
	fmt.Printf("Registros: %d | Workers: %d\n\n", numRegistros, numWorkers)

	for i := 0; i < corridas; i++ {
		tSec := ejecucionSecuencial(datos).Seconds()
		tConc := ejecucionConcurrente(datos, numWorkers).Seconds()

		tiemposSec = append(tiemposSec, tSec)
		tiemposConc = append(tiemposConc, tConc)

		fmt.Printf("Corrida %d -> Secuencial: %.4fs | Concurrente: %.4fs | Speedup: %.2fx\n",
			i+1, tSec, tConc, tSec/tConc)
	}
}