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

// ExpedienteTC representa la estructura real de tu dataset
// Solo mapeamos los campos que nos interesan para la limpieza (ID/Fecha y el Texto)
type ExpedienteTC struct {
	FecIngreso  string
	Procedencia string
	TextoLegal  string // Esta es la columna 'TEXT' que agregaste
}

// FUNCION INTENSIVA (NLP / Regex): Anonimización de datos sensibles
var reDNI = regexp.MustCompile(`\b\d{8}\b`)

func limpiarYAnonimizar(texto string) string {
	// Simulamos carga de CPU (quitar esto en producción)
	time.Sleep(1 * time.Millisecond)

	// Expresión regular para buscar DNIs (8 dígitos consecutivos)
	textoLimpio := reDNI.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")

	return textoLimpio
}

// ==========================================
// WORKER POOL CONCURRENTE
// ==========================================
func workerLimpiador(id int, jobs <-chan ExpedienteTC, wg *sync.WaitGroup, contadorGlobal *int64) {
	defer wg.Done()

	for exp := range jobs {
		inicioWorker := time.Now()

		// 1. Procesamiento pesado fuera de la sección crítica
		textoAnonimizado := limpiarYAnonimizar(exp.TextoLegal)
		_ = textoAnonimizado

		// 2. SECCIÓN CRÍTICA: Actualizamos el contador de forma segura
		total := atomic.AddInt64(contadorGlobal, 1)

		// Logs cada cierto número de registros procesados
		if total%5000 == 0 {
			fmt.Printf("[Worker %d] Procesados globales: %d | Tiempo último: %v\n",
				id, total, time.Since(inicioWorker))
		}
	}
}

func main() {
	rutaArchivoCSV := "../datasets/raw/expedientes_tc_masivo.csv"
	numWorkers := 8

	var wg sync.WaitGroup
	var contadorGlobal int64 // cambiado a int64 para atomic

	inicio := time.Now()
	fmt.Println("=== INICIANDO PIPELINE CONCURRENTE DE LIMPIEZA NLP ===")
	fmt.Printf("Workers activos: %d\n", numWorkers)
	fmt.Println("==============================================")

	// 2. Abrir el archivo CSV real
	archivo, err := os.Open(rutaArchivoCSV)
	if err != nil {
		log.Fatalf("Error al abrir el archivo CSV: %v", err)
	}
	defer archivo.Close()

	lectorCSV := csv.NewReader(archivo)

	cabeceras, err := lectorCSV.Read()
	if err != nil {
		log.Fatal("Error leyendo cabeceras:", err)
	}
	fmt.Printf("Detectadas %d columnas. Iniciando procesamiento...\n", len(cabeceras))

	// 3. Crear el Canal de Trabajos (Buffer)
	jobs := make(chan ExpedienteTC, 1000)

	// Canal para métricas en tiempo real
	done := make(chan struct{})

	// ==========================================
	// MONITOR EN TIEMPO REAL
	// ==========================================
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		var ultimoConteo int64

		for {
			select {
			case <-ticker.C:
				actual := atomic.LoadInt64(&contadorGlobal)
				procesados := actual - ultimoConteo
				ultimoConteo = actual

				elapsed := time.Since(inicio).Seconds()
				rps := float64(actual) / elapsed

				fmt.Printf("\n[MONITOR] Procesados: %d | +%d en últimos 2s | Velocidad: %.2f reg/s\n",
					actual, procesados, rps)

			case <-done:
				return
			}
		}
	}()

	// 4. Iniciar las Goroutines (Workers)
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go workerLimpiador(w, jobs, &wg, &contadorGlobal)
	}

	// 5. Ingesta: Leer el CSV fila por fila y enviar al canal (Productor)
	filasLeidas := 0
	inicioLectura := time.Now()

	for {
		fila, err := lectorCSV.Read()
		if err == io.EOF {
			break // Fin del archivo
		}
		if err != nil {
			log.Printf("Advertencia: Error leyendo fila %d: %v", filasLeidas+1, err)
			continue
		}

		filasLeidas++

		// Log de progreso de lectura
		if filasLeidas%10000 == 0 {
			fmt.Printf("[LECTURA] Filas leídas: %d | Tiempo: %v\n",
				filasLeidas, time.Since(inicioLectura))
		}

		exp := ExpedienteTC{
			FecIngreso:  fila[0],  // FEC_INGRESO
			Procedencia: fila[1],  // PROCEDENCIA
			TextoLegal:  fila[20], // Ajusta este índice si cambia el CSV
		}

		jobs <- exp
	}

	// 6. Cerrar el canal y esperar sincronización
	close(jobs)
	wg.Wait()
	close(done)

	tiempoTotal := time.Since(inicio)

	// ==========================================
	// TABLA DE RESULTADOS
	// ==========================================
	fmt.Println("\n==============================================")
	fmt.Println("          RESULTADOS FINALES")
	fmt.Println("==============================================")

	fmt.Printf("%-35s %10d\n", "Total filas leídas:", filasLeidas)
	fmt.Printf("%-35s %10d\n", "Total procesadas:", contadorGlobal)
	fmt.Printf("%-35s %10v\n", "Tiempo total:", tiempoTotal)

	if tiempoTotal.Seconds() > 0 {
		rendimiento := float64(contadorGlobal) / tiempoTotal.Seconds()
		fmt.Printf("%-35s %10.2f\n", "Rendimiento (reg/s):", rendimiento)
	}

	fmt.Println("==============================================")

	// Barra visual simple final
	fmt.Print("Progreso final: [")
	for i := 0; i < 50; i++ {
		fmt.Print("=")
	}
	fmt.Println("] 100%")
}
