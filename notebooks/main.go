package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sync"
	"time"
)

// ExpedienteTC representa la estructura real de tu dataset
// Solo mapeamos los campos que nos interesan para la limpieza (ID/Fecha y el Texto)
type ExpedienteTC struct {
	FecIngreso string
	Procedencia string
	TextoLegal string // Esta es la columna 'TEXT' que agregaste
}

// FUNCION INTENSIVA (NLP / Regex): Anonimización de datos sensibles
func limpiarYAnonimizar(texto string) string {
	// Simulamos carga de CPU (quitar esto en producción)
	time.Sleep(1 * time.Millisecond) 
	
	// Expresión regular para buscar DNIs (8 dígitos consecutivos)
	reDNI := regexp.MustCompile(`\b\d{8}\b`)
	textoLimpio := reDNI.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
	
	// Aquí podrías agregar más regex (ej. nombres propios, correos, etc.)
	return textoLimpio
}

// ==========================================
// WORKER POOL CONCURRENTE
// ==========================================
func workerLimpiador(id int, jobs <-chan ExpedienteTC, wg *sync.WaitGroup, mu *sync.Mutex, contadorGlobal *int) {
	defer wg.Done()
	
	for exp := range jobs {
		// 1. Procesamiento pesado fuera de la sección crítica
		textoAnonimizado := limpiarYAnonimizar(exp.TextoLegal)
		
		// Opcional: Aquí escribirías 'textoAnonimizado' a un nuevo archivo o base de datos.
		// (Para la prueba, solo lo procesamos en memoria).
		_ = textoAnonimizado 

		// 2. SECCIÓN CRÍTICA: Actualizamos el contador de forma segura
		mu.Lock()
		*contadorGlobal++
		mu.Unlock()
	}
}

func main() {
	rutaArchivoCSV := "./datasets/raw/expedientes_tc_masivo.csv"
	numWorkers := 8 

	var wg sync.WaitGroup
	var mu sync.Mutex
	anonimizadosTotal := 0

	inicio := time.Now()
	fmt.Println("=== INICIANDO PIPELINE CONCURRENTE DE LIMPIEZA NLP ===")
	
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
	// Un buffer de 1000 evita que la lectura del CSV agote la memoria RAM
	jobs := make(chan ExpedienteTC, 1000)

	// 4. Iniciar las Goroutines (Workers)
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go workerLimpiador(w, jobs, &wg, &mu, &anonimizadosTotal)
	}

	// 5. Ingesta: Leer el CSV fila por fila y enviar al canal (Productor)
	filasLeidas := 0
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

		// Mapear las columnas a la estructura. 
		// IMPORTANTE: Asegúrate de que el índice [20] corresponda a tu columna "TEXT".
		// Si "TEXT" es la columna 21 (después de las 20 originales), el índice es 20 en Go.
		exp := ExpedienteTC{
			FecIngreso: fila[0], // FEC_INGRESO
			Procedencia: fila[1], // PROCEDENCIA
			TextoLegal: fila[20], // Ajusta este número al índice de tu columna TEXT
		}

		// Enviar el expediente al Worker Pool
		jobs <- exp
	}

	// 6. Cerrar el canal y esperar sincronización
	close(jobs) // Indica a los workers que ya no hay más líneas por leer
	wg.Wait()   // El hilo principal espera a que todos los workers terminen

	tiempoTotal := time.Since(inicio)
	
	// 7. Resultados
	fmt.Println("\n=== RESULTADOS DE LA EJECUCIÓN ===")
	fmt.Printf("Total de filas leídas: %d\n", filasLeidas)
	fmt.Printf("Total de expedientes anonimizados: %d\n", anonimizadosTotal)
	fmt.Printf("Tiempo total de procesamiento: %v\n", tiempoTotal)
	if tiempoTotal.Seconds() > 0 {
		rendimiento := float64(anonimizadosTotal) / tiempoTotal.Seconds()
		fmt.Printf("Rendimiento: %.2f registros por segundo\n", rendimiento)
	}
}