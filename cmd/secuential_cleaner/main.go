package main
import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"
	"flag"
)
var reDNI_secuencial = regexp.MustCompile(`\b\d{8}\b`)

func limpiarYAnonimizarSecuencial(texto string) string {
	time.Sleep(1 * time.Millisecond) // Simulación de carga CPU
	return reDNI_secuencial.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

func main() {
	//Configuración de flags para facilitar la ejecución
	inputPath := flag.String("csv", "../../datasets/processed/processed_records.csv", "Ruta al archivo CSV")
	outputPath := flag.String("output", "metricas_secuencial_100.csv", "CSV de métricas")	
	corridas := flag.Int("runs", 100, "Cantidad de corridas") //Mínimo pedido
	limite := flag.Int("limit", 20000, "Límite de registros a procesar por corrida") //muestra significativa
	flag.Parse()

	// Validación
	if *inputPath == "" {
		log.Fatal("Error: La flag -input es obligatoria. Proporciona la ruta del archivo CSV.")
	}

	// Guardamos las metricas
	fMetricas, err := os.Create(*outputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()

	fMetricas.WriteString("corrida,registros_procesados,tiempo_segundos\n")
	fmt.Printf("=== INICIANDO %d CORRIDAS SECUENCIALES (MUESTRA: %d) ===\n", *corridas, *limite)

	//Logica: Se harán 100 corridas, cada una procesando exactamente 20k registros diferentes. 
	// Si se llega al final del archivo, se reinicia el cursor para continuar leyendo.
	archivo, err := os.Open(*inputPath)
	if err != nil {
		log.Fatalf("Error al abrir archivo: %v", err)
	}
	lectorCSV := csv.NewReader(archivo)
	_, _ = lectorCSV.Read() // Ignorar la cabecera inicial

	// 2. Iniciamos las 100 corridas
	for i := 1; i <= *corridas; i++ {
		inicio := time.Now()
		procesados := 0

		// 3. Leemos exactamente hasta el límite, sin importar dónde se quedó el cursor
		for procesados < *limite {
			fila, err := lectorCSV.Read()
			
			// Si llegamos al final del archivo (EOF), hacemos el "reinicio"
			if err == io.EOF {
				fmt.Println("  [Aviso] Final del archivo alcanzado. Reiniciando cursor...")
				archivo.Close()
				
				archivo, err = os.Open(*inputPath)
				if err != nil {
					log.Fatalf("Error al reabrir archivo: %v", err)
				}
				lectorCSV = csv.NewReader(archivo)
				_, _ = lectorCSV.Read() // Ignorar la cabecera nuevamente
				continue // Volvemos al inicio del for para intentar leer la fila de nuevo
			}
			
			// Si hay otro tipo de error o la fila está incompleta, saltamos
			if err != nil || len(fila) <= 17 {
				continue
			}
			
			_ = limpiarYAnonimizarSecuencial(fila[17])
			procesados++
		}

		tiempo := time.Since(inicio).Seconds()
		fmt.Printf("Corrida %3d | Procesados: %d | Tiempo: %.4f s\n", i, procesados, tiempo)
		fMetricas.WriteString(fmt.Sprintf("%d,%d,%.4f\n", i, procesados, tiempo))
	}
	
	// Cerramos el archivo al finalizar todo el programa
	archivo.Close()
	fmt.Println("Resultados secuenciales guardados con éxito.")
}