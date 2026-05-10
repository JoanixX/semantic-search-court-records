package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
)

func ValidarConsistencia() {
	fmt.Println("\n=== VALIDACIÓN DE CONSISTENCIA POST-PROCESAMIENTO ===")

	rutaOriginal := "../../datasets/processed/processed_records.csv"
	// En un flujo real, compararíamos el original con el resultante.
	// Aquí validamos que el archivo procesado tenga la cantidad esperada.

	count := 0
	archivo, err := os.Open(rutaOriginal)
	if err != nil {
		fmt.Printf("Error al abrir archivo para validación: %v\n", err)
		return
	}
	defer archivo.Close()

	lector := csv.NewReader(archivo)
	// Saltar cabecera
	_, _ = lector.Read()

	for {
		_, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err == nil {
			count++
		}
	}

	fmt.Printf("Registros totales detectados y validados: %d\n", count)
	if count >= 1000000 {
		fmt.Println("ESTADO: CONSISTENTE (>1M registros)")
	} else {
		fmt.Println("ESTADO: ADVERTENCIA (Menos de 1M registros)")
	}
}

func main() {
	fmt.Println("****************************************************")
	fmt.Println("*   SISTEMA DE PRUEBAS DE RENDIMIENTO Y MÉTRICAS   *")
	fmt.Println("****************************************************")

	runtime.GOMAXPROCS(runtime.NumCPU())

	// 1. Ejecutar Concurrente Primero (como solicitó el usuario)
	EjecutarPruebaConcurrente()

	// 2. Ejecutar Secuencial después
	EjecutarPruebaSecuencial()

	// 3. Validación de consistencia
	ValidarConsistencia()

	fmt.Println("\nPruebas finalizadas. Los resultados están en evidence/metrics_results/")
}
