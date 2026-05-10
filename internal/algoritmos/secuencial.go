package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"
)

var reDNI_secuencial = regexp.MustCompile(`\b\d{8}\b`)

func limpiarYAnonimizarSecuencial(texto string) string {
	time.Sleep(1 * time.Millisecond) // Simulación de carga CPU
	return reDNI_secuencial.ReplaceAllString(texto, "[DNI_ANONIMIZADO]")
}

func EjecutarPruebaSecuencial() {
	rutaArchivoCSV := "../../datasets/processed/processed_records.csv"
	
	fMetricas, err := os.Create("../../evidence/metrics_results/metricas_secuencial.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()
	fMetricas.WriteString("corrida,tiempo_segundos\n")

	// Puntos de control solicitados: 10 y 25 corridas
	puntosControl := []int{10, 25}

	fmt.Println("=== INICIANDO PRUEBAS SECUENCIALES (PUNTOS DE CONTROL: 10, 25) ===")
	
	actualCorrida := 1
	for _, limite := range puntosControl {
		fmt.Printf("--- Iniciando bloque hasta %d corridas ---\n", limite)
		for actualCorrida <= limite {
			archivo, err := os.Open(rutaArchivoCSV)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}

		lectorCSV := csv.NewReader(archivo)
		_, _ = lectorCSV.Read()

		inicio := time.Now()
		procesados := 0

		for {
			fila, err := lectorCSV.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			_ = limpiarYAnonimizarSecuencial(fila[17])
			procesados++
		}
		archivo.Close()

		tiempo := time.Since(inicio).Seconds()
		fmt.Printf("Secuencial - Corrida %d: %.4f segundos\n", actualCorrida, tiempo)
		fMetricas.WriteString(fmt.Sprintf("%d,%.4f\n", actualCorrida, tiempo))
		actualCorrida++
	}
    }
}
