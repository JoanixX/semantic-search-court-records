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

func main() {
	rutaArchivoCSV := "../datasets/processed/processed_records.csv"
	corridas := 10 // El secuencial es lento, 10 corridas para testear

	// Guardamos las metricas
	fMetricas, err := os.Create("metricas_secuencial.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()
	fMetricas.WriteString("corrida,tiempo_segundos\n")

	fmt.Printf("=== INICIANDO %d CORRIDAS SECUENCIALES ===\n", corridas)
	for i := 1; i <= corridas; i++ {
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
		fmt.Printf("Secuencial - Corrida %d: %.4f segundos\n", i, tiempo)
		fMetricas.WriteString(fmt.Sprintf("%d,%.4f\n", i, tiempo))
	}
}