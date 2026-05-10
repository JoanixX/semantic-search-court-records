package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"semantic-search-court-records/internal/expedientes"
	"time"
)

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

				// Estandarización de fechas usando el módulo centralizado
				fecIngreso := expedientes.NormalizarFecha(fila[0])
				pubPagWeb := expedientes.NormalizarFecha(fila[11])

				// Anonimización usando el módulo centralizado (delay simulado de 1ms)
				especificaLimpia := expedientes.CleanAndAnonymize(fila[10], 1*time.Millisecond)

				_ = fecIngreso
				_ = pubPagWeb
				_ = especificaLimpia

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
