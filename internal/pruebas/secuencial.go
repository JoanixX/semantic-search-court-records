package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"semantic-search-court-records/internal/expedientes"
	"time"
)

func EjecutarPruebaSecuencial() {
	rutaArchivoCSV := "../../datasets/processed/processed_records.csv"

	fMetricas, err := os.Create(
		"../../evidence/metrics_results/metricas_secuencial.csv",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer fMetricas.Close()

	fMetricas.WriteString(
		"corrida,tiempo_segundos,heap_mb,gc\n",
	)

	totalCorridas := 10
	fmt.Println("=== INICIANDO PRUEBAS SECUENCIALES ===")

	for corrida := 1; corrida <= totalCorridas; corrida++ {
		archivo, err := os.Open(rutaArchivoCSV)
		if err != nil {
			log.Fatalf("Error abriendo CSV: %v", err)
		}

		lectorCSV := csv.NewReader(archivo)
		_, _ = lectorCSV.Read()

		var memInicio runtime.MemStats
		runtime.ReadMemStats(&memInicio)

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
			if len(fila) <= 11 {
				continue
			}
			_ = expedientes.NormalizarFecha(fila[0])
			_ = expedientes.NormalizarFecha(fila[11])
			_ = expedientes.CleanAndAnonymize(
				fila[10],
				1*time.Nanosecond,
			)
			procesados++
		}

		archivo.Close()
		tiempo := time.Since(inicio).Seconds()

		var memFin runtime.MemStats
		runtime.ReadMemStats(&memFin)

		heapMB := float64(memFin.Alloc) / (1024 * 1024)

		fmt.Printf(
			"[SECUENCIAL] Corrida %d | Tiempo: %.4f s | Heap: %.2f MB | GC: %d | Registros: %d\n",
			corrida,
			tiempo,
			heapMB,
			memFin.NumGC,
			procesados,
		)

		fMetricas.WriteString(
			fmt.Sprintf(
				"%d,%.4f,%.2f,%d\n",
				corrida,
				tiempo,
				heapMB,
				memFin.NumGC,
			),
		)
	}
}
