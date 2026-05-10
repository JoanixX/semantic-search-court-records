package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"semantic-search-court-records/internal/expedientes"
	"time"
)

func main() {
	// 1. Definición de flags para cumplir con la flexibilidad del Trabajo Parcial
	inputPath := flag.String("input", "datasets/processed/processed_records.csv", "Ruta del dataset de entrada")
	workers := flag.Int("workers", 8, "Número de goroutines en el Worker Pool")
	chunkSize := flag.Int("chunk", 100, "Tamaño del buffer de los canales (gestión de memoria)")
	logEvery := flag.Int("log-every", 10000, "Frecuencia de logs de progreso")
	simulatedCost := flag.Duration("cost", 0, "Costo simulado por registro (ej. 1ms)")

	// Nuevas fases integradas
	combine := flag.Bool("combine", false, "Unificar archivos CSV de la carpeta raw antes de procesar")
	validate := flag.Bool("validate", true, "Validar cantidad mínima de registros antes de procesar")

	flag.Parse()

	// 2. Configuración de Logging en carpeta evidence/
	logDir := "evidence/pipeline_logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Error creando directorio de logs: %v", err)
	}

	logFile := filepath.Join(logDir, fmt.Sprintf("execution_%s.log", time.Now().Format("20060102_150405")))
	f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	multiLogger := log.New(os.Stdout, "[PIPELINE] ", log.LstdFlags)
	fileLogger := log.New(f, "", log.LstdFlags)

	// FASE 1: Combinación (Opcional y con precaución)
	if *combine {
		multiLogger.Printf("--- FASE: COMBINACIÓN DE ARCHIVOS RAW ---")
		rawDir := "datasets/raw"

		// Verificar si el archivo de destino ya es grande para evitar sobreescrituras accidentales
		if info, err := os.Stat(*inputPath); err == nil && info.Size() > 50*1024*1024 { // > 50MB
			multiLogger.Printf("AVISO: %s ya existe y es grande (%d MB).", *inputPath, info.Size()/(1024*1024))
			multiLogger.Printf("La combinación podría reducir el tamaño si hay muchos duplicados.")
		}

		totalCombined, err := CombineCSVs(rawDir, *inputPath)
		if err != nil {
			multiLogger.Fatalf("Error crítico en combinación: %v", err)
		}
		multiLogger.Printf("Éxito: %d registros unificados en %s", totalCombined, *inputPath)
	}

	// FASE 2: Validación
	if *validate {
		multiLogger.Printf("Iniciando Fase: Validación de integridad...")
		reportPath := "evidence/pipeline_logs/validation_summary.txt"
		count, ok, err := expedientes.ValidateDataset(*inputPath, 1000000, reportPath)
		if err != nil {
			multiLogger.Fatalf("Error en validación: %v", err)
		}
		if !ok {
			multiLogger.Printf("ADVERTENCIA: El dataset tiene %d registros (objetivo: 1M). Continuando...", count)
		} else {
			multiLogger.Printf("Validación exitosa: %d registros detectados.", count)
		}
	}

	multiLogger.Printf("Iniciando Fase: Limpieza Paralela y Anonimización")
	multiLogger.Printf("Configuración: Workers=%d, ChunkSize=%d, Cost=%v", *workers, *chunkSize, *simulatedCost)

	// 3. Inicialización del Procesador Modular
	proc := expedientes.Processor{
		Workers:       *workers,
		ChunkSize:     *chunkSize,
		LogEvery:      *logEvery,
		SimulatedCost: *simulatedCost,
		Logger:        fileLogger,
	}

	// 4. Ejecución del Pipeline (Patrón Productor-Trabajador-Consumidor)
	tempOutput := *inputPath + ".tmp"
	multiLogger.Printf("Procesando archivo: %s", *inputPath)
	result, err := proc.RunPipeline(*inputPath, tempOutput)
	if err != nil {
		multiLogger.Fatalf("Error crítico en el pipeline: %v", err)
	}

	// 5. Sobreescritura segura: Reemplazar el original con el procesado
	multiLogger.Printf("Finalizando limpieza. Sobreescribiendo archivo original...")
	if err := os.Remove(*inputPath); err != nil {
		multiLogger.Printf("Advertencia: No se pudo eliminar el original: %v", err)
	}
	if err := os.Rename(tempOutput, *inputPath); err != nil {
		multiLogger.Fatalf("Error fatal al renombrar archivo temporal: %v", err)
	}

	// 6. Informe de Resultados Finales
	multiLogger.Printf("=== PROCESAMIENTO COMPLETADO ===")
	multiLogger.Printf("Registros procesados: %d", result.Processed)
	multiLogger.Printf("Duración total: %v", result.Duration)
	multiLogger.Printf("Rendimiento: %.2f registros/seg", float64(result.Processed)/result.Duration.Seconds())
	multiLogger.Printf("Dataset original sobreescrito con éxito.")
	multiLogger.Printf("Log detallado guardado en: %s", logFile)
}
