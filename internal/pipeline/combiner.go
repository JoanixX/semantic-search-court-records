package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// TargetColumns define el orden oficial de 21 columnas del scraper para integridad total.
var TargetColumns = []string{
	"FEC_INGRESO", "PROCEDENCIA", "CDES_TIPOPROCESO", "SALA_ORIGEN",
	"TIPO_DEMANDANTE", "TIPO_DEMANDADO", "SALA", "FEC_VISTA",
	"MATERIA", "SUB_MATERIA", "ESPECIFICA", "PUB_PAGWEB",
	"PUB_PERUANO", "TIPO_RESOLUCION", "FALLO", "FEC_DEVPJ",
	"FEC_DEVPJ_1", "DEPARTAMENTO", "PROVINCIA", "DISTRITO",
	"RESUMEN_SENTENCIA",
}

// toUTF8 convierte una cadena a UTF-8 solo si no lo es, asumiendo Latin-1 de lo contrario.
func toUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	// Si no es UTF-8 válido, tratamos cada byte como un caracter Latin-1
	runes := make([]rune, len(s))
	for i := 0; i < len(s); i++ {
		runes[i] = rune(s[i])
	}
	return string(runes)
}

// CombineCSVs unifica archivos y registra duplicados exactos en un log de auditoría.
func CombineCSVs(rawDir, outputPath string) (int, error) {
	files, err := filepath.Glob(filepath.Join(rawDir, "*.csv"))
	if err != nil {
		return 0, err
	}

	if len(files) == 0 {
		return 0, fmt.Errorf("no se encontraron archivos CSV en %s", rawDir)
	}

	// Abrir log de duplicados para auditoría del usuario
	dupLogPath := "evidence/pipeline_logs/duplicates_found.log"
	os.MkdirAll(filepath.Dir(dupLogPath), 0755)
	dupFile, _ := os.Create(dupLogPath)
	defer dupFile.Close()

	outputFile, err := os.Create(outputPath + ".tmp")
	if err != nil {
		return 0, err
	}
	defer outputFile.Close()

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	if err := writer.Write(TargetColumns); err != nil {
		return 0, err
	}

	seen := make(map[string]struct{})
	totalRows := 0
	duplicateCount := 0

	for _, fileMatch := range files {
		f, err := os.Open(fileMatch)
		if err != nil {
			continue
		}

		reader := csv.NewReader(f)
		reader.LazyQuotes = true
		reader.FieldsPerRecord = -1

		header, err := reader.Read()
		if err != nil {
			f.Close()
			continue
		}

		colMap := make(map[string]int)
		for i, name := range header {
			cleanName := strings.ToUpper(strings.TrimSpace(name))
			// Normalización agresiva de cabeceras para match robusto
			cleanName = strings.ReplaceAll(cleanName, "Í", "I")
			cleanName = strings.ReplaceAll(cleanName, "É", "E")
			cleanName = strings.ReplaceAll(cleanName, "Á", "A")
			cleanName = strings.ReplaceAll(cleanName, "Ó", "O")
			cleanName = strings.ReplaceAll(cleanName, "Ú", "U")
			colMap[cleanName] = i
		}

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}

			unifiedRow := make([]string, len(TargetColumns))
			for i, target := range TargetColumns {
				if idx, ok := colMap[target]; ok && idx < len(row) {
					unifiedRow[i] = toUTF8(row[idx])
				}
			}

			// DEDUPLICACIÓN 100% ESTRICTA: Solo si la fila entera (incluyendo resumen) es igual.
			rowKey := strings.Join(unifiedRow, "|")
			
			if _, exists := seen[rowKey]; !exists {
				if err := writer.Write(unifiedRow); err != nil {
					break
				}
				seen[rowKey] = struct{}{}
				totalRows++
			} else {
				duplicateCount++
				if duplicateCount < 10000 {
					dupFile.WriteString(fmt.Sprintf("DUPLICADO: %s\n", rowKey[:min(len(rowKey), 100)]))
				}
			}
		}
		f.Close()
	}

	writer.Flush()
	outputFile.Close()

	if totalRows > 0 {
		os.Remove(outputPath)
		os.Rename(outputPath+".tmp", outputPath)
	}

	return totalRows, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
