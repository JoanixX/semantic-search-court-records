package expedientes

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

// ValidateDataset cuenta las filas de un CSV y verifica si cumple con el objetivo mínimo.
// Genera un reporte en evidence/ para trazabilidad.
func ValidateDataset(path string, target int, reportPath string) (int, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Omitir cabecera
	if _, err := reader.Read(); err != nil {
		return 0, false, err
	}

	count := 0
	for {
		_, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		count++
	}

	satisfied := count >= target
	status := "no cumple"
	if satisfied {
		status = "cumple"
	}

	// Generar reporte de texto
	report := fmt.Sprintf("Validacion del dataset\n\nArchivo: %s\nRegistros: %d\nObjetivo: %d\nEstado: %s\n",
		path, count, target, status)
	
	_ = os.WriteFile(reportPath, []byte(report), 0644)

	return count, satisfied, nil
}
