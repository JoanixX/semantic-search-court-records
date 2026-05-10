package expedientes

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var dniPattern = regexp.MustCompile(`\b\d{8}\b`)

// NormalizarFecha estandariza fechas en formato YYYYMMDD a YYYY-MM-DD.
// Centralizar esto aquí evita duplicidad en los algoritmos de benchmark y el pipeline real.
func NormalizarFecha(fecha string) string {
	fecha = strings.TrimSpace(fecha)
	if fecha == "" || fecha == "--" || strings.ToLower(fecha) == "null" {
		return "N/A"
	}
	if len(fecha) == 10 && fecha[4] == '-' && fecha[7] == '-' {
		return fecha
	}
	if len(fecha) == 8 {
		return fmt.Sprintf("%s-%s-%s", fecha[0:4], fecha[4:6], fecha[6:8])
	}
	return fecha
}

// NormalizeText estandariza el texto para tareas de indexación y comparación.
func NormalizeText(text string) string {
	return strings.ToLower(strings.TrimSpace(strings.Join(strings.Fields(text), " ")))
}

// AnonymizeText reemplaza patrones de DNI detectados dentro del texto.
func AnonymizeText(text string) string {
	return dniPattern.ReplaceAllString(text, "[DNI_ANONIMIZADO]")
}

// CleanAndAnonymize encadena la normalización de texto con la anonimización y un delay simulado.
func CleanAndAnonymize(text string, delay time.Duration) string {
	if delay > 0 {
		time.Sleep(delay)
	}
	return AnonymizeText(NormalizeText(text))
}
