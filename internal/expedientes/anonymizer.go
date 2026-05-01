package expedientes

import (
	"regexp"
	"strings"
	"time"
)

var dniPattern = regexp.MustCompile(`\b\d{8}\b`)

// NormalizeText estandariza el texto para tareas de indexación y comparación.
// La normalización es deliberadamente simple para no introducir dependencias externas.
func NormalizeText(text string) string {
	return strings.ToLower(strings.TrimSpace(strings.Join(strings.Fields(text), " ")))
}

// AnonymizeText reemplaza patrones de DNI detectados dentro del texto.
// Esta operación representa la sección crítica lógica del preprocesamiento legal.
func AnonymizeText(text string) string {
	return dniPattern.ReplaceAllString(text, "[DNI_ANONIMIZADO]")
}

// CleanAndAnonymize encadena una normalización ligera con la anonimización.
// El delay simulado permite mostrar en el informe el beneficio del trabajo concurrente.
func CleanAndAnonymize(text string, delay time.Duration) string {
	if delay > 0 {
		time.Sleep(delay)
	}
	return AnonymizeText(NormalizeText(text))
}
