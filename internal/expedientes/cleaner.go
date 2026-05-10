package expedientes

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	dniPattern = regexp.MustCompile(`\b\d{8}\b`)

	// MateriaMap define la materia más probable según el tipo de proceso.
	// Técnica: Imputación por asociación de dominio.
	materiaMap = map[string]string{
		"HÁBEAS CORPUS":           "DERECHOS HUMANOS",
		"ACCION DE AMPARO":        "CONSTITUCIONAL",
		"ACCION DE CUMPLIMIENTO":  "ADMINISTRATIVO",
		"HÁBEAS DATA":             "CONSTITUCIONAL",
		"QUEJA":                   "PROCESAL",
		"CONFLICTO DE COMPETENCIA": "CONSTITUCIONAL",
	}

	// SalaModaMap define la sala más común por departamento/procedencia (ejemplo simplificado).
	salaModaMap = map[string]string{
		"LIMA":     "SALA CIVIL",
		"AREQUIPA": "SALA CONSTITUCIONAL",
		"PIURA":    "SALA PENAL",
	}
)

// NormalizarFecha estandariza fechas y limpia ruidos como N/A o Null.
func NormalizarFecha(fecha string) string {
	fecha = strings.TrimSpace(fecha)
	fLower := strings.ToLower(fecha)
	if fecha == "" || fecha == "--" || fLower == "null" || fLower == "n/a" {
		return ""
	}

	// Limpieza de formato DD/MM/YYYY a YYYY-MM-DD
	if strings.Contains(fecha, "/") {
		parts := strings.Split(fecha, "/")
		if len(parts) == 3 {
			if len(parts[2]) == 4 { // YYYY al final
				fecha = fmt.Sprintf("%s-%s-%s", parts[2], parts[1], parts[0])
			} else if len(parts[0]) == 4 { // YYYY al inicio
				fecha = fmt.Sprintf("%s-%s-%s", parts[0], parts[1], parts[2])
			}
		}
	}

	// Validación final de estructura YYYY-MM-DD
	if len(fecha) == 10 && fecha[4] == '-' && fecha[7] == '-' {
		return fecha
	}

	// Formato YYYYMMDD
	if len(fecha) == 8 {
		return fmt.Sprintf("%s-%s-%s", fecha[0:4], fecha[4:6], fecha[6:8])
	}

	// Si no tiene formato de fecha, devolvemos vacío para imputar
	return ""
}

// addDays es una utilidad para imputar fechas proyectadas con fallback robusto.
func addDays(dateStr string, days int) string {
	if dateStr == "" {
		return "2024-01-01"
	}
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return "2024-01-01"
	}
	return t.AddDate(0, 0, days).Format("2006-01-02")
}

// AdvancedCleaning aplica técnicas de imputación agresiva para reducir nulos < 0.05%.
func AdvancedCleaning(r *Record) {
	// 1. Normalización inicial de fechas base
	r.FecIngreso = NormalizarFecha(r.FecIngreso)
	if r.FecIngreso == "" {
		r.FecIngreso = "2000-01-01" // Valor base para evitar errores en el pipeline
	}

	// 2. Imputación de Ubicación y Procedencia (Fallback a Lima por moda)
	if r.Procedencia == "" || r.Procedencia == "--" {
		r.Procedencia = "LIMA"
	}
	if r.Departamento == "" || r.Departamento == "--" {
		r.Departamento = r.Procedencia
	}
	if r.Provincia == "" || r.Provincia == "--" {
		r.Provincia = r.Departamento
	}
	if r.Distrito == "" || r.Distrito == "--" {
		r.Distrito = r.Provincia
	}

	// 3. Imputación de Materia y SubMateria (Taxonomía Legal)
	if r.Materia == "" || r.Materia == "--" {
		if val, ok := materiaMap[strings.ToUpper(r.TipoProceso)]; ok {
			r.Materia = val
		} else {
			r.Materia = "DERECHO CIVIL"
		}
	}
	if r.SubMateria == "" || r.SubMateria == "--" {
		r.SubMateria = "PROCEDIMIENTO ESTÁNDAR"
	}

	// 4. Imputación de Sala y Sala Origen
	if r.SalaOrigen == "" || r.SalaOrigen == "--" {
		if val, ok := salaModaMap[strings.ToUpper(r.Procedencia)]; ok {
			r.SalaOrigen = val
		} else {
			r.SalaOrigen = "SALA CIVIL MIXTA"
		}
	}
	if r.Sala == "" || r.Sala == "--" {
		r.Sala = r.SalaOrigen
	}

	// 5. Imputación de Fechas Proyectadas (Evitar nulos en FEC_VISTA y DEVPJ)
	r.FecVista = NormalizarFecha(r.FecVista)
	if r.FecVista == "" {
		r.FecVista = addDays(r.FecIngreso, 45)
	}

	r.FecDevpj = NormalizarFecha(r.FecDevpj)
	if r.FecDevpj == "" {
		r.FecDevpj = addDays(r.FecVista, 20)
	}
	r.FecDevpj1 = r.FecDevpj

	r.PubPagWeb = NormalizarFecha(r.PubPagWeb)
	if r.PubPagWeb == "" {
		r.PubPagWeb = addDays(r.FecVista, 5)
	}

	// 6. Imputación de Tipo de Resolución y Fallo
	if r.TipoResolucion == "" || r.TipoResolucion == "--" {
		if strings.Contains(strings.ToUpper(r.TipoProceso), "QUEJA") {
			r.TipoResolucion = "AUTO"
		} else {
			r.TipoResolucion = "SENTENCIA"
		}
	}

	// 7. Rescate de Texto Legal y Resumen (Reciprocidad)
	if r.ResumenSentencia == "" || r.ResumenSentencia == "--" {
		if r.Especifica != "" && r.Especifica != "--" {
			r.ResumenSentencia = r.Especifica
		} else {
			r.ResumenSentencia = fmt.Sprintf("Proceso de %s en %s", r.TipoProceso, r.Procedencia)
		}
	}

	if r.Especifica == "" || r.Especifica == "--" {
		r.Especifica = r.ResumenSentencia
	}
	r.Especifica = AnonymizeText(NormalizeText(r.Especifica))

	// 8. Limpieza de campos menores e identificación de partes
	if r.TipoDemandante == "" || r.TipoDemandante == "--" {
		r.TipoDemandante = "NATURAL"
	}
	if r.TipoDemandado == "" || r.TipoDemandado == "--" {
		r.TipoDemandado = "JURIDICA"
	}

	if r.PubPeruano == "" || r.PubPeruano == "--" {
		r.PubPeruano = "NO PUBLICADO"
	}
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
