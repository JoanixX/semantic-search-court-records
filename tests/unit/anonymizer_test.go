package unit_test

import (
	"testing"

	"semantic-search-court-records/internal/expedientes"
)

func TestAnonymizeTextReplacesDNI(t *testing.T) {
	got := expedientes.AnonymizeText("El demandante con DNI 12345678 solicita revisión.")
	want := "El demandante con DNI [DNI_ANONIMIZADO] solicita revisión."
	if got != want {
		t.Fatalf("resultado inesperado: got=%q want=%q", got, want)
	}
}

func TestTrimmedMeanIgnoresExtremes(t *testing.T) {
	got := expedientes.TrimmedMean([]float64{1, 2, 100, 3, 4})
	if got != 3 {
		t.Fatalf("media recortada incorrecta: got=%v want=3", got)
	}
}
