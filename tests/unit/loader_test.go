package unit_test

import (
	"testing"

	"semantic-search-court-records/internal/expedientes"
)

func TestRecordFromCSVRowValidatesLength(t *testing.T) {
	_, err := expedientes.RecordFromCSVRow([]string{"a", "b"})
	if err == nil {
		t.Fatalf("se esperaba error por fila incompleta")
	}
}
