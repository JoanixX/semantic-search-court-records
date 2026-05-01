package integration_test

import (
	"bytes"
	"log"
	"testing"
	"time"

	"semantic-search-court-records/internal/expedientes"
)

func TestConcurrentProcessorCompletesAllRecords(t *testing.T) {
	records := expedientes.GenerateSyntheticRecords(100)
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	processor := expedientes.Processor{
		Workers:       4,
		ChunkSize:     16,
		LogEvery:      25,
		SimulatedCost: 1 * time.Millisecond,
		Logger:        logger,
	}

	seq := processor.Sequential(records)
	conc := processor.Concurrent(records)

	if seq.Processed != int64(len(records)) {
		t.Fatalf("secuencial no procesó todos los registros: got=%d want=%d", seq.Processed, len(records))
	}
	if conc.Processed != int64(len(records)) {
		t.Fatalf("concurrente no procesó todos los registros: got=%d want=%d", conc.Processed, len(records))
	}
	if conc.Duration <= 0 || seq.Duration <= 0 {
		t.Fatalf("se esperaban duraciones positivas")
	}
	if len(buf.String()) == 0 {
		t.Fatalf("se esperaban logs de trazabilidad")
	}
}
