package expedientes

import "fmt"

var sampleSentences = []string{
	"El demandante con DNI %08d solicita revisión del expediente.",
	"La parte actora reporta vulneración del derecho con DNI %08d.",
	"Se registra escrito de apelación asociado al documento %08d.",
	"El tribunal evalúa la pretensión con identificación %08d y texto complementario.",
	"Se detecta coincidencia parcial en el expediente con DNI %08d.",
}

// GenerateSyntheticRecords crea datos controlados para benchmarking y pruebas.
// Se usa para reproducir la comparación secuencial vs concurrente sin depender del CSV real.
func GenerateSyntheticRecords(n int) []Record {
	records := make([]Record, 0, n)
	for i := 0; i < n; i++ {
		template := sampleSentences[i%len(sampleSentences)]
		text := fmt.Sprintf(template, 10000000+(i%89999999))
		records = append(records, Record{
			FecIngreso:  fmt.Sprintf("2026-01-%02d", (i%28)+1),
			Procedencia: "LIMA",
			TipoProceso: "AMPARO",
			TextoLegal:  text,
			Source:      fmt.Sprintf("synthetic-%d", i+1),
		})
	}
	return records
}
