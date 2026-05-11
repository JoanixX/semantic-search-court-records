# Pipeline Concurrente Principal

## Propósito

El pipeline principal, ubicado en `internal/pipeline/`, es el motor encargado de procesar el dataset real de expedientes judiciales. Su objetivo es realizar la limpieza de datos, normalización y anonimización de información sensible (DNI) de forma eficiente.

## Cómo ejecutar

```bash
go run ./internal/pipeline -input datasets/raw/dataset.csv -workers 12 -chunk 1000 -validate
```

## Parámetros de Ejecución

- `-input`: Ruta al archivo CSV original (por defecto `datasets/processed/processed_records.csv`).
- `-workers`: Cantidad de hilos (goroutines) concurrentes para el procesamiento.
- `-chunk`: Tamaño del buffer de los canales (gestion de memoria y contención).
- `-validate`: Activa la validación de integridad (conteo de registros y campos) antes del proceso.
- `-cost`: Simula un costo de procesamiento artificial (ej. `1ms`) para pruebas de latencia.
- `-combine`: Opcional. Unifica archivos CSV de la carpeta `raw` antes de iniciar el pipeline.

## Ciclo de Vida del Registro

1. **Lectura**: El Productor extrae líneas del CSV y las convierte en estructuras `Expediente`.
2. **Validación**: Se verifica la estructura mínima requerida.
3. **Limpieza y Anonimización**:
    - Normalización de fechas y textos.
    - Ofuscación de números de DNI detectados en el texto.
4. **Escritura**: El Consumidor escribe los registros procesados en un archivo temporal de forma secuencial para evitar corrupción.
5. **Finalización**: Se reemplaza el archivo original por el procesado tras la validación final.

## Trazabilidad

- Los logs detallados se guardan en `evidence/pipeline_logs/`.
- Cada ejecución reporta el rendimiento final en registros procesados por segundo.

