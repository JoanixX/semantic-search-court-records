# Scraper y Aumento de Datos

## Propósito

El subsistema de scraping, ubicado en `scrapers/`, es responsable de la adquisición de datos reales desde fuentes judiciales oficiales (Tribunal Constitucional, Datos Abiertos). Su objetivo es garantizar que el dataset supere el umbral de 1,000,000 de registros sin comprometer la veracidad de la información.

## Arquitectura Modular

El scraper ha sido refactorizado en módulos especializados para mejorar la mantenibilidad y robustez:

- **`base_utils.py`**: Utilidades comunes de red, manejo de proxies y logs.
- **`extraction.py`**: Lógica de extracción multiformato (HTML, PDF, CSV, ZIP, XLSX, JSON).
- **`crawling.py`**: Gestión de semillas y exploración recursiva de endpoints oficiales.
- **`augment_dataset.py`**: Orquestador del scraper que integra los módulos anteriores.

## Funcionamiento en el Workflow

Dentro del flujo de 10 pasos (`run_workflow.py`), el scraper se ejecuta en el **Paso 3**. Utiliza una estrategia de descubrimiento dinámico de semillas para priorizar fuentes que generen registros con mayor densidad de datos.

## Comandos de Ejecución

### Uso Estándar (Recomendado)
```bash
python scrapers/augment_dataset.py --target-total 1000000
```

### Configuración Avanzada para Redes Restringidas
```bash
python scrapers/augment_dataset.py --target-total 1000000 --proxy http://mi-proxy:8080 --timeout 120
```

### Prueba Rápida de Conectividad
```bash
python scrapers/augment_dataset.py --target-total 100 --max-pages 5
```

## Características de Robustez

- **Manejo de XLSX**: Procesa hojas masivas de forma incremental para evitar desbordamiento de memoria.
- **Detección de Semillas Muertas**: Registra fuentes con rendimiento nulo para omitirlas en ejecuciones posteriores.
- **Validación de Formatos**: Salta contenido binario corrupto o HTML malformado automáticamente.
- **Trazabilidad**: Genera reportes de cosecha en `evidence/official_harvest_summary.txt`.

## Verificación
```bash
python -m unittest discover -s tests/python -p "test_scraper.py"
```
