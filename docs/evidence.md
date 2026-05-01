# Evidencias Reproducibles

Las salidas del proyecto se guardan en `evidence/`.

Archivos principales:

- `evidence/workflow.log`
- `evidence/tests.log`
- `evidence/analysis.log`
- `evidence/prep.log`
- `evidence/go.log`
- `evidence/workflow_summary.txt`
- `evidence/original_eda_summary.txt`
- `evidence/original_nulls_table.txt`
- `evidence/original_process_table.txt`
- `evidence/original_year_table.txt`
- `evidence/feature_eda_summary.txt`
- `evidence/feature_length_table.txt`
- `evidence/feature_word_table.txt`
- `evidence/feature_has_dni_table.txt`
- `evidence/feature_year_table.txt`
- `evidence/dataset_profile.txt`

## Como regenerarlas

```bash
python scripts/run_workflow.py
```

Si quieres correr cada etapa manualmente:

```bash
go test ./tests/unit ./tests/integration
python -m unittest discover -s tests/python -p "test_*.py"
python scripts/eda_original.py
python scrapers/augment_dataset.py --target-total 1000000
python scripts/merge_datasets.py
python scripts/validate_dataset.py --target 1000000
python scripts/eda_features.py
go run ./cmd/pipeline -csv datasets/raw/expedientes_tc_masivo.csv -workers 8 -delay-ms 1 -log-every 2000
go run ./cmd/benchmark -records 10000 -runs 3 -delay-ms 2
```
