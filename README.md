# Semantic Search Court Records — Concurrent anonymization pipeline for Peruvian court files

A Go worker-pool pipeline that normalizes and anonymizes the text of Peruvian
Constitutional Court case records, plus a Python scraper and EDA layer that build the
corpus. The source dataset is 149,387 real case records; the pipeline scales to a
combined corpus of roughly 1.4M rows. The measurement question the repository answers is
how a channel-fed goroutine pool scales from 15 to 150 workers over 1,100 timed runs.

**Docs index:** [docs/README.md](docs/README.md) · **License:** MIT

## Results

Measured over 1,100 timed runs committed to the repository — 100 sequential runs and 100
runs at each of 10 worker counts. All figures are trimmed means (min and max discarded),
matching `expedientes.TrimmedMean`.

Concurrent pipeline, full processed corpus per run (`cmd/concurrent_cleaner`):

| Workers | Trimmed mean (s) | Speedup vs 15 workers | Scaling efficiency |
|---|---|---|---|
| 15 | 98.31 | 1.00x | 100% |
| 30 | 49.12 | 2.00x | 100% |
| 45 | 32.83 | 2.99x | 100% |
| 60 | 23.93 | 4.11x | 103% |
| 75 | 18.83 | 5.22x | 104% |
| 90 | 15.62 | 6.29x | 105% |
| 105 | 13.87 | 7.09x | 101% |
| 120 | 12.17 | 8.08x | 101% |
| 135 | 10.76 | 9.14x | 102% |
| 150 | 9.58 | 10.26x | 103% |

Sequential baseline, 20,000 records per run (`cmd/secuential_cleaner`):

| Metric | Value |
|---|---|
| Runs | 100 |
| Trimmed mean | 26.34 s |
| Standard deviation | 0.54 s (2.1% of the mean) |
| Throughput | 759 records/s |

**Scaling is linear to 150 goroutines** — 10x the workers gives 10.26x the throughput,
with efficiency at or above 100% at every step. That is the honest headline, and it is
also the warning: efficiency above 100% is only achievable because the per-record cost is
an artificial `time.Sleep`, not CPU work (see Limitations). The two tables are **not
divisible into a single speedup number**: the sequential runs process a fixed 20,000
records while the concurrent runs consume the whole corpus, so the volumes differ by
roughly 70x.

Evaluation protocol: 100 repetitions per configuration, trimmed mean to suppress
scheduler outliers, single machine, no warm-up discarded beyond the trim. The sequential
baseline re-opens the CSV and wraps the cursor to guarantee a full 20,000 records per run.

Reproduce:

```bash
go run ./cmd/secuential_cleaner -runs 100 -limit 20000
go run ./cmd/concurrent_cleaner -runs 100 -max-workers 150 -step 15
go run ./cmd/benchmark -records 20000 -runs 3 -delay-ms 2   # synthetic sweep
python notebooks/metrics/metrics.py                          # speedup + scaling plots
```

## How it works

- **Two entrypoint pairs exist on purpose.** `cmd/pipeline` and `cmd/benchmark` run the
  library implementation (`internal/expedientes`) over real and synthetic data
  respectively; `cmd/secuential_cleaner` and `cmd/concurrent_cleaner` are the standalone
  measurement harnesses that produced the committed CSVs. Keeping measurement out of the
  production path stops instrumentation cost from contaminating the numbers it reports.
- **The worker pool is fed by a buffered channel and closed to signal completion** — a
  5,000-slot job channel with `close(jobs)` plus `WaitGroup` rather than a sentinel value
  per worker, so adding workers requires no change to the shutdown logic.
- **Global accounting uses `atomic.AddInt64`, not a mutex.** The counter is the only
  shared state in the hot loop; a mutex there would serialize every worker on each record
  and cap scaling well below the 150 goroutines measured above.
- **The trimmed mean drops the minimum and maximum** before averaging. With 100 runs on a
  non-isolated machine, a single scheduler stall or background process would otherwise
  move the reported time by more than the effect being measured.
- **Anonymization is a compiled package-level regex** (`\b\d{8}\b` → `[DNI_ANONIMIZADO]`),
  compiled once at init rather than per record, matching the 8-digit Peruvian DNI format.
- **Malformed CSV rows are skipped, not fatal.** `LoadCSVRecords` drops unparseable rows
  and continues, because a single bad row in a scraped 1.4M-row corpus should not abort a
  90-second batch.
- **A Promela model (`modelo1.pml`, `notebooks/simulation.pml`) accompanies the
  implementation**, so the concurrency design can be checked for deadlock and starvation
  by model checking rather than by hoping the race detector sees the interleaving.

## Quick start

```bash
go test ./tests/unit ./tests/integration
python -m unittest discover -s tests/python -p "test_*.py"
python scripts/run_workflow.py          # full orchestrated pipeline
```

The orchestrator runs, in order: tests, EDA of the original dataset, the scraper, merge
and validation, feature engineering, then the Go pipeline and benchmark. Individual
stages are documented in [docs/flow.md](docs/flow.md) and
[docs/pipeline.md](docs/pipeline.md).

Run logs land in `evidence/tests.log`, `evidence/analysis.log`, `evidence/prep.log`,
`evidence/go.log`, with `evidence/workflow.log` as the orchestrator's master record.

## Data & provenance

Observed: `datasets/raw/dataset.csv`, 149,387 real Peruvian Constitutional Court case
records with 21 columns (filing date, court of origin, case type, subject matter,
ruling summary, ruling, department/province/district). Sources are listed in
`datasets/raw/official_sources.txt`. Derived: the scraper-augmented rows that extend the
corpus toward 1M+, the merged `datasets/processed/processed_records.csv`, and all
engineered features (text length, word count, year). Anonymization replaces detected
8-digit DNI patterns before any text is retained downstream.

## Limitations

- **The per-record cost is a simulated `time.Sleep(1ms)`, not real work.** Regex
  substitution on one field takes microseconds; the sleep dominates by three orders of
  magnitude. A sleeping goroutine holds no CPU, so the pool scales past the core count and
  posts efficiencies above 100%. These numbers characterize how well Go overlaps blocking
  I/O-shaped latency — they do not predict speedup for a CPU-bound rewrite, which would
  flatten at the physical core count.
- **The sequential and concurrent harnesses process different volumes** (20,000 records
  vs the entire corpus) and the concurrent CSV records no row count at all, so no
  end-to-end speedup can be derived from the committed data. The two harnesses need a
  common `-limit` before a single speedup figure is defensible.
- **The 8.05x / 50.44 s / 6.27 s figures previously quoted here come from a course report
  PDF that is not in this repository and do not match the committed CSVs.** They have been
  removed in favour of the measured tables above.
- **Anonymization is a single regex for one identifier type.** An 8-digit match is
  presumed to be a DNI, so 8-digit case numbers, amounts, and dates are redacted as false
  positives, while names, addresses, RUC numbers, and DNIs written with separators pass
  through untouched. This is not sufficient anonymization for releasing the corpus.
- **No semantic search exists yet, despite the repository name.** There is no embedding
  model, no index, and no retrieval evaluation — the pipeline prepares and anonymizes
  text that a search layer would later consume.
- **All timings come from one unspecified machine.** No CPU model, core count, or Go
  version is recorded alongside the CSVs, so the absolute seconds are not portable; only
  the scaling ratios transfer.

## License

MIT
