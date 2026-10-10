# Collected data

This branch holds the data collected by the pipeline on the `main` branch. It contains no code.

A scheduled GitHub Actions workflow adds new files here every 15 minutes.

- `raw/<source>/dt=YYYY-MM-DD/hour=HH/` holds snapshots exactly as received from each source, gzipped. Dates and hours are in UTC.
- `run_log/dt=YYYY-MM-DD/runs.jsonl` holds one line per collection run: what was written, skipped, or failed.

See the README on `main` for how the pipeline works.
