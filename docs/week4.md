# Week 4: Airflow Orchestration, Pipeline Hardening & Full Catchup

## Objectives

- Deploy Airflow DAG end-to-end: Bronze ingestion → dbt Silver/Gold → tests
- Run full historical catchup from January 2025
- Fix all pipeline failures encountered during catchup
- Avoid redundant reprocessing as data accumulates

---

## What was done

Airflow DAG deployed and running in Docker with a full catchup from January 2025 to April 2026 — 16 months of Sentinel-5P data and monthly AER reports.

On the dbt side, all Silver and Gold models were converted to incremental materialisation. The pipeline was also exposing a fundamental data modelling gap: `aer_facilities_cleaned` collapsed all historical AER records to the latest month per facility, making any temporal analysis impossible. A new `aer_facilities_monthly` model was added to preserve the full time-series. Two new Gold models followed: `monthly_emissions_correlation` (AER volumes aligned month-by-month with satellite aggregates) and `facility_anomaly_flags` (z-score based inconsistency detection).

Architecture documented in `docs/architecture.md`.

---

## Pipeline

```
load_aer_bronze ──────────────────────────┐
                                          │
download_sentinel5p                       │
    └── process_netcdf_to_bronze          │
            └── load_sentinel5p_to_bronze ┘
                        │
                 dbt_run_silver_gold
                        │
                    dbt_test
```

Schedule: `0 0 7 * *` — runs on the 7th of month N to process month N-1.
`max_active_runs=1` — DuckDB does not support concurrent writes.

---

## Issues & Fixes

### Bronze schema not initialised on fresh Docker start

The DAG has no init task. On a clean start (after deleting `emissions_ghg.duckdb`), `load_aer_bronze` immediately fails with `Catalog Error: schema "bronze" does not exist`.

Workaround: run setup scripts manually before the first DAG trigger.
```bash
docker exec airflow-scheduler python /opt/airflow/project/scripts/setup/init_iceberg_catalog.py
docker exec airflow-scheduler python /opt/airflow/project/scripts/setup/create_bronze_tables.py
```

---

### duckdb_pool lost after Airflow reset

The pool is stored in PostgreSQL metadata. Deleting the `postgres-data` volume wipes it. Recreate after every reset:
```bash
docker exec airflow-webserver airflow pools set duckdb_pool 1 "DuckDB single-connection pool"
```

---

### Downloads stalling indefinitely

`requests.get(..., stream=True, timeout=300)` only covers the TCP connection, not the data stream. A slow file would block the task forever. Fixed with a tuple timeout and a retry loop that deletes the partial file on failure:
```python
response = requests.get(url, headers=headers, stream=True, timeout=(30, 120))
```
3 attempts, exponential backoff (10s, 20s). Catches `Timeout`, `ConnectionError`, and `ChunkedEncodingError` (mid-stream disconnect).

---

### 401 cascade after a long download

The Copernicus token expires after 9 minutes. A large file that takes longer than that silently invalidates the cached token — every subsequent file in the same task then fails with 401. Fixed by resetting the token on 401 to force re-authentication on the next attempt.

---

### First days of each month missing

The search API returns results ordered newest-first with a cap of 50. For months with 60-80 qualifying orbits over Alberta, the earliest days were silently dropped. Fixed by raising `max_results` to 500.

---

### NRTI files downloaded unnecessarily

The filter `contains(Name,'L2__CH4___')` matched both OFFL (offline, ~100 min/orbit) and NRTI (near real-time, ~5 min/file) products. NRTI files have a different HDF group structure — they all fail parsing and are never loaded. Fixed by restricting to OFFL:
```python
"contains(Name,'OFFL_L2__CH4___')"
```

---

### `process_netcdf_to_bronze` reprocessing all history every run

`process_all()` scanned the full `data/raw/sentinel5p/` directory each time. By the 16th monthly run it re-read and re-converted all 16 months of files. Fixed by checking which filenames are already in `bronze.sentinel5p_raw` and skipping them.

---

### dbt full-refresh on every run

All models were `materialized='table'`. Every `dbt run` rebuilt everything from scratch as data grew. Converted the high-volume models to incremental, filtering on the max timestamp or month already present in the target table. `regional_ch4_hotspots` and `facility_emissions_correlation` stay full-refresh since they compute all-time aggregates.

---

### Schedule triggering too early

`@monthly` triggered on the 1st. Sentinel-5P OFFL products have a ~5-day ESA processing delay — a run on February 1st finds almost no February data.

Changed to `0 0 7 * *` with `start_date=datetime(2025, 1, 7)`. The task code extracts `year/month` from `data_interval_start`, which for a run on February 7th points to January 7th — so the download range correctly covers January 1–31.

Also replaced deprecated `context["execution_date"]` with `context["data_interval_start"]`.

---

### Corrupted .nc files from interrupted downloads

Files interrupted before the timeout fix were partially written on disk. `os.path.exists()` treated them as complete, so they were skipped on retry and raised `NetCDF: HDF error` during processing. Fixed with a scan that opens each file with xarray and deletes any that fail.

---

## Data after full catchup

| Month | Pixels |
|-------|--------|
| 2025-01 | 18,781 |
| 2025-02 | 100,277 |
| 2025-03 | 106,325 |
| 2025-04 | 73,052 |
| 2025-05 | 112,536 |
| 2025-06 | 59,305 |
| 2025-07 | 88,462 |
| 2025-08 | 145,044 |
| 2025-09 | 190,370 |
| 2025-10 | 118,722 |
| 2025-11 | 42,922 |
| 2025-12 | 1,410 |
| 2026-01 | 26,396 |
| 2026-02 | 66,503 |
| 2026-03 | 83,511 |
| 2026-04 | 100,644 |

~1.2M satellite pixels total. AER: 16 monthly snapshots per active facility.

Winter months (November–February) are largely unusable — QA drops below 0.5 for most pixels due to cloud cover, snow and low solar elevation over Alberta. December 2025 has 135x fewer usable pixels than September 2025.

---

## Key Learnings

Docker volume reset wipes all Airflow state. `duckdb_pool` and any admin setup must be recreated manually after every `docker volume rm postgres-data`.

A `requests` streaming timeout is not a total download timeout. Always use `timeout=(connect, read)`.

The Silver dimension model (`aer_facilities_cleaned`) collapsing all AER history to one row per facility looked fine for spatial joins but was silently breaking any temporal analysis. Worth checking this kind of collapse early when building models for time-series use cases.

---

## Next Steps

- Add `init_bronze_schema` as first task in the DAG to remove the manual setup dependency
- Run `dbt run --full-refresh` to rebuild all models on the full 16-month dataset
- First pass on `facility_anomaly_flags` results — which facilities show the strongest inconsistencies?
- Visualise: monthly CH4 vs AER flaring for top flagged facilities
- Bronze migration to Iceberg on MinIO (see `docs/architecture.md`)
