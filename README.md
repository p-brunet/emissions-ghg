# GHG Emissions Analysis Pipeline
Medallion Architecture for Methane Attribution — Alberta, Canada

## Overview

This project builds a geospatial data pipeline that cross-references satellite methane observations with regulatory oil & gas facility data to detect atmospheric anomalies inconsistent with declared emissions.

Satellite column concentrations (Sentinel-5P / TROPOMI) are spatially joined with AER monthly battery reports using the H3 hexagonal grid (resolution 6, ~36 km² per cell). The goal is not to measure facility-level emissions directly — resolution is too coarse for that — but to flag cells where the satellite signal is statistically inconsistent with what operators declared to the regulator.

The pipeline runs end-to-end in Docker: Airflow orchestrates ingestion, dbt handles all transformations, DuckDB is the query engine, and raw satellite data lands in Apache Iceberg on MinIO.

---

## Results

- **17 months** of Sentinel-5P CH₄ data ingested (Jan 2025 → May 2026), 1,394,442 raw pixels
- **707,218 silver pixels** after QA ≥ 0.5 filter and valid CH₄ range [1700–2100 ppb]
- **9,945 AER facilities** across 14 reporting months (Jan 2025 → Feb 2026)
- **73,908 facility-month pairs** with matching satellite coverage
- **340 anomaly flags** across 3 types:
  - `HIGH_CH4_LOW_REPORTED` — 146 pairs: satellite elevated, declared volumes below facility P25
  - `HIGH_CH4_HIGH_REPORTED` — 155 pairs: both satellite and declared volumes elevated
  - `LOW_CH4_HIGH_REPORTED` — 39 pairs: declared volumes high, satellite quiet

Key finding: no strong linear correlation between reported AER volumes and satellite CH₄ at H3 resolution 6. This is expected — cells cover ~36 km² and often contain 5–47 facilities whose combined signal cannot be attributed to individual operators without wind dispersion modelling.

---

## Architecture

```
Sentinel-5P NetCDF          AER Monthly CSV
(ESA Copernicus API)        (AER Public Reports)
        │                          │
        ▼                          ▼
  Bronze Layer (Iceberg/MinIO)    Bronze Layer (DuckDB)
  sentinel5p_raw                  aer_battery_monthly
  1.4M rows, partitioned          ingestion_log
  by measurement_month
        │                          │
        └──────────┬───────────────┘
                   │  dbt (silver)
                   ▼
         Silver Layer (DuckDB)
         sentinel5p_ch4_cleaned   ← QA filter, H3 assignment
         aer_facilities_cleaned   ← dedup, geocode
         aer_facilities_monthly   ← monthly fact table
                   │
                   │  dbt (gold)
                   ▼
          Gold Layer (DuckDB)
          monthly_emissions_correlation   ← satellite × AER join per H3 cell × month
          facility_anomaly_flags          ← z-score anomaly detection
```

### Storage split

| Layer | Engine | Location |
|---|---|---|
| `bronze.sentinel5p_raw` | Apache Iceberg | MinIO `s3://ghg-warehouse/` |
| `bronze.aer_battery_monthly` | DuckDB table | `emissions_ghg.duckdb` |
| Silver + Gold | DuckDB tables | `emissions_ghg.duckdb` |

Only the satellite bronze layer uses Iceberg — it's the only dataset large enough to benefit from partition pruning and snapshot isolation.

---

## Technology Stack

| Component | Tool | Version |
|---|---|---|
| Query engine | DuckDB | 1.5.0 |
| Table format | Apache Iceberg | PyIceberg 0.9.0 |
| Object storage | MinIO | Latest |
| Orchestration | Apache Airflow | 2.9.3 |
| Transformations | dbt-duckdb | 1.8.2 |
| Spatial indexing | H3 (DuckDB extension) | Resolution 6 |
| Language | Python | 3.13 |
| Containerisation | Docker Compose | — |

---

## Airflow DAG

```
init_bronze_schema
    ├── load_aer_bronze
    └── download_sentinel5p → process_netcdf → load_sentinel5p_to_bronze
                                                          │
                                               dbt_run_silver_gold → dbt_test
```

`init_bronze_schema` runs first on every DAG trigger. It re-registers the Iceberg catalog (SQLite-backed, recreated on container restart) and ensures the DuckDB view over `iceberg_scan()` exists before any downstream task reads from bronze.

---

## Project Structure

```
├── airflow/dags/           # ghg_pipeline.py — full DAG definition
├── config/                 # constants.py — shared paths and thresholds
├── dbt_emissions_ghg/      # dbt project
│   ├── models/
│   │   ├── silver/         # sentinel5p_ch4_cleaned, aer_facilities_*
│   │   └── gold/           # monthly_emissions_correlation, facility_anomaly_flags
│   └── profiles.yml        # DuckDB connection + S3 settings
├── docker/
│   ├── docker-compose.yml  # Airflow + MinIO + Postgres
│   ├── Dockerfile.airflow  # pyiceberg venv isolation
│   └── requirements-airflow.txt
├── scripts/
│   ├── setup/              # init_iceberg_catalog.py, create_bronze_tables.py
│   ├── ingest/             # download_sentinel5p, process_netcdf, load_*
│   └── visualization/      # visualize_ch4_data.py — 6 plots
├── outputs/visualizations/ # ch4_heatmap, overlay, scatter, monthly, anomaly_flags
├── docs/
│   ├── week0.md … week5.md # Weekly implementation logs
│   └── architecture.md
├── warehouse/              # SQLite Iceberg catalog (local, not committed)
└── emissions_ghg.duckdb    # Main DuckDB file (silver + gold + AER bronze)
```

---

## Running Locally

### Prerequisites

**1. Docker Desktop** — must be running before any command below.

**2. Copernicus account** — Sentinel-5P data is downloaded automatically by the pipeline from the [Copernicus Data Space Ecosystem](https://dataspace.copernicus.eu). Create a free account, then set your credentials in `.env`:

```env
# Copernicus Data Space — https://dataspace.copernicus.eu (free registration)
COPERNICUS_USERNAME=your@email.com
COPERNICUS_PASSWORD=yourpassword

# MinIO (default credentials for local Docker stack)
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT=localhost:9000

# Iceberg catalog
ICEBERG_CATALOG_URI=sqlite:///./warehouse/iceberg_catalog.db
ICEBERG_WAREHOUSE=s3://ghg-warehouse/

# DuckDB
DUCKDB_DATABASE_PATH=./emissions_ghg.duckdb
```

**3. AER data (manual download)** — The Alberta Energy Regulator provides no API. Monthly battery reports must be downloaded manually from:

> **https://www.aer.ca/data-and-performance-reports/statistical-reports/st60**

Download the CSV files for the months you want to analyse (format: `ST60_YYYY-MM.csv`) and place them in `data/raw/`. The `load_aer_bronze` Airflow task picks them up automatically on each DAG run — if a file for the execution month is absent, the task skips without failing.

---

```bash
# Start the stack
docker compose -f docker/docker-compose.yml up -d

# Airflow UI → http://localhost:8080  (admin / admin)
# MinIO UI  → http://localhost:9001  (minioadmin / minioadmin)

# Trigger the DAG manually from the UI, or:
docker exec airflow-webserver airflow dags trigger ghg_pipeline

# Run dbt manually (full refresh after bulk ingest)
cd dbt_emissions_ghg
dbt run --no-partial-parse --full-refresh --select sentinel5p_ch4_cleaned
dbt run --no-partial-parse --full-refresh --select monthly_emissions_correlation facility_anomaly_flags

# Generate visualizations (from project root)
python scripts/visualization/visualize_ch4_data.py
```

> **Important:** always run Python scripts and dbt from the **project root**, not from inside `dbt_emissions_ghg/`. The DuckDB path is relative (`./emissions_ghg.duckdb`) and resolves differently depending on the working directory.

---

## Known Limitations

- **H3 resolution 6 (~36 km²) is too coarse for facility attribution.** Cells contain 1–47 facilities; satellite signal is a cell-level average.
- **AER data is self-reported.** `HIGH_CH4_LOW_REPORTED` flags cannot distinguish unreported emissions from neighbouring sources (wetlands, agriculture, other operators).
- **Winter satellite gaps.** November–January have 3–10× fewer usable pixels due to cloud cover and low solar elevation (QA < 0.5). Z-score baselines are summer-weighted.
- **Iceberg catalog is session-local.** The SQLite catalog at `./warehouse/iceberg_catalog.db` is recreated by Airflow on container restart. Direct Python calls outside the DAG require the catalog to exist first.

---

## Documentation

- [Week 0: Setup & Data Audit](docs/week0.md)
- [Week 1: AER Ingestion Pipeline](docs/week1.md)
- [Week 2: Sentinel-5P Ingestion & Visualization](docs/week2.md)
- [Week 3: dbt Silver Models & H3 Spatial Join](docs/week3.md)
- [Week 4: Gold Models & Anomaly Detection](docs/week4.md)
- [Week 5: Iceberg Migration, Gold Exploitation & Conclusion](docs/week5.md)

---

## Author

**Paul** — Data Engineer, specialised in Energy and Geo data
