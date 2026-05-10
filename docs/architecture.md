# Architecture — GHG Emissions Pipeline

## Stack

| Component | Tool |
|-----------|------|
| Orchestration | Apache Airflow (`0 0 7 * *` — 7th of each month) |
| Query engine | DuckDB |
| Transformations | dbt-duckdb |
| Table format | Apache Iceberg (target for Bronze) |
| Object storage | MinIO (S3-compatible) |
| Spatial indexing | H3 resolution 6 (~36 km² cells) |
| Infrastructure | Docker Compose |

---

## Data Flow

```
Copernicus CDSE             AER (Alberta Energy Regulator)
Sentinel-5P L2 NetCDF       ST60_YYYY-MM.csv
        │                           │
        ▼                           ▼
  download + process            load CSV
  NetCDF → Parquet → DuckDB    CSV → DuckDB
        │                           │
        └──────────┬────────────────┘
                   ▼
              BRONZE
   sentinel5p_raw          aer_battery_monthly
   ~1.2M rows, 16 months   facilities × 16 months
                   │
                dbt run
                   │
                   ▼
              SILVER
   sentinel5p_ch4_cleaned     QA ≥ 0.5, incremental
   aer_facilities_cleaned     latest month per facility (dimension)
   aer_facilities_monthly     all months per facility (fact)
                   │
                dbt run
                   │
                   ▼
              GOLD
   regional_ch4_hotspots          h3_cell aggregates (all-time)
   temporal_ch4_trends            daily CH4 per h3_cell
   facility_emissions_correlation facility × satellite (all-time)
   monthly_emissions_correlation  facility × month, aligned AER + satellite
   facility_anomaly_flags         inconsistency flags
```

---

## Spatial Join

Sentinel-5P pixels (~7×7 km) and AER facility GPS points are both mapped to **H3 resolution 6 cells (~36 km²)**. Two records share the same `h3_cell` if they fall in the same grid cell.

---

## Iceberg (target)

Bronze tables are currently stored in a flat DuckDB file. Target: Iceberg tables on MinIO, partitioned by month.

```
s3://ghg-warehouse/bronze/
├── sentinel5p_raw/measurement_month=2025-01/
├── sentinel5p_raw/measurement_month=2025-02/
├── aer_battery_monthly/reporting_month=2025-01/
└── ...
```

Each monthly ingestion becomes an atomic Iceberg snapshot. Silver/gold dbt models are unchanged — DuckDB reads Iceberg via `iceberg_scan()`.

---

## Airflow DAG

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

`max_active_runs=1` — DuckDB is single-writer.

---

## QA Thresholds (Sentinel-5P)

| Layer | Threshold |
|-------|-----------|
| Bronze | ≥ 0.1 |
| Silver | ≥ 0.5 |
| Gold (target) | ≥ 0.7 |

Winter months (Nov–Feb) have significantly fewer qualifying pixels due to clouds and low solar angle over Alberta.

---

## Anomaly Flags

| Flag | Condition |
|------|-----------|
| `HIGH_CH4_LOW_REPORTED` | satellite z-score > 1.5 AND reported emissions < P25 |
| `LOW_CH4_HIGH_REPORTED` | satellite z-score < -1.5 AND reported emissions > P75 |
| `HIGH_CH4_HIGH_REPORTED` | satellite z-score > 1.5 AND reported emissions > P75 |
