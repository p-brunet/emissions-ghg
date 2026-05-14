# Architecture — GHG Emissions Pipeline

## Stack

| Component | Tool | Version |
|-----------|------|---------|
| Orchestration | Apache Airflow (`0 0 7 * *` — 7th of each month) | 2.9.3 |
| Query engine | DuckDB | 1.5.0 |
| Transformations | dbt-duckdb | 1.8.2 |
| Table format | Apache Iceberg — bronze satellite layer | PyIceberg 0.9.0 |
| Object storage | MinIO (S3-compatible) | — |
| Spatial indexing | H3 resolution 6 (~36 km² cells) | — |
| Infrastructure | Docker Compose | — |

---

## Data Flow

```
Copernicus CDSE             AER (Alberta Energy Regulator)
Sentinel-5P L2 NetCDF       ST60_YYYY-MM.csv
        │                           │
        ▼                           ▼
  download + process            load CSV
  NetCDF → PyArrow               CSV → DuckDB
        │                           │
        ▼                           │
  Iceberg append (MinIO)            │
  s3://ghg-warehouse/               │
  bronze/sentinel5p_raw/            │
        │                           │
  DuckDB VIEW over iceberg_scan()   │
        │                           │
        └──────────┬────────────────┘
                   ▼
              BRONZE
   sentinel5p_raw (VIEW → Iceberg)   aer_battery_monthly (DuckDB table)
   1.4M rows, 17 months              ~8,400 facilities × 14 months
   partitioned by measurement_month  ingestion_log (DuckDB table)
                   │
                dbt run
                   │
                   ▼
              SILVER
   sentinel5p_ch4_cleaned     QA ≥ 0.5, H3 assignment, incremental
   aer_facilities_cleaned     latest location per facility (dimension)
   aer_facilities_monthly     all months per facility (fact)
                   │
                dbt run
                   │
                   ▼
              GOLD
   regional_ch4_hotspots          h3_cell aggregates (all-time)
   temporal_ch4_trends            daily CH4 per h3_cell
   facility_emissions_correlation facility × satellite (all-time)
   monthly_emissions_correlation  facility × month — 116K rows, 73K with satellite
   facility_anomaly_flags         340 inconsistency flags across 3 types
```

---

## Spatial Join

Sentinel-5P pixels (~7×7 km) and AER facility GPS points are both mapped to **H3 resolution 6 cells (~36 km²)**. Two records share the same `h3_cell` if they fall in the same grid cell.

---

## Iceberg

`bronze.sentinel5p_raw` is stored as an Apache Iceberg table on MinIO, partitioned by `measurement_month`. The DuckDB file holds only a VIEW over `iceberg_scan()` — silver and gold dbt models read from it transparently.

`bronze.aer_battery_monthly` stays as a plain DuckDB table (small dataset, no benefit from Iceberg overhead).

```
s3://ghg-warehouse/
└── bronze.db/
    └── sentinel5p_raw/
        ├── data/
        │   ├── measurement_month=2025-01/*.parquet
        │   ├── measurement_month=2025-02/*.parquet
        │   └── ...  (17 partitions)
        └── metadata/
            ├── *.metadata.json   ← snapshot history (time travel)
            └── snap-*.avro       ← manifest files
```

Each `table.append()` call creates an atomic Iceberg snapshot. Snapshots are immutable — a failed load can be rolled back without touching already-committed partitions. The Iceberg catalog is SQLite-backed (`warehouse/iceberg_catalog.db`), session-local, and recreated by the `init_bronze_schema` Airflow task on container restart.

---

## Airflow DAG

```
init_bronze_schema   ← registers Iceberg catalog, creates DuckDB VIEW
    ├── load_aer_bronze
    └── download_sentinel5p
            └── process_netcdf_to_bronze
                    └── load_sentinel5p_to_bronze   ← PyIceberg append to MinIO
                                │
                         dbt_run_silver_gold
                                │
                            dbt_test
```

`init_bronze_schema` gates both branches — it re-registers the SQLite Iceberg catalog and ensures `bronze.sentinel5p_raw` VIEW exists before any downstream task reads from it.

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
