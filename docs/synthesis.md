# Project Synthesis

## What this project is about

This project had two parallel goals: build a production-grade data architecture from scratch, and apply it to a concrete environmental monitoring question — detecting inconsistencies between satellite-observed methane concentrations and volumes declared by oil and gas operators to the Alberta Energy Regulator (AER).

The intent was not to prove fraud, but to demonstrate that a geospatial data pipeline built on public sources can produce actionable signals for regulatory cross-referencing.

---

## What was built

A full medallion pipeline running in Docker, orchestrated by Airflow, with dbt handling all transformations and DuckDB as the query engine:

```
Sentinel-5P (ESA)          AER Monthly Reports
17 months of CH₄ data      ~9,900 O&G facilities
        │                          │
        ▼                          ▼
   BRONZE (Iceberg/MinIO)     BRONZE (DuckDB)
   1,394,442 raw pixels       14 months of declarations
        │                          │
        └──────────┬───────────────┘
                   ▼
              SILVER (DuckDB)
              707,218 pixels after QA ≥ 0.5
              H3 resolution 6 spatial join
                   │
                   ▼
               GOLD (DuckDB)
               73,908 facility-month pairs with satellite coverage
               340 anomaly flags
```

The satellite bronze layer was migrated to Apache Iceberg on MinIO at the end of the project — raw data is now stored as partitioned Parquet files with snapshot isolation, while a DuckDB VIEW makes it transparent to all downstream models.

---

## Results

| Metric | Value |
|---|---|
| Raw satellite pixels ingested | 1,394,442 (17 months) |
| Pixels passing QA filter (≥ 0.5) | 707,218 (51%) |
| AER facilities tracked | 9,945 |
| Facility-month pairs with satellite data | 73,908 |
| Anomaly flags generated | 340 |
| — HIGH_CH4_LOW_REPORTED | 146 — satellite elevated, declared volumes below facility P25 |
| — HIGH_CH4_HIGH_REPORTED | 155 — both signals elevated (consistent activity) |
| — LOW_CH4_HIGH_REPORTED | 39 — declared volumes high, satellite quiet |

---

## What I learned

**On the data side:** the H3 resolution 6 grid (~36 km² per cell) proved too coarse for facility-level attribution. Cells contain up to 47 independent facilities. The anomaly flags identify zones worth investigating, not individual responsible operators. Going to resolution 7 (~5 km²) or 8 (~0.7 km²) would significantly improve attribution confidence.

**On the engineering side:** the Iceberg migration exposed real production constraints I hadn't anticipated — a SQLAlchemy version conflict between PyIceberg 0.9.0 and Airflow 2.9.3 requiring a fully isolated Python venv, DuckDB incremental models silently under-populating after a bulk historical load, and working directory dependencies making relative paths unreliable across a Docker/local hybrid setup. These problems taught me as much as the architecture itself.

**On the domain side:** AER data is self-reported. A `HIGH_CH4_LOW_REPORTED` flag cannot distinguish unreported emissions from a neighbouring source, agricultural methane, or wetlands. Satellite column concentrations (ppb, integrated over the full atmospheric column) are not directly comparable to surface-level volumetric declarations (1000 m³). The correlation signal at H3-6 is weak, which is expected and documented.

---

## What comes next

- **H3 resolution 7 or 8** — finer cells to reduce multi-facility contamination
- **REST Iceberg catalog** — replace the SQLite catalog with a Hive Metastore or REST service to eliminate the Docker volume dependency
- **Atmospheric baseline subtraction** — remove a regional CH₄ background before computing z-scores; currently the baseline includes all cells regardless of emission activity
- **Wind-adjusted attribution** — use ERA5 surface wind fields to back-trace satellite enhancements to likely source locations

### A different architecture for a future project

This project converts satellite data (NetCDF swaths) into tabular form early in the pipeline — pixels become rows, spatial structure is discarded in favour of H3 cell IDs. This works for monthly aggregation, but it loses information: overpass geometry, scan angle, sub-pixel variability.

A natural evolution would be a **native geospatial medallion architecture** where the bronze layer retains the data in its original array format using **Zarr** (a cloud-native N-dimensional array format analogous to what Iceberg is for tables). Silver would apply quality filters and reprojection directly on the array, and gold would only flatten to tabular form at the point of the analytical join. This approach would preserve the full spatial resolution of TROPOMI retrievals and enable pixel-level radiative transfer corrections that are impossible once the data is tabularised.

---

## Visual overview

```
┌────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                │
│  ESA Copernicus (Sentinel-5P NetCDF)  │  AER (monthly CSV)     │
└───────────────────┬─────────────────────────────┬──────────────┘
                    │                             │
                    ▼                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                                 │
│  Iceberg on MinIO                  │  DuckDB table              │
│  sentinel5p_raw                    │  aer_battery_monthly       │
│  1.4M rows · 17 months             │  9,900 facilities          │
│  partitioned by month              │  14 months                 │
│  DuckDB VIEW ← iceberg_scan()      │                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │  dbt (QA filter · H3 join)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (DuckDB)                       │
│  sentinel5p_ch4_cleaned   707K pixels · QA ≥ 0.5                │
│  aer_facilities_monthly   monthly fact table                    │
│  aer_facilities_cleaned   facility dimension                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │  dbt (spatial join · z-score)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (DuckDB)                         │
│  monthly_emissions_correlation   73K matched facility-months    │
│  facility_anomaly_flags          340 flags across 3 types       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OUTPUTS                                      │
│  6 visualizations · docs/week0–5.md · dbt tests (50)            │
└─────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow  │  Query engine: DuckDB 1.5.0
Transforms: dbt-duckdb 1.8.2   │  Infrastructure: Docker Compose
```
