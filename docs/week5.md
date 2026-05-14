# Week 5: Iceberg Migration, Gold Exploitation & Project Conclusion

## Objectives

- Migrate `bronze.sentinel5p_raw` from flat DuckDB table to Iceberg on MinIO
- Run gold models on the full 16-month dataset and inspect anomaly flags
- Extend visualizations to cover the new gold layer
- Conclude the project and document its limits

---

## What was done

### Iceberg migration

`bronze.sentinel5p_raw` is now an Apache Iceberg table on MinIO, partitioned by `measurement_month`. The DuckDB file no longer contains the 1.2M-row table — it holds a VIEW over `iceberg_scan()` instead. dbt models and visualization scripts are unchanged.

Key changes:
- `pyiceberg[s3fs]` added to requirements
- `init_iceberg_catalog.py`: now creates a `SqlCatalog` (SQLite metadata) pointing to `s3://ghg-warehouse/bronze/sentinel5p_raw/`
- `create_bronze_tables.py`: on first run, migrates existing DuckDB rows to Iceberg in monthly batches, then drops the table and creates the view
- `load_sentinel5p_to_bronze.py`: writes new data via `table.append(pyarrow_table)` instead of DuckDB INSERT
- `bronze.ingestion_log`: new DuckDB table tracking loaded filenames — decouples file-skip logic from the Iceberg table, avoiding S3 queries in `process_netcdf_to_bronze.py`
- `profiles.yml`: iceberg extension + S3 credentials added so dbt sessions can query the view

One operational note: the SQLite catalog file lives at `./warehouse/iceberg_catalog.db`. In Docker, this directory must be mounted as a volume or the catalog is recreated on container restart. The Iceberg files on MinIO are unaffected — only the catalog registration is lost. `init_bronze_schema` (first Airflow task) recreates the catalog entry if it's missing.

### Airflow DAG: init task added

`init_bronze_schema` now runs first and gates both `load_aer_bronze` and `download_sentinel5p`. No more manual `docker exec` after a fresh container start.

```
init_bronze_schema
    ├── load_aer_bronze
    └── download_sentinel5p → process_netcdf_to_bronze → load_sentinel5p_to_bronze
                                                                    │
                                                           dbt_run_silver_gold → dbt_test
```

### Gold models validated

First full run on the 16-month dataset:

```bash
dbt run --no-partial-parse --full-refresh \
  --select monthly_emissions_correlation facility_anomaly_flags
dbt test --no-partial-parse
```

`monthly_emissions_correlation` produces 116,717 rows (all facility × month combinations from AER), of which 73,908 have matching satellite data. December 2025 has 0 satellite rows (winter blackout). January and November have very sparse coverage.

`facility_anomaly_flags` flags **340 facility-month pairs**. Distribution:
- `HIGH_CH4_HIGH_REPORTED`: 155 (satellite elevated AND high declared volumes — consistent elevated activity)
- `HIGH_CH4_LOW_REPORTED`: 146 (satellite elevated, declared volumes below facility P25 — most interesting for unreported emission detection)
- `LOW_CH4_HIGH_REPORTED`: 39 (declared volumes high, satellite quiet — possible over-reporting or attribution error)

### Visualizations

Three new plots added to `scripts/visualization/visualize_ch4_data.py`:
- **Plot 4** (`monthly_ch4_vs_aer.png`): monthly CH4 time series vs AER emissions for the 5 most-observed h3_cells. Dual y-axis.
- **Plot 5** (`emissions_scatter.png`): scatter of reported emissions vs satellite CH4, colored by z-score. The correlation is weak across all facilities combined — expected at H3 resolution 6.
- **Plot 6** (`anomaly_flags.png`): stacked horizontal bars, top 15 facilities by flag count, colored by flag type.

---

## Implementation bugs encountered and fixed

These are the runtime issues hit during the actual deployment, in order of occurrence.

**1. `SqlCatalog` has no `namespace_exists()` method (pyiceberg 0.9.0)**
`init_iceberg_catalog.py` called `catalog.namespace_exists()` which doesn't exist on `SqlCatalog`. Fixed by replacing the guard with a `try/except NamespaceAlreadyExistsError`.

**2. PyArrow S3 client uses subdomain-style access by default**
`iceberg_scan()` was trying to reach `ghg-warehouse.localhost:9000` instead of `localhost:9000/ghg-warehouse`. Fixed by adding `"s3.path-style-access": "true"` to the `SqlCatalog` properties.

**3. SQLAlchemy version conflict between pyiceberg and Airflow 2.9**
`pyiceberg[s3fs]==0.9.0` requires SQLAlchemy ≥ 2.0 (`DeclarativeBase`). Airflow 2.9.3 requires SQLAlchemy 1.4.x. Installing both in the same Python environment crashes Airflow at startup. Fixed by creating a separate venv `/home/airflow/pyiceberg-env` in the Dockerfile and calling all pyiceberg scripts via `subprocess.run([PYICEBERG_PYTHON, ...])` so pyiceberg never runs inside the Airflow process.

**4. Docker venv at `/opt/pyiceberg-env` — permission denied**
The `airflow` user has no write access to `/opt`. Moved the venv to `/home/airflow/pyiceberg-env`.

**5. DuckDB extensions not installed in Docker**
`spatial`, `httpfs`, `iceberg`, `aws` are not bundled in the Docker image. Added a `RUN python -c "import duckdb; con.execute('INSTALL spatial; ...')"` step in `Dockerfile.airflow` to pre-download them at build time.

**6. `MINIO_ENDPOINT=localhost:9000` unreachable from inside Docker**
From inside a container, `localhost` refers to the container itself. MinIO is reachable as `minio:9000` on the Docker network. Added `MINIO_ENDPOINT: minio:9000` to the `environment` block of `x-airflow-common` in `docker-compose.yml` to override the `.env` value.

**7. `iceberg_scan()` fails on empty table — version guessing disabled**
DuckDB refuses to scan an Iceberg table with no `version-hint.text` file unless `SET unsafe_enable_version_guessing = true` is set. Added this SET before creating the DuckDB VIEW in `create_bronze_tables.py`.

**8. `information_schema.tables.table_type` returns `"BASE TABLE"` not `"TABLE"`**
The migration guard `if result[0] == "TABLE"` never matched, causing `CREATE OR REPLACE VIEW` to fail on an existing table. Fixed by checking `"BASE TABLE"`.

**9. `duckdb.execute().arrow()` returns `RecordBatchReader`, not `pyarrow.Table`**
`pyiceberg.table.append()` expects a `pyarrow.Table`. Added `.read_all()` call when the result is a `RecordBatchReader`.

**10. Timestamp timezone mismatch (`timestamptz` vs `timestamp`)**
DuckDB returns timezone-aware timestamps; the Iceberg schema uses `TimestampType()` (naive). Fixed by casting both `measurement_timestamp` and `ingestion_timestamp` with `::TIMESTAMP` in the migration SQL.

**11. `ON CONFLICT DO NOTHING` requires a UNIQUE constraint**
`ingestion_log` has no primary key, so the conflict clause was rejected. Replaced with `AND file_path NOT IN (SELECT file_path FROM bronze.ingestion_log)`.

**12. Migration not idempotent — double-write risk on re-run after failure**
If the script fails after writing to Iceberg but before dropping the DuckDB table, re-running migrates again and duplicates data. Fixed by checking `iceberg_table.current_snapshot() is None` before calling `_migrate_to_iceberg`.

**13. Silver layer under-populated after incremental-only runs**
`sentinel5p_ch4_cleaned` is materialized as `incremental` with filter `measurement_timestamp > MAX(measurement_timestamp)`. Each `dbt run` only processes rows newer than the current silver maximum. After the initial Iceberg migration, bronze held 17 months of data but silver only contained the months corresponding to each manual pipeline trigger (5 months out of 17). The gold models and visualizations showed only those 5 months.

Workaround: run silver with `--full-refresh` after any bulk bronze load to reprocess the entire Iceberg table:

```bash
dbt run --no-partial-parse --full-refresh --select sentinel5p_ch4_cleaned
dbt run --no-partial-parse --full-refresh --select monthly_emissions_correlation facility_anomaly_flags
```

After full-refresh, silver went from 112K to 707K rows covering 16 months. This is expected behavior for incremental models — `--full-refresh` is the intended recovery path after a bulk ingest.

**14. Gold model coverage bounded by AER data, not satellite data**
`monthly_emissions_correlation` drives its row generation from AER facilities (`FROM aer_facilities_monthly AS f LEFT JOIN satellite...`). Months where satellite data exists but AER data does not (March–May 2026 in the current dataset) produce no gold rows. This is by design: without a facility declaration there is nothing to correlate against. The workaround is to ingest a fresh AER export covering those months.

**16. Working directory mismatch causes scripts to connect to the wrong DuckDB file**
Two DuckDB files coexist in the repo: `./emissions_ghg.duckdb` (72 MB, all data) at the project root and `dbt_emissions_ghg/emissions_ghg.duckdb` (274 KB, empty) left over from before profiles.yml was updated to `path: ../emissions_ghg.duckdb`. Any script using a relative path (`DUCKDB_DATABASE_PATH=./emissions_ghg.duckdb`) will connect to the empty file if the working directory is `dbt_emissions_ghg/`. This caused false "0 tables" results after a `cd dbt_emissions_ghg` in the shell session. Fix: always run visualization scripts and one-off Python queries from the project root, never from inside `dbt_emissions_ghg/`. The empty file can be deleted once confirmed obsolete.

**15. December 2025 absent from gold — satellite blackout**
Bronze has only 1,410 pixels for December 2025 (vs ~100K for adjacent months). Nearly all fail QA ≥ 0.5 (polar winter: cloud cover, snow albedo, low solar elevation angle reduce TROPOMI retrieval quality). Silver has effectively 0 usable pixels for that month; gold has no satellite column for December. No fix — this is a physical constraint of the sensor.

---

## Limitations

**H3 resolution 6 is too coarse for facility attribution.** Each cell covers ~36 km², often containing several independent facilities and their cumulative emissions. A satellite enhancement in a cell cannot be attributed to one operator without a finer grid (resolution 7 = ~5 km², resolution 8 = ~0.7 km²) or wind-based dispersion modelling.

**AER data is self-reported, not measured.** Operators declare flaring and venting volumes. The `HIGH_CH4_LOW_REPORTED` flag identifies cells where satellite CH4 is anomalously high relative to what was reported — but it cannot distinguish unreported emissions from a neighbouring industrial source, agricultural methane, or wetlands.

**Winter data gaps break the time series.** November, December, January, and February have 3–10× fewer usable pixels than summer months (QA < 0.5 due to cloud cover, snow, and low solar elevation). Monthly averages for those periods are unreliable and the z-score baseline is skewed by the summer-heavy distribution.

**Iceberg catalog is session-local in Docker.** The SQLite catalog at `./warehouse/iceberg_catalog.db` is not persisted by default across container restarts. The Airflow `init_bronze_schema` task handles re-registration, but any direct Python call to `create_bronze_tables.py` outside the DAG also needs the catalog to exist first.

---

## What would come next

- **H3 resolution 7 or 8**: finer cells reduce multi-facility contamination and improve attribution confidence
- **Atmospheric baseline**: subtract a regional CH4 background (e.g., median of cells with no AER activity) before computing z-scores — currently the baseline includes all cells regardless of emission activity
- **Wind-adjusted attribution**: use ERA5 surface wind fields to back-trace satellite enhancements to likely source locations; would require adding a meteorology data source to the pipeline
- **REST catalog for Iceberg**: replace the SQLite-based SqlCatalog with a Hive Metastore or REST catalog service — eliminates the Docker volume dependency and supports multi-process writes
- **Regulatory cross-referencing**: join anomaly flags against AER enforcement records to see whether flagged facilities have prior compliance issues

---

## Key Learnings

Iceberg's value is not in the write speed — PyArrow parquet would be faster locally. It's in the guarantees: atomic snapshots (no partial reads), time-travel (`AS OF SNAPSHOT`), partition pruning on scan, and format portability (DuckDB, Spark, and Trino can all read the same files). On a 1.2M-row dataset the overhead is invisible. On 100M rows it pays for itself in query pruning.

The DuckDB VIEW over `iceberg_scan()` is transparent to dbt. Silver and gold models read from `bronze.sentinel5p_raw` exactly as before — the only change is that the data now lives on MinIO instead of in the local DuckDB file. This is the architecture described from week 1; week 5 is the first time it actually runs.
