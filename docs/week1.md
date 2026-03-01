# Week 1: Initial Tables & AER Facility Ingestion

---

## Objectives
- Set up docker-compose.yml for local services (MinIO, Airflow)
- Initialize database schemas (Bronze / Silver / Gold) with Iceberg
- Create initial Bronze tables
- Load AER facility data into DuckDB

## Work Done
✅ Created docker-compose.yml (MinIO only; Airflow postponed)
✅ Initialized DuckDB and Iceberg catalog and Bronze schema
✅ Created Bronze tables: aer_facility table included
✅ Ingested AER facility data into aer_facility table
⚠️ Sentinel-5P ingestion still pending (test file not yet loaded)

## Notes / Issues

- DuckDB 0.10.0 has segfault issues on macOS (some DESCRIBE queries)
- Workaround: Avoid introspection queries, use simple SELECT COUNT(*)

## Next Steps (Week 2)
Finish Airflow setup and test DAGs
Load first Sentinel-5P test file into Bronze layer
Start Silver-layer normalization (facilities + satellite pixels)
