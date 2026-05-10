# Week 3: dbt Silver & Gold + Data Quality

## Objectives

- Build Silver and Gold dbt models
- Add data quality tests
- Fix AER ingestion reliability

## Work Done

✅ Silver models: `aer_facilities_cleaned`, `sentinel5p_ch4_cleaned`
✅ Gold models: `facility_emissions_correlation`, `regional_ch4_hotspots`, `temporal_ch4_trends`
✅ Centralised thresholds as dbt vars in `dbt_project.yml` (QA, CH4 range, Alberta bbox, H3 resolution)
✅ 8 custom SQL tests + 2 schema.yml files covering Silver and Gold layers
✅ Fixed AER ingestion: added `source_file` dedup guard and global `row_id` offset

## Architecture Note

dbt writes all outputs into `emissions_ghg.duckdb` — no files in `data/silver/` or `data/gold/`.
Bronze is a time-series table (one row per facility per month). Silver deduplicates to one row per facility (latest month) for spatial joining.
H3 resolution 6 (~36 km²) is the join key between satellite pixels and AER facilities.

## Key Learnings

**DuckDB version alignment**: CLI, Python package, and dbt adapter must all use the same DuckDB version — mismatches cause silent failures or crashes.

**H3 extension**: must be installed and loaded explicitly per session.
```sql
INSTALL h3 FROM community;
```
In dbt, extensions are session-scoped → load via pre-hook in `dbt_project.yml`:
```yaml
+pre-hook: "LOAD h3"
```

## dbt Test Semantics

Custom SQL tests **pass when they return 0 rows** — the query selects violations.
All thresholds in tests reference `{{ var(...) }}` from `dbt_project.yml`, not hardcoded values.

## Next Steps

- Airflow DAG to orchestrate Bronze ingestion → `dbt run`
- Expand temporal coverage beyond July 2025
- Gold layer documentation and architecture diagram
