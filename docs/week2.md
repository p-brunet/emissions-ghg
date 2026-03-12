# Week 2: Sentinel-5P Data Ingestion
---

## Objectives

- Download Sentinel-5P CH₄ NetCDF files
- Parse and load into DuckDB Bronze
- Validate data quality
- Basic visualizations



## Deliverables

### Data Loaded
- **Source**: 4 Sentinel-5P orbits (July 14-15, 2025)
- **Rows**: ~18,000 pixels
- **Table**: `bronze.sentinel5p_raw`
- **Coverage**: Alberta bbox, QA >= 0.1

### Scripts Created
```
scripts/ingest/
├── download_sentinel5p.py        # OAuth2 + multi-orbit download
├── process_netcdf_to_bronze.py   # NetCDF → Parquet
└── load_sentinel5p_to_bronze.py  # Parquet → DuckDB

scripts/tests/
└── test_sentinel5p_data.py       # Validation queries

scripts/visualization/
└── visualize_ch4_data.py         # Heatmaps + overlay
```

### Visualizations
- `outputs/visualizations/ch4_heatmap.png`
- `outputs/visualizations/ch4_facilities_overlay.png`
- `outputs/visualizations/summary_statistics.png`


**Observations:**
I could not find data with sufficient QA over Alberta in winter; I first doubted about my parsing so I had to drop nan values. - I plot some methane emissions from 2 days in july overlayed with AER facilities. \
The project itself aims to demonstrate strong engineering pipeline more than ML methane detection. Another source could have been from EMIT L2B Methane Enhancement Data 60 m. 


## Key Learnings

**Winter data unusable**: January Alberta has QA < 0.3 (possible causes clouds, snow, low sun angle - I actually tested over one file)  
**xarray bug**: `where(drop=True)` doesn't drop NaN rows -> add `df.dropna()` after `to_dataframe()`


## Data Summary

| Metric | Value |
|--------|-------|
| Pixels | 18,000 |
| Dates | July 14-15, 2025 |
| Orbits | 4 |
| Avg QA | 0.800 |
| Avg CH₄ | 1895.92 ppb |
| Overlap with facilities | 1,500 pixels (10km radius) |


## Next Steps (Week 3)

- Install dbt-duckdb
- Create Silver layer (QA >= 0.5 filter)
- Data validation tests
- Documentation
- Finish Airflow setup and test DAGs
