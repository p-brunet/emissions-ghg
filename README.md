# GHG Emissions Analysis Pipeline
Medallion Architecture for Methane Attribution

## Overview

This project develops a geospatial data pipeline for analyzing methane emissions over Alberta, Canada, by integrating satellite observations with regulatory facility data.
Atmospheric methane column concentrations are derived from Sentinel-5P (TROPOMI). These observations are contextualized using facility-level operational data published by the Alberta Energy Regulator (AER). The objective is not to directly measure emissions at facility level, but to compare atmospheric methane enhancements with reported gas handling activity in order to identify spatial and temporal inconsistencies.
The system follows a Bronze / Silver / Gold medallion architecture implemented using DuckDB and Apache Iceberg.

## Objectives

The project demonstrates:
- A layered medallion data architecture
- Ingestion of satellite remote sensing products (Sentinel-5P CH₄)
- Integration of regulatory oil and gas facility data
- Geospatial analytics using DuckDB spatial extensions
- Table versioning and partitioning with Apache Iceberg
- Workflow orchestration with Airflow
- Reproducible transformations with dbt
- Monitoring and operational visibility

## Data Sources

### Sentinel-5P (TROPOMI)
Source: European Space Agency
Product: Methane (CH₄) column concentration
Resolution: ~7 × 7 km
Temporal resolution: Daily
Spatial coverage: Alberta, Canada

### Alberta Energy Regulator (AER)
Source: Alberta Energy Regulator statistical reports
Content: Monthly battery-level volumetric reporting
Includes:
- Gas production
- Gas flared
- Gas vented
- Oil production
- Water production
- Operator information
- Legal land location (ATS)
Each record represents one battery facility for one reporting month.

## Architecture

### Bronze Layer
Raw structured ingest with minimal transformation:
- Sentinel-5P swath products
- AER monthly battery data
- ATS → latitude/longitude conversion
- Ingestion metadata
Stored as Iceberg tables queried through DuckDB.

### Silver Layer
Cleaned and standardized datasets:
- Unique facility dimension table
- Monthly battery fact table
- Satellite observations reprojected and quality filtered
- Pixel-level facility aggregation

### Gold Layer
Analytical outputs:
TODO

## Technology Stack

- Query Engine: DuckDB
- Table Format: Apache Iceberg
- Orchestration: Apache Airflow
- Transformations: dbt
- Storage: S3-compatible object storage
- Language: Python 3.12
- Spatial Extensions: DuckDB Spatial

## Project Structure
```
├── airflow/          # DAGs and Airflow configuration
├── dbt/              # DBT models (bronze/silver/gold)
├── docker/           # Docker Compose setup
├── scripts/          # Utility scripts
│   └── setup/
│   └── ingestion/
├── notebooks/        # Exploratory analysis
└── docs/             # Documentation
```

## Project Status

The project is under active development.
Current focus:
- Bronze ingestion pipelines
- ATS coordinate conversion
- Iceberg table initialization
Next phase:
- Satellite data ingestion
- Silver-layer normalization
- Pixel aggregation modeling

I intend to work through weekly sessions and produce small documentation after each one.

## Documentation

- [Week 0: Setup & Data Audit](docs/week0.md)
- [Week 1: Create Tables and BTY Ingestion](docs/week1.md) *(coming soon)*
- [Architecture Overview](docs/architecture.md) *(coming soon)*

## Ressources

This project follows a structured curriculum integrating:
- Wu (2024) - *Introduction to GIS Programming*
- Wu (2024) - *Spatial Data Management with DuckDB*
- Wherobots - *Geospatial Data Engineering Associate* tutorial
- And whatever ressources I would find interesting useful to share along the project


## Author

**Paul** - Data Scientist Engineer, specialised in Energy and Geo data
