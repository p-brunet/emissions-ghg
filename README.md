# GHG Emissions Analysis Pipeline - Medallion Architecture

Geospatial data engineering project for detecting and quantifying methane emissions using satellite remote sensing (Sentinel-5P) and facility data (Alberta Energy Regulator).

## 🎯 Project Goals

Build a production-ready data pipeline demonstrating:
- **Bronze/Silver/Gold medallion architecture**
- **Satellite data integration** (Sentinel-5P TROPOMI CH₄)
- **Geospatial analytics** with PostGIS
- **Workflow orchestration** with Airflow
- **Data transformations** with DBT
- **Monitoring & visualization** with Grafana

## 🛠️ Tech Stack

- **Database**: PostgreSQL 15 + PostGIS 3.4
- **Orchestration**: Apache Airflow 2.8
- **Transformations**: DBT 1.7
- **Storage**: MinIO (S3-compatible)
- **Container**: Docker Compose
- **Languages**: Python 3.12, SQL
- **Libraries**: GeoPandas, Rasterio, Xarray

## 📁 Project Structure
```
├── airflow/          # DAGs and Airflow configuration
├── dbt/              # DBT models (bronze/silver/gold)
├── docker/           # Docker Compose setup
├── scripts/          # Utility scripts
├── notebooks/        # Exploratory analysis
└── docs/             # Documentation
```

## 🚀 Quick Start

**Prerequisites**: Docker, Python 3.12+
```bash
# Clone repository
git clone https://github.com/p-brunet/emissions-ghg.git
cd emissions-ghg

# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure credentials
cp .env.example .env
# Edit .env with your Copernicus credentials

# Start services
cd docker
docker-compose up -d

# Access services
# Airflow: http://localhost:8080 (admin/admin)
# MinIO: http://localhost:9001 (minioadmin/minioadmin)
```

## 📅 Project Status

**Current Week**: Week 0 ✅ Complete

### Completed
- ✅ Week 0: Environment setup & data availability confirmation
  - Copernicus Data Space authentication working
  - 5 Sentinel-5P CH4 products identified (10-day window)
  - AER 2024 facilities data downloaded

### Next
- Week 1: Docker infrastructure (PostgreSQL/PostGIS, MinIO, Airflow)

## 📊 Data Sources

1. **Sentinel-5P TROPOMI**: CH₄ column concentrations (7x7 km resolution)
   - Source: Copernicus Open Access Hub
   - Coverage: Alberta, Canada (2023-2024)

2. **Alberta Energy Regulator**: O&G facility locations and reported emissions
   - Source: AER Statistical Reports
   - Format: CSV with lat/lon coordinates

## 📚 Documentation

- [Week 0: Setup & Data Audit](docs/week00_data_audit.md)
- [Architecture Overview](docs/architecture.md) *(coming soon)*

## 🎓 Ressources

This project follows a structured curriculum integrating:
- Wu (2024) - *Introduction to GIS Programming*
- Wherobots - *Geospatial Data Engineering Associate* tutorial
- And whatever ressources I would find interesting useful to share along the project



## 👤 Author

**Paul** - Data Scientist Engineer, specialised in Energy and Geo data
