# Week 0: Project Setup & Data Availability

---

## Objectives

1. Validate data source availability
2. Setup development environment
3. Initialize GitHub repository

---

## Data Availability Results

### Sentinel-5P TROPOMI

**Source**: Copernicus Data Space Ecosystem  
**Results**:
- ✅ Authentication working (OAuth2 via CDSE API)
- ✅ 5 CH4 products found (10-day test period)
- ✅ Product type: `L2__CH4___` (methane column)
- ✅ Resolution: 7x7 km
- ✅ Coverage: Alberta confirmed

**API Access**:
```python
# OAuth2 token endpoint
https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token

# Product catalogue
https://catalogue.dataspace.copernicus.eu/odata/v1/Products
```

### Alberta Energy Regulator Facilities

**Source**: AER Statistical Reports ST60A  
**Results**:
- ✅ 2024 monthly facility data downloaded
- ✅ Format: Excel with lat/lon coordinates
- ✅ File: `data/raw/aer_facilities_2024.xlsx`
- ✅ Coverage: Alberta O&G installations

---

## Environment Setup

**Python**: 3.12 + virtual environment  
**Dependencies**: 
```txt
requests==2.31.0
python-dotenv==1.0.1
```

**Credentials**: `.env` file (gitignored)
```bash
COPERNICUS_USERNAME=your_email@example.com
COPERNICUS_PASSWORD=your_password
```

**Project Structure**:
```
emissions-ghg/
├── data/raw/              # AER data
├── docs/                  # Documentation
├── scripts/               # test_data_availability.py
├── .env                   # Credentials
├── requirements.txt
└── README.md
```

---

## Testing Script

**File**: `scripts/test_data_availability.py` (100 lines)

**Tests**:
1. Copernicus OAuth2 authentication
2. Sentinel-5P product search (Alberta bbox)


**Output**:
```
✅ Copernicus auth successful
✅ Found 5 Sentinel-5P products
✅ AER data accessible (checked manually)
```

---


## Deliverables

- [x] `scripts/test_data_availability.py`
- [x] `docs/week00.md`
- [x] `.env` with Copernicus credentials
- [x] `requirements.txt`
- [x] GitHub repo initialized
- [x] `data/raw/aer_facilities_2024.xlsx`

---

## Next Steps (Week 1)

**Prerequisites**:
- [ ] Install Docker Desktop
- [ ] Read Wu Ch.2 (Coordinate Systems) & Ch.5 (Vector Data)

**Week 1 Goals**:
- Create `docker-compose.yml` (PostgreSQL/PostGIS, MinIO, Airflow)
- Initialize database with bronze/silver/gold schemas
- Load 1 test Sentinel-5P file
- Load AER facilities into PostgreSQL

---

**References**:
- Copernicus: https://dataspace.copernicus.eu/
- AER Reports: https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st60