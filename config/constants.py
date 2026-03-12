from pathlib import Path

# Sentinel-5P quality filters
SENTINEL5P_QA_THRESHOLD_BRONZE = 0.1   # Bronze: permissive (exploration)
SENTINEL5P_QA_THRESHOLD_SILVER = 0.5   # Silver: high quality (analysis)
SENTINEL5P_QA_THRESHOLD_GOLD = 0.7     # Gold: very high quality (reporting)

# CH4 valid range (ppb)
CH4_MIN_VALID = 1700.0
CH4_MAX_VALID = 2100.0


# Alberta bounding box
ALBERTA_BBOX = {
    "min_lon": -120.0,
    "min_lat": 49.0,
    "max_lon": -110.0,
    "max_lat": 60.0
}

# Spatial analysis parameters
FACILITY_BUFFER_DISTANCE_M = 10000  # 10km buffer around facilities
H3_RESOLUTION = 6                   # H3 resolution (~36km²)
GRID_RESOLUTION_DEGREES = 0.1       # Fallback grid (if h3 unavailable)


# Preferred data collection period (best QA)
PREFERRED_START_MONTH = 6   # June
PREFERRED_END_MONTH = 8     # August

# Data retention
DATA_RETENTION_DAYS = 365


# Base directories
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

# Data subdirectories
RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DATA_DIR = DATA_DIR / "bronze"
SENTINEL5P_RAW_DIR = RAW_DATA_DIR / "sentinel5p"

# Output subdirectories
VISUALIZATIONS_DIR = OUTPUT_DIR / "visualizations"
REPORTS_DIR = OUTPUT_DIR / "reports"

# DuckDB settings
DUCKDB_MEMORY_LIMIT = "4GB"
DUCKDB_THREADS = 4

# Table names
TABLE_AER_FACILITIES = "bronze.aer_battery_monthly"
TABLE_SENTINEL5P_RAW = "bronze.sentinel5p_raw"
TABLE_SENTINEL5P_CLEANED = "silver.sentinel5p_ch4_cleaned"
TABLE_CH4_HOTSPOTS = "gold.regional_ch4_hotspots"


# Copernicus CDSE
COPERNICUS_TOKEN_EXPIRY_MINUTES = 9  # Safety margin
COPERNICUS_MAX_PRODUCTS_PER_QUERY = 50
COPERNICUS_PRODUCT_TYPE = "L2__CH4___"

# Minimum pixels per grid cell for aggregation
MIN_PIXELS_PER_CELL = 5


# Expected data ranges for validation tests
EXPECTED_AVG_QA_MIN = 0.3
EXPECTED_AVG_CH4_MIN = 1750.0
EXPECTED_AVG_CH4_MAX = 1950.0
MIN_ROWS_FOR_ANALYSIS = 1000
