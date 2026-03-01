import math
import os
import re
from datetime import datetime

import duckdb
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ATS -> lat, lon


def ats_to_latlon(
    lsd, section, township, range_num, direction, meridian
) -> tuple[None, None] | tuple:
    """
    Convert ATS (LSD, Section, Township, Range, Meridian)
    to approximate WGS84 lat/lon.

    Accuracy: ~300–800m (sufficient for 7km Sentinel-5P).
    """

    # --- Validate inputs ---
    if not (1 <= lsd <= 16):
        return None, None
    if not (1 <= section <= 36):
        return None, None
    if not (1 <= township <= 126):
        return None, None
    if not (1 <= range_num <= 34):
        return None, None
    if meridian not in (4, 5, 6):
        return None, None
    if direction not in ("W", "E"):
        return None, None

    # --- Meridian base longitudes ---
    MERIDIAN_LON = {4: -110.0, 5: -114.0, 6: -118.0}

    base_lat = 49.0  # US border baseline
    base_lon = MERIDIAN_LON[meridian]

    # --- DLS constants ---
    MILE = 1609.344
    TOWNSHIP = 6 * MILE
    SECTION = 1 * MILE
    LSD = SECTION / 4

    # --- Township offset ---
    north_m = township * TOWNSHIP
    west_m = range_num * TOWNSHIP

    # --- Section serpentine layout ---
    sec_row = (section - 1) // 6
    sec_col = (section - 1) % 6

    if sec_row % 2 == 1:
        sec_col = 5 - sec_col

    north_m += sec_row * SECTION
    west_m += sec_col * SECTION

    # --- LSD grid ---
    lsd_row = (lsd - 1) // 4
    lsd_col = (lsd - 1) % 4

    north_m += lsd_row * LSD
    west_m += lsd_col * LSD

    # --- Convert meters → lat/lon ---
    lat = base_lat + north_m / 111320

    meters_per_degree_lon = 111320 * math.cos(math.radians(lat))

    if direction == "W":
        lon = base_lon - west_m / meters_per_degree_lon
    else:
        lon = base_lon + west_m / meters_per_degree_lon

    return round(lat, 6), round(lon, 6)


def parse_bty_to_latlon(bty_location) -> tuple[None, None] | tuple:
    """
    Convert ATS (dashed or 10-digit) to lat/lon.

    Supported formats:
      - 02-21-065-04W4
      - 0654042102

    Returns:
      (latitude, longitude) or (None, None)
    """

    if bty_location is None:
        return None, None

    value = str(bty_location).strip().upper()

    if value == "" or value == "NAN":
        return None, None

    # Try dashed format: LSD-SEC-TWP-RGE-MER
    # Example: 02-21-065-04W4
    dashed_pattern = r"^(\d{1,2})-(\d{1,2})-(\d{1,3})-(\d{1,2})([WE])(\d)$"
    match = re.match(dashed_pattern, value)

    if match:
        lsd = int(match.group(1))
        section = int(match.group(2))
        township = int(match.group(3))
        range_num = int(match.group(4))
        direction = match.group(5)
        meridian = int(match.group(6))

        return ats_to_latlon(lsd, section, township, range_num, direction, meridian)

    # Try 10-digit compact format
    # Format: MM TT RR SS LL
    # Example: 0654042102
    compact_pattern = r"^(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$"
    match = re.match(compact_pattern, value)

    if match:
        meridian = int(match.group(1))
        township = int(match.group(2))
        range_num = int(match.group(3))
        section = int(match.group(4))
        lsd = int(match.group(5))

        # Compact format does NOT encode direction.
        # Alberta ATS is always West of meridian.
        direction = "W"

        return ats_to_latlon(lsd, section, township, range_num, direction, meridian)
    # No match
    return None, None


# Ingestion


def extract_reporting_month_from_filename(path):
    """
    Expect filename like:
    ST60_2024_06.csv
    """

    filename = os.path.basename(path)
    m = re.search(r"(\d{4})[_-](\d{2})", filename)

    if not m:
        raise ValueError("Could not extract reporting month from filename")

    year = int(m.group(1))
    month = int(m.group(2))

    return datetime(year, month, 1).date()


def load_aer_data(csv_path, db_path="./emissions_ghg.duckdb") -> None:
    print("Loading AER battery monthly dataset")
    print(f"File: {csv_path}")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    reporting_month = extract_reporting_month_from_filename(csv_path)

    df = pd.read_csv(csv_path, skiprows=1, header=[0], low_memory=False)
    df = df.iloc[1:].reset_index(drop=True)

    if "BTY LOCATION EDIT" not in df.columns:
        raise ValueError("Expected column 'BTY LOCATION EDIT' not found")

    coords = df["BTY LOCATION EDIT"].apply(parse_bty_to_latlon)
    df["latitude"] = coords.apply(lambda x: x[0])
    df["longitude"] = coords.apply(lambda x: x[1])

    df = df.dropna(subset=["latitude", "longitude"]).copy()

    df["reporting_month"] = reporting_month
    df["ingestion_date"] = datetime.now().date()
    df["source_file"] = os.path.basename(csv_path)

    df["row_id"] = range(1, len(df) + 1)

    # Rename columns to normalized schema
    df = df.rename(
        columns={
            "BATTERY": "facility_id",
            "OPERATOR": "operator",
            "BTY": "facility_type",
            "BTY.1": "facility_description",
            "GAS PROD": "gas_prod_1000m3",
            "GAS FLARED": "gas_flared_1000m3",
            "GAS VENTED": "gas_vented_1000m3",
            "OIL PROD": "oil_prod_m3",
            "WTR PROD": "water_prod_m3",
            "TOTAL": "total_wells",
            "BTY LOCATION EDIT": "bty_location_raw",
            "LICENCE": "licence",
        }
    )

    numeric_cols = {
        "gas_prod_1000m3": "float",
        "gas_flared_1000m3": "float",
        "gas_vented_1000m3": "float",
        "oil_prod_m3": "float",
        "water_prod_m3": "float",
        "total_wells": "Int64",  # Nullable integer
    }

    for col, dtype in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if dtype == "Int64":
                df[col] = df[col].astype("Int64")

    # Ensure lat/lon are float
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    print("\n=== Converting 'str' dtype to 'object' for DuckDB compatibility ===")
    for col in df.columns:
        if df[col].dtype == "str":
            df[col] = df[col].astype("object")
            print(f"  - Converted {col}: str to object")

    # Verify
    print("\n=== Final dtypes (should be no 'str') ===")
    print(df.dtypes)
    con = duckdb.connect(db_path)
    con.execute("LOAD spatial;")

    con.register("df_aer", df)

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.aer_battery_monthly AS
        SELECT * FROM df_aer WHERE 1=0
    """)

    con.execute("""
        INSERT INTO bronze.aer_battery_monthly
        SELECT
            row_id,
            facility_id,
            facility_type,
            licence,
            operator,
            facility_description,
            reporting_month,
            ingestion_date,
            source_file,
            bty_location_raw,
            latitude,
            longitude,
            ST_Point(longitude, latitude) AS location,
            oil_prod_m3,
            gas_prod_1000m3,
            gas_flared_1000m3,
            gas_vented_1000m3,
            water_prod_m3,
            total_wells
        FROM df_aer
    """)

    count = con.execute(
        """
        SELECT COUNT(*) FROM bronze.aer_battery_monthly
        WHERE reporting_month = ?
    """,
        [reporting_month],
    ).fetchone()[0]

    print(f"Inserted {count} records for {reporting_month}")

    con.close()
    return


if __name__ == "__main__":
    # Path to your AER CSV file
    csv_path = "./data/raw/ST60_2024-01.csv"

    try:
        success = load_aer_data(csv_path)
        exit(0 if success else 1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
