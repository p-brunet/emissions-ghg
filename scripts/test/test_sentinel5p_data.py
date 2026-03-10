import os

import duckdb
from dotenv import load_dotenv

load_dotenv()

ALBERTA_BBOX = {"min_lat": 49, "max_lat": 60, "min_lon": -120, "max_lon": -110}

def run_validation_tests():
    db_path = os.getenv('DUCKDB_DATABASE_PATH', './emissions_ghg.duckdb')
    con = duckdb.connect(db_path)
    con.execute("LOAD spatial;")

    print("="*60)
    print("Sentinel-5P Data Validation Tests")
    print("="*60)

    # Test 1: Row count
    total_rows = con.execute("SELECT COUNT(*) FROM bronze.sentinel5p_raw").fetchone()[0]
    print(f"\n[Test 1] Total rows: {total_rows:,}")
    if total_rows == 0:
        print("  FAILED: No data!")
        return False
    elif total_rows < 10000:
        print("  WARNING: Low row count")
    else:
        print("  PASSED")

    # Test 2: Temporal coverage
    df = con.execute("""
        SELECT MIN(measurement_timestamp) AS first_date,
               MAX(measurement_timestamp) AS last_date,
               COUNT(DISTINCT DATE(measurement_timestamp)) AS unique_dates,
               COUNT(DISTINCT orbit_number) AS unique_orbits
        FROM bronze.sentinel5p_raw
    """).fetchdf()
    unique_dates = df['unique_dates'].iloc[0]
    unique_orbits = df['unique_orbits'].iloc[0]
    print(f"\n[Test 2] Date coverage: {unique_dates} day(s), Orbit coverage: {unique_orbits}")
    print("  PASSED" if unique_orbits >= 3 else f"  WARNING: Only {unique_orbits} orbit(s)")

    # Test 3: Spatial extent
    df = con.execute("""
        SELECT ROUND(MIN(latitude),2) AS min_lat,
               ROUND(MAX(latitude),2) AS max_lat,
               ROUND(MIN(longitude),2) AS min_lon,
               ROUND(MAX(longitude),2) AS max_lon
        FROM bronze.sentinel5p_raw
    """).fetchdf()
    min_lat, max_lat = df['min_lat'].iloc[0], df['max_lat'].iloc[0]
    min_lon, max_lon = df['min_lon'].iloc[0], df['max_lon'].iloc[0]
    bbox_check = ALBERTA_BBOX['min_lat'] <= min_lat <= max_lat <= ALBERTA_BBOX['max_lat'] and \
                 ALBERTA_BBOX['min_lon'] <= min_lon <= max_lon <= ALBERTA_BBOX['max_lon']
    print(f"\n[Test 3] Spatial extent within Alberta bbox: {'PASSED' if bbox_check else 'WARNING'}")

    # Test 4: QA distribution
    df = con.execute("""
        SELECT ROUND(AVG(qa_value),3) AS avg_qa,
               SUM(CASE WHEN qa_value >= 0.5 THEN 1 ELSE 0 END) AS high_quality,
               COUNT(*) AS total
        FROM bronze.sentinel5p_raw
    """).fetchdf()
    avg_qa = df['avg_qa'].iloc[0]
    high_quality_pct = df['high_quality'].iloc[0] / df['total'].iloc[0] * 100
    print(f"\n[Test 4] Avg QA: {avg_qa:.3f}, High quality: {high_quality_pct:.1f}%")
    print("  PASSED" if avg_qa >= 0.5 else "  WARNING: Moderate/low QA")

    # Test 5: CH4 distribution
    avg_ch4 = con.execute("SELECT AVG(ch4_column) FROM bronze.sentinel5p_raw").fetchone()[0]
    ch4_check = 1700 <= avg_ch4 <= 2000
    print(f"\n[Test 5] Avg CH4: {avg_ch4:.2f} ppb - {'PASSED' if ch4_check else 'WARNING'}")

    # Test 6: NULL check
    null_geometry = con.execute("SELECT SUM(CASE WHEN location IS NULL" \
    " THEN 1 ELSE 0 END) FROM bronze.sentinel5p_raw").fetchone()[0]
    print(f"\n[Test 6] Null geometries: {null_geometry} - {'PASSED' if null_geometry==0
                                                           else 'FAILED'}")

    # Test 7: Spatial overlap with AER facilities
    df = con.execute("""
        WITH sentinel_pixels AS (
            SELECT DISTINCT ST_Point(ROUND(longitude,1), ROUND(latitude,1)) AS pixel_center
            FROM bronze.sentinel5p_raw
        ), facilities AS (
            SELECT location FROM bronze.aer_battery_monthly
        )
        SELECT COUNT(*) AS total_pixels,
               (SELECT COUNT(*) FROM facilities) AS total_facilities,
               COUNT(*) AS pixels_near_facilities
        FROM sentinel_pixels sp
        WHERE EXISTS (
            SELECT 1 FROM facilities f
            WHERE ST_Distance_Sphere(sp.pixel_center, f.location) <= 10000
        )
    """).fetchdf()
    pixels_near = df['pixels_near_facilities'].iloc[0]
    total_pixels = df['total_pixels'].iloc[0]
    overlap_pct = pixels_near / total_pixels * 100 if total_pixels else 0
    print(f"\n[Test 7] Spatial overlap: {overlap_pct:.1f}% - {'PASSED' if pixels_near>0
                                                              else 'WARNING'}")

    # Test 8: Duplicate check
    duplicates = con.execute("""
        SELECT COUNT(*) - COUNT(DISTINCT row_id) FROM bronze.sentinel5p_raw
    """).fetchone()[0]
    print(f"\n[Test 8] Duplicate row_ids: {duplicates} - {'PASSED' if duplicates==0
                                                          else 'WARNING'}")

    con.close()

    print("\nValidation Summary")
    print("="*60)
    print(f"Total rows: {total_rows:,}")
    print(f"Date coverage: {unique_dates} day(s), {unique_orbits} orbit(s)")
    print(f"Avg QA: {avg_qa:.3f}, Avg CH4: {avg_ch4:.2f} ppb")
    print(f"Spatial overlap: {pixels_near:,} pixels near facilities")
    print("="*60)
    return True

if __name__ == "__main__":
    import sys
    import traceback
    try:
        success = run_validation_tests()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
