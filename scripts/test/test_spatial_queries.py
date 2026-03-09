import os

import duckdb
from dotenv import load_dotenv

load_dotenv()


def test_spatial_queries():
    """
    Run validation queries on loaded AER data
    """
    print("=" * 60)
    print("Testing Spatial Queries - AER Battery Data")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    con = duckdb.connect(db_path)
    con.execute("LOAD spatial;")

    # Test 1: Count total facilities
    print("\n[Test 1] Total facilities loaded:")
    result = con.execute("""
        SELECT COUNT(*) as total_facilities
        FROM bronze.aer_battery_monthly
    """).fetchone()
    print(f"  Total: {result[0]:,}")

    if result[0] == 0:
        print("  WARNING: No data loaded!")
        con.close()
        return False

    # Test 2: Facilities by operator (top 10)
    print("\n[Test 2] Top 10 operators by facility count:")
    result = con.execute("""
        SELECT
            operator,
            COUNT(*) as facility_count
        FROM bronze.aer_battery_monthly
        WHERE operator IS NOT NULL
        GROUP BY operator
        ORDER BY facility_count DESC
        LIMIT 10
    """).fetchdf()
    print(result.to_string(index=False))

    # Test 3: Spatial extent (bounding box)
    print("\n[Test 3] Spatial extent (bounding box):")
    result = con.execute("""
        SELECT
            ROUND(MIN(latitude), 2) as min_lat,
            ROUND(MAX(latitude), 2) as max_lat,
            ROUND(MIN(longitude), 2) as min_lon,
            ROUND(MAX(longitude), 2) as max_lon,
            ROUND(MAX(latitude) - MIN(latitude), 2) as lat_range,
            ROUND(MAX(longitude) - MIN(longitude), 2) as lon_range
        FROM bronze.aer_battery_monthly
    """).fetchdf()
    print(result.to_string(index=False))

    # Validate Alberta bounds
    min_lat = result["min_lat"].iloc[0]
    max_lat = result["max_lat"].iloc[0]
    if min_lat < 49 or max_lat > 60:
        print("  WARNING: Some coordinates outside Alberta bounds!")
    else:
        print("  SUCCESS: All coordinates within Alberta bounds")

    # Test 4: Distance query (facilities near Calgary)
    print("\n[Test 4] Facilities within 50km of Calgary (51.0447, -114.0719):")
    result = con.execute("""
        SELECT
            facility_id,
            operator,
            ROUND(latitude, 4) as lat,
            ROUND(longitude, 4) as lon,
            ROUND(
                ST_Distance_Sphere(
                    location,
                    ST_Point(-114.0719, 51.0447)
            ) / 1000, 1
        ) AS distance_km
        FROM bronze.aer_battery_monthly
        WHERE ST_Distance_Sphere(
            location,
            ST_Point(-114.0719, 51.0447)
        ) <= 50000
        ORDER BY distance_km
        LIMIT 10
    """).fetchdf()

    if len(result) > 0:
        print(result.to_string(index=False))
        print(f"\n  SUCCESS: Found {len(result)} facilities near Calgary")
    else:
        print("  INFO: No facilities within 50km of Calgary")

    # Test 5: Monthly production statistics
    print("\n[Test 5] Monthly production statistics:")
    result = con.execute("""
        SELECT
            reporting_month,
            COUNT(*) as num_facilities,
            ROUND(SUM(gas_prod_1000m3), 1) as total_gas_prod_1000m3,
            ROUND(SUM(gas_flared_1000m3), 1) as total_gas_flared_1000m3,
            ROUND(SUM(gas_vented_1000m3), 1) as total_gas_vented_1000m3,
            ROUND(
                SUM(gas_flared_1000m3 + gas_vented_1000m3) /
                NULLIF(SUM(gas_prod_1000m3), 0) * 100,
                2
            ) as pct_emissions
        FROM bronze.aer_battery_monthly
        WHERE gas_prod_1000m3 > 0
        GROUP BY reporting_month
        ORDER BY reporting_month
    """).fetchdf()

    print(result.to_string(index=False))

    # Test 6: Geometry validation
    print("\n[Test 6] Geometry validation:")
    result = con.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(location) as rows_with_geometry,
            COUNT(*) - COUNT(location) as rows_without_geometry
        FROM bronze.aer_battery_monthly
    """).fetchdf()
    print(result.to_string(index=False))

    if result["rows_without_geometry"].iloc[0] > 0:
        print("  WARNING: Some rows missing geometry!")
    else:
        print("  SUCCESS: All rows have valid geometry")

    con.close()

    print("\n" + "=" * 60)
    print("SUCCESS: All spatial queries completed")
    print("=" * 60)

    return True


if __name__ == "__main__":
    try:
        success = test_spatial_queries()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
