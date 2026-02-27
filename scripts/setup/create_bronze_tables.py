"""
Create Bronze layer tables for medallion architecture
Bronze = raw data, no constraints, append-only
"""

import os
from typing import Literal

import duckdb
from dotenv import load_dotenv

load_dotenv()


def create_aer_facilities_table(con) -> Literal[True]:
    """
    Create bronze.aer_facilities table
    NO PRIMARY KEY - we accept duplicates/errors in Bronze
    """
    print("\n" + "=" * 60)
    print("Creating bronze.aer_facilities Table")
    print("=" * 60)

    con.execute("DROP TABLE IF EXISTS bronze.aer_facilities;")

    con.execute("""
        CREATE TABLE bronze.aer_facilities (
            row_id BIGINT,                    -- Auto-increment row number
            facility_id VARCHAR,              -- From CSV (may have duplicates)
            facility_name VARCHAR,
            facility_type VARCHAR,
            operator VARCHAR,
            bty_location VARCHAR,             -- BTY legal land description
            latitude DOUBLE,                  -- Converted from BTY
            longitude DOUBLE,                 -- Converted from BTY
            location GEOMETRY,                -- ST_Point(lon, lat)
            reported_ch4_tonnes_yr DOUBLE,    -- If available in CSV
            ingestion_date DATE,              -- When we loaded this
            source_file VARCHAR               -- Which CSV file
        );
    """)

    print("SUCCESS: Table 'bronze.aer_facilities' created (no PK)")

    return True


def create_sentinel5p_table(con) -> Literal[True]:
    """
    Create bronze.sentinel5p_raw table
    NO PRIMARY KEY - raw pixels from NetCDF
    """
    print("\n" + "=" * 60)
    print("Creating bronze.sentinel5p_raw Table")
    print("=" * 60)

    con.execute("DROP TABLE IF EXISTS bronze.sentinel5p_raw;")

    con.execute("""
        CREATE TABLE bronze.sentinel5p_raw (
            row_id BIGINT,                    -- Auto-increment
            measurement_timestamp TIMESTAMP,   -- From NetCDF time dimension
            ch4_column DOUBLE,                 -- CH4 mixing ratio
            ch4_column_precision DOUBLE,       -- Uncertainty
            qa_value DOUBLE,                   -- Quality flag (0-1)
            latitude DOUBLE,                   -- Pixel center latitude
            longitude DOUBLE,                  -- Pixel center longitude
            location GEOMETRY,                 -- ST_Point(lon, lat)
            orbit_number INTEGER,              -- Satellite orbit
            processing_level VARCHAR,          -- e.g., 'L2'
            product_version VARCHAR,           -- e.g., 'v02.06.00'
            file_path VARCHAR,                 -- Source NetCDF file
            ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    print("SUCCESS: Table 'bronze.sentinel5p_raw' created (no PK)")

    return True


def main() -> None:
    """
    Main execution function
    """
    print("=" * 60)
    print("Bronze Layer Table Creation")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    print(f"\nConnecting to: {db_path}")
    con = duckdb.connect(db_path)

    print("Loading extensions...")
    con.execute("LOAD spatial;")
    print("DONE: Extensions loaded")

    create_aer_facilities_table(con)
    create_sentinel5p_table(con)

    # Simple verification - count tables
    print("\n" + "=" * 60)
    print("Verification")
    print("=" * 60)

    count = con.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
    """).fetchone()[0]

    print(f"Tables in bronze schema: {count}")
    print("Expected: 2 (aer_facilities, sentinel5p_raw)")

    con.close()

    print("\n" + "=" * 60)
    print("SUCCESS: All Bronze tables created")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
