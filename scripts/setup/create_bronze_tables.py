"""
Create Bronze layer tables for medallion architecture.

- aer_battery_monthly: DuckDB table (small, no Iceberg overhead)
- sentinel5p_raw: Iceberg table on MinIO, exposed as a DuckDB VIEW
- ingestion_log: DuckDB table tracking which NetCDF files have been loaded

On first run after migration: existing DuckDB sentinel5p_raw TABLE data is
exported to Iceberg, then the table is replaced by the view.
"""

import os
import sys

import pyarrow as pa
import duckdb
from dotenv import load_dotenv

load_dotenv()


def create_aer_facilities_table(con) -> None:
    print("\n" + "=" * 60)
    print("Creating bronze.aer_battery_monthly")
    print("=" * 60)

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.aer_battery_monthly (
            row_id BIGINT,
            facility_id VARCHAR,
            licence VARCHAR,
            operator VARCHAR,
            facility_type VARCHAR,
            facility_description VARCHAR,
            reporting_month DATE,
            ingestion_date DATE,
            source_file VARCHAR,
            bty_location_raw VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            location GEOMETRY,
            oil_prod_m3 DOUBLE,
            gas_prod_1000m3 DOUBLE,
            gas_flared_1000m3 DOUBLE,
            gas_vented_1000m3 DOUBLE,
            water_prod_m3 DOUBLE,
            total_wells INTEGER
        );
    """)
    print("SUCCESS: Table 'bronze.aer_battery_monthly' ready")


def create_ingestion_log_table(con) -> None:
    print("\n" + "=" * 60)
    print("Creating bronze.ingestion_log")
    print("=" * 60)

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
            file_path VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("SUCCESS: Table 'bronze.ingestion_log' ready")


def _migrate_to_iceberg(con, iceberg_table, total_count: int) -> None:
    """Export existing DuckDB bronze.sentinel5p_raw rows to Iceberg in monthly batches."""
    print(f"  Migrating {total_count:,} existing rows to Iceberg...")

    months = con.execute("""
        SELECT DISTINCT DATE_TRUNC('month', measurement_timestamp)::VARCHAR AS m
        FROM bronze.sentinel5p_raw
        ORDER BY m
    """).fetchall()

    migrated = 0
    for (month_str,) in months:
        result = con.execute(f"""
            SELECT
                row_id,
                measurement_timestamp::TIMESTAMP AS measurement_timestamp,
                ch4_column,
                ch4_column_precision,
                qa_value,
                latitude,
                longitude,
                orbit_number,
                processing_level,
                product_version,
                file_path,
                COALESCE(ingestion_timestamp, CURRENT_TIMESTAMP)::TIMESTAMP AS ingestion_timestamp,
                DATE_TRUNC('month', measurement_timestamp)::DATE AS measurement_month
            FROM bronze.sentinel5p_raw
            WHERE DATE_TRUNC('month', measurement_timestamp)::VARCHAR = '{month_str}'
        """).arrow()
        batch = result.read_all() if isinstance(result, pa.RecordBatchReader) else result

        iceberg_table.append(batch)
        migrated += len(batch)
        print(f"    {month_str}: {len(batch):,} rows → Iceberg")

    print(f"  Migration complete: {migrated:,} rows")


def create_sentinel5p_iceberg_and_view(con) -> None:
    """
    Create the Iceberg table on MinIO, migrate any existing DuckDB data,
    then replace the DuckDB TABLE with a VIEW over iceberg_scan().
    """
    print("\n" + "=" * 60)
    print("Creating bronze.sentinel5p_raw (Iceberg + DuckDB view)")
    print("=" * 60)

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent.parent))
    from scripts.setup.init_iceberg_catalog import init_iceberg_catalog

    catalog = init_iceberg_catalog()
    iceberg_table = catalog.load_table(("bronze", "sentinel5p_raw"))
    table_location = iceberg_table.location()

    # Check if we have an old DuckDB TABLE (not a VIEW) to migrate
    result = con.execute("""
        SELECT table_type FROM information_schema.tables
        WHERE table_schema = 'bronze' AND table_name = 'sentinel5p_raw'
    """).fetchone()

    if result and result[0] == "BASE TABLE":
        count = con.execute("SELECT COUNT(*) FROM bronze.sentinel5p_raw").fetchone()[0]
        print(f"  Found existing DuckDB TABLE with {count:,} rows — migrating to Iceberg")

        if count > 0:
            if iceberg_table.current_snapshot() is None:
                _migrate_to_iceberg(con, iceberg_table, count)
            else:
                print("  Iceberg already has data — skipping migration (resuming after failure)")

        # Populate ingestion_log from old table before dropping it
        con.execute("""
            INSERT INTO bronze.ingestion_log (file_path, ingested_at)
            SELECT DISTINCT file_path, MIN(ingestion_timestamp)
            FROM bronze.sentinel5p_raw
            WHERE file_path IS NOT NULL
              AND file_path NOT IN (SELECT file_path FROM bronze.ingestion_log)
            GROUP BY file_path
        """)
        print("  Ingestion log populated from old table")

        con.execute("DROP TABLE bronze.sentinel5p_raw")
        print("  Old DuckDB TABLE dropped")

    elif result and result[0] == "VIEW":
        print("  View already exists — skipping migration")
        return
    else:
        print("  No existing table — creating fresh view")

    # Create DuckDB view over Iceberg (transparent to dbt models)
    con.execute(f"""
        CREATE OR REPLACE VIEW bronze.sentinel5p_raw AS
        SELECT
            row_id,
            measurement_timestamp,
            ch4_column,
            ch4_column_precision,
            qa_value,
            latitude,
            longitude,
            ST_Point(longitude, latitude) AS location,
            orbit_number,
            processing_level,
            product_version,
            file_path,
            ingestion_timestamp,
            measurement_month
        FROM iceberg_scan('{table_location}')
    """)
    print(f"SUCCESS: DuckDB VIEW 'bronze.sentinel5p_raw' → iceberg_scan('{table_location}')")


def main() -> None:
    print("=" * 60)
    print("Bronze Layer Setup")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    print(f"\nConnecting to: {db_path}")
    con = duckdb.connect(db_path)

    # Load extensions and configure S3 + Iceberg once for the whole session
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{endpoint}';")
    con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}';")
    con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY', 'minioadmin')}';")
    con.execute("SET s3_url_style = 'path';")
    con.execute("SET s3_use_ssl = false;")
    con.execute("LOAD iceberg;")
    con.execute("SET unsafe_enable_version_guessing = true;")
    con.execute("LOAD spatial;")

    create_aer_facilities_table(con)
    create_ingestion_log_table(con)
    create_sentinel5p_iceberg_and_view(con)

    # Verify
    print("\n" + "=" * 60)
    print("Verification")
    print("=" * 60)

    result = con.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'bronze'
        ORDER BY table_name
    """).fetchall()

    for row in result:
        print(f"  {row[0]}.{row[1]} ({row[2]})")

    con.close()
    print("\nSUCCESS: Bronze layer ready")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
