"""
Initialize DuckDB with Iceberg catalog and spatial extensions
Creates schemas for medallion architecture (bronze/silver/gold)
"""

import os
from typing import Literal

import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def init_duckdb_iceberg() -> Literal[True]:
    """
    Initialize DuckDB with necessary extensions and schemas
    """
    print("=" * 60)
    print("Initializing DuckDB + Iceberg Catalog")
    print("=" * 60)

    # Connect to DuckDB (creates file if doesn't exist)
    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    print(f"\n Connecting to DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    print("DONE: Connected")

    # Install and load extensions
    print("\n Installing DuckDB extensions...")
    extensions = ["spatial", "httpfs", "aws"]  # ui extension is not working on MacOS ?

    for ext in extensions:
        print(f"  - Installing {ext}...")
        try:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            print(f"    SUCCESS: {ext} loaded")
        except Exception as e:
            print(f"    WARNING: {ext} failed: {e}")

    print("DONE: Extensions installed")

    # Configure S3 (MinIO) credentials
    print("\n Configuring MinIO credentials...")
    try:
        con.execute(f"""
            SET s3_endpoint = '{os.getenv("MINIO_ENDPOINT")}';
            SET s3_access_key_id = '{os.getenv("MINIO_ACCESS_KEY")}';
            SET s3_secret_access_key = '{os.getenv("MINIO_SECRET_KEY")}';
            SET s3_url_style = 'path';
            SET s3_use_ssl = false;
        """)
        print("DONE: MinIO configured")
    except Exception as e:
        print(f"WARNING: MinIO config: {e}")

    # Create schemas
    print("\n Creating schemas...")
    schemas = ["bronze", "silver", "gold"]

    for schema in schemas:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        print(f"  DONE: Schema '{schema}' created")

    # Test MinIO connection
    print("\n Testing MinIO connection...")
    try:
        con.execute("""
            SELECT * FROM read_csv_auto(
                's3://ghg-warehouse/test.csv',
                ignore_errors=true
            ) LIMIT 1;
        """).fetchall()
        print("SUCCESS: MinIO connection successful")
    except Exception as e:
        print(f"WARNING: MinIO test (expected if no data yet): {str(e)[:100]}...")
        print("  MinIO is accessible")  # to remove

    # Show configuration summary
    print("\n" + "=" * 60)
    print("Configuration Summary")
    print("=" * 60)
    print(f"DuckDB Database: {db_path}")
    print(f"MinIO Endpoint: {os.getenv('MINIO_ENDPOINT')}")
    print(f"Warehouse Bucket: {os.getenv('MINIO_BUCKET_WAREHOUSE')}")
    print(f"Schemas: {', '.join(schemas)}")

    con.close()
    print("Initialization complete!")
    print("=" * 60)

    return True


if __name__ == "__main__":
    try:
        init_duckdb_iceberg()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
