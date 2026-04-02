"""
Initialize DuckDB with spatial extensions and medallion architecture schemas
(bronze/silver/gold)
"""

import os
import sys

import duckdb
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def init_duckdb() -> None:
    """
    Initialize DuckDB with necessary extensions and schemas.
    """
    print("=" * 60)
    print("Initializing DuckDB")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    print(f"\n Connecting to DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    print("DONE: Connected")

    # Install and load extensions
    print("\n Installing DuckDB extensions...")
    extensions = ["spatial", "httpfs", "aws"]

    for ext in extensions:
        print(f"  - Installing {ext}...")
        try:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            print(f"    SUCCESS: {ext} loaded")
        except Exception as e:
            print(f"    WARNING: {ext} failed: {e}")

    # h3 community extension
    try:
        con.execute("INSTALL h3 FROM community;")
        con.execute("LOAD h3;")
        print("    SUCCESS: h3 loaded")
    except Exception as e:
        print(f"INFO: h3 not available on this platform ({e})")

    print("DONE: Extensions installed")

    # Configure S3 (MinIO) credentials — use individual statements to avoid
    # multi-statement injection if a value contains a semicolon
    print("\n Configuring MinIO credentials...")
    try:
        endpoint = os.getenv("MINIO_ENDPOINT", "")
        access_key = os.getenv("MINIO_ACCESS_KEY", "")
        secret_key = os.getenv("MINIO_SECRET_KEY", "")

        con.execute("SET s3_endpoint = ?", [endpoint])
        con.execute("SET s3_access_key_id = ?", [access_key])
        con.execute("SET s3_secret_access_key = ?", [secret_key])
        con.execute("SET s3_url_style = 'path'")
        con.execute("SET s3_use_ssl = false")
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


if __name__ == "__main__":
    try:
        init_duckdb()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
