"""
Initialize DuckDB with spatial extensions and medallion architecture schemas
(bronze/silver/gold), and create the PyIceberg catalog + bronze.sentinel5p_raw
Iceberg table on MinIO.
"""

import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def init_duckdb() -> None:
    """Initialize DuckDB with necessary extensions and schemas."""
    print("=" * 60)
    print("Initializing DuckDB")
    print("=" * 60)

    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
    print(f"\n Connecting to DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    print("DONE: Connected")

    print("\n Installing DuckDB extensions...")
    extensions = ["spatial", "httpfs", "aws", "iceberg"]

    for ext in extensions:
        print(f"  - Installing {ext}...")
        try:
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")
            print(f"    SUCCESS: {ext} loaded")
        except Exception as e:
            print(f"    WARNING: {ext} failed: {e}")

    try:
        con.execute("INSTALL h3 FROM community;")
        con.execute("LOAD h3;")
        print("    SUCCESS: h3 loaded")
    except Exception as e:
        print(f"INFO: h3 not available on this platform ({e})")

    print("DONE: Extensions installed")

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

    print("\n Creating schemas...")
    schemas = ["bronze", "silver", "gold"]
    for schema in schemas:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        print(f"  DONE: Schema '{schema}' created")

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

    print("\n" + "=" * 60)
    print("Configuration Summary")
    print("=" * 60)
    print(f"DuckDB Database: {db_path}")
    print(f"MinIO Endpoint: {os.getenv('MINIO_ENDPOINT')}")
    print(f"Schemas: {', '.join(schemas)}")

    con.close()
    print("DuckDB initialization complete!")
    print("=" * 60)


def get_iceberg_catalog():
    """Return a configured PyIceberg SqlCatalog pointing to MinIO."""
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse_dir = PROJECT_ROOT / "warehouse"
    warehouse_dir.mkdir(exist_ok=True)
    catalog_db = str(warehouse_dir / "iceberg_catalog.db")

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"

    return SqlCatalog(
        "ghg",
        **{
            "uri": f"sqlite:///{catalog_db}",
            "warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3://ghg-warehouse/"),
            "s3.endpoint": endpoint,
            "s3.access-key-id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "s3.secret-access-key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            "s3.path-style-access": "true",
        },
    )


def init_iceberg_catalog():
    """Create the PyIceberg catalog namespace and sentinel5p_raw table if absent."""
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import (
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
        TimestampType,
    )

    print("=" * 60)
    print("Initializing Iceberg Catalog")
    print("=" * 60)

    catalog = get_iceberg_catalog()

    namespace = ("bronze",)
    try:
        catalog.create_namespace(namespace)
        print("DONE: Namespace 'bronze' created")
    except NamespaceAlreadyExistsError:
        print("INFO: Namespace 'bronze' already exists")

    table_id = ("bronze", "sentinel5p_raw")

    try:
        catalog.load_table(table_id)
        print("INFO: Table 'bronze.sentinel5p_raw' already registered")
    except NoSuchTableError:
        schema = Schema(
            NestedField(1, "row_id", LongType(), required=False),
            NestedField(2, "measurement_timestamp", TimestampType(), required=False),
            NestedField(3, "ch4_column", DoubleType(), required=False),
            NestedField(4, "ch4_column_precision", DoubleType(), required=False),
            NestedField(5, "qa_value", DoubleType(), required=False),
            NestedField(6, "latitude", DoubleType(), required=False),
            NestedField(7, "longitude", DoubleType(), required=False),
            NestedField(8, "orbit_number", IntegerType(), required=False),
            NestedField(9, "processing_level", StringType(), required=False),
            NestedField(10, "product_version", StringType(), required=False),
            NestedField(11, "file_path", StringType(), required=False),
            NestedField(12, "ingestion_timestamp", TimestampType(), required=False),
            NestedField(13, "measurement_month", DateType(), required=False),
        )
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=13,
                field_id=1000,
                transform=IdentityTransform(),
                name="measurement_month",
            )
        )
        catalog.create_table(
            table_id,
            schema=schema,
            partition_spec=partition_spec,
            properties={"write.parquet.compression-codec": "zstd"},
        )
        print("DONE: Table 'bronze.sentinel5p_raw' created in Iceberg")

    table = catalog.load_table(table_id)
    print(f"Table location: {table.location()}")
    print("=" * 60)
    return catalog


if __name__ == "__main__":
    try:
        init_duckdb()
        init_iceberg_catalog()
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
