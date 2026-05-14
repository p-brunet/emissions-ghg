"""
Load processed Sentinel-5P parquet data into the Iceberg bronze table on MinIO,
then update bronze.ingestion_log so subsequent runs skip already-loaded files.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")
PARQUET_FILE = Path(DB_PATH).parent / "data" / "bronze" / "sentinel5p_ch4.parquet"


def get_already_loaded_files() -> set:
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        rows = con.execute(
            "SELECT DISTINCT file_path FROM bronze.ingestion_log"
        ).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _to_naive_timestamp(arr: pa.Array) -> pa.Array:
    """Strip timezone from a PyArrow timestamp array (UTC → naive)."""
    if pa.types.is_timestamp(arr.type) and arr.type.tz:
        series = arr.to_pandas()
        if hasattr(series.dt, "tz") and series.dt.tz is not None:
            series = series.dt.tz_localize(None)
        return pa.array(series, type=pa.timestamp("us"))
    return arr.cast(pa.timestamp("us"))


def load_to_bronze() -> None:
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")

    already_loaded = get_already_loaded_files()

    raw = pq.read_table(PARQUET_FILE)

    # Filter to files not yet in ingestion_log
    source_files = raw["source_file"].to_pandas()
    mask = ~source_files.isin(already_loaded)
    new_data = raw.filter(pa.array(mask.tolist()))

    if len(new_data) == 0:
        print("No new rows — all source files already loaded")
        return

    n = len(new_data)
    new_files = new_data["source_file"].to_pandas().unique().tolist()

    # Determine row_id offset from existing data
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        con.execute("LOAD iceberg;")
        con.execute(f"SET s3_endpoint = '{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}'")
        con.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}'")
        con.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY', 'minioadmin')}'")
        con.execute("SET s3_url_style = 'path'")
        con.execute("SET s3_use_ssl = false")
        max_row_id = con.execute(
            "SELECT COALESCE(MAX(row_id), 0) FROM bronze.sentinel5p_raw"
        ).fetchone()[0]
        con.close()
    except Exception:
        max_row_id = 0

    # Build transformed PyArrow table matching Iceberg schema
    times_raw = new_data["time"]
    times = _to_naive_timestamp(times_raw)

    times_pd = times.to_pandas()
    measurement_months = pa.array(
        (times_pd.dt.to_period("M").dt.to_timestamp()).dt.date.tolist(),
        type=pa.date32(),
    )

    orbit_raw = new_data["orbit"].to_pandas()
    orbit_int = pa.array(
        orbit_raw.where(orbit_raw.notna(), other=None).astype("Int64").tolist(),
        type=pa.int32(),
    )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    ingestion_timestamps = pa.array([now] * n, type=pa.timestamp("us"))

    transformed = pa.table(
        {
            "row_id": pa.array(
                range(max_row_id + 1, max_row_id + n + 1), type=pa.int64()
            ),
            "measurement_timestamp": times,
            "ch4_column": new_data["ch4"].cast(pa.float64()),
            "ch4_column_precision": new_data["ch4_precision"].cast(pa.float64()),
            "qa_value": new_data["qa"].cast(pa.float64()),
            "latitude": new_data["lat"].cast(pa.float64()),
            "longitude": new_data["lon"].cast(pa.float64()),
            "orbit_number": orbit_int,
            "processing_level": pa.array(["L2"] * n, type=pa.string()),
            "product_version": pa.array(["v02"] * n, type=pa.string()),
            "file_path": new_data["source_file"].cast(pa.string()),
            "ingestion_timestamp": ingestion_timestamps,
            "measurement_month": measurement_months,
        }
    )

    # Write to Iceberg
    from scripts.setup.init_iceberg_catalog import get_iceberg_catalog

    catalog = get_iceberg_catalog()
    table = catalog.load_table(("bronze", "sentinel5p_raw"))
    table.append(transformed)

    # Update ingestion_log in DuckDB
    con = duckdb.connect(DB_PATH)
    for fp in new_files:
        con.execute(
            "INSERT INTO bronze.ingestion_log (file_path, ingested_at) VALUES (?, CURRENT_TIMESTAMP)",
            [fp],
        )
    con.close()

    print(f"Inserted {n:,} rows into Iceberg bronze.sentinel5p_raw ({len(new_files)} files)")


def main() -> None:
    load_to_bronze()


if __name__ == "__main__":
    main()
