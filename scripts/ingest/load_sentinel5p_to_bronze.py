import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

PARQUET_FILE = Path("./data/bronze/sentinel5p_ch4.parquet")
DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")


def load_to_bronze() -> None:
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")

    con = duckdb.connect(DB_PATH)
    con.execute("LOAD spatial")

    existing = con.execute(
        "SELECT COUNT(*) FROM bronze.sentinel5p_raw"
    ).fetchone()[0]

    con.execute(
        """
        INSERT INTO bronze.sentinel5p_raw
        SELECT
            ROW_NUMBER() OVER () + ? AS row_id,
            time::TIMESTAMP AS measurement_timestamp,
            ch4::DOUBLE AS ch4_column,
            NULL::DOUBLE AS ch4_column_precision,
            qa::DOUBLE AS qa_value,
            lat::DOUBLE AS latitude,
            lon::DOUBLE AS longitude,
            ST_Point(lon, lat) AS location,
            orbit::INTEGER AS orbit_number,
            'L2' AS processing_level,
            'v02' AS product_version,
            source_file::VARCHAR AS file_path,
            CURRENT_TIMESTAMP AS ingestion_timestamp
        FROM read_parquet(?)
        """,
        [existing, str(PARQUET_FILE)],
    )

    inserted = con.execute(
        "SELECT COUNT(*) FROM bronze.sentinel5p_raw"
    ).fetchone()[0] - existing

    print(f"Inserted {inserted:,} rows into bronze.sentinel5p_raw")

    con.close()


def main() -> None:
    load_to_bronze()


if __name__ == "__main__":
    main()
