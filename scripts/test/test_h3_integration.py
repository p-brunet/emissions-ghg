import os

import duckdb
import h3
from dotenv import load_dotenv

load_dotenv()


def h3_from_point(lat, lon, resolution):
    return h3.latlng_to_cell(lat, lon, resolution)


def test_h3_python():
    lat, lon = 51.0447, -114.0719

    h3_cell = h3.latlng_to_cell(lat, lon, 6)
    print(f"H3 cell: {h3_cell}")
    print("h3-py working")


def test_h3_udf_duckdb():
    print("\n[Test] h3 UDF in DuckDB")

    con = duckdb.connect(os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb"))

    con.create_function("h3_from_point", h3_from_point, return_type="VARCHAR")

    result = con.execute("""
        SELECT h3_from_point(51.0447, -114.0719, 6)
    """).fetchone()

    print(f"H3 cell: {result[0]}")
    print("DuckDB UDF working")

    con.close()


def test_h3_on_data():
    print("\n[Test] h3 on Sentinel data")

    con = duckdb.connect(os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb"))
    con.execute("LOAD spatial;")

    con.create_function("h3_from_point", h3_from_point, return_type="VARCHAR")

    result = con.execute("""
        SELECT
            h3_from_point(latitude, longitude, 6) AS h3_cell,
            COUNT(*) AS pixel_count,
            ROUND(AVG(ch4_column), 2) AS avg_ch4
        FROM bronze.sentinel5p_raw
        WHERE qa_value >= 0.5
        GROUP BY h3_cell
        ORDER BY pixel_count DESC
        LIMIT 5
    """).fetchdf()

    print(result)
    print("Aggregation working")

    con.close()


def main():
    print("=== H3 Tests ===")

    test_h3_python()
    test_h3_udf_duckdb()
    test_h3_on_data()


if __name__ == "__main__":
    main()
