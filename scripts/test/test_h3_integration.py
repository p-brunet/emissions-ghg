import os

import duckdb
import h3
from dotenv import load_dotenv

from config.constants import SENTINEL5P_QA_THRESHOLD_SILVER

load_dotenv()


def test_h3_python():
    lat, lon = 51.0447, -114.0719
    h3_cell = h3.latlng_to_cell(lat, lon, 6)
    assert h3_cell, "h3.latlng_to_cell returned empty result"
    print(f"H3 cell: {h3_cell}")
    print("h3-py working")


def test_h3_native_duckdb():
    print("\n[Test] h3 native extension in DuckDB")

    con = duckdb.connect(os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb"))
    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")

    result = con.execute("""
        SELECT h3_latlng_to_cell(51.0447, -114.0719, 6)
    """).fetchone()

    assert result and result[0], "h3_latlng_to_cell returned empty result"
    print(f"H3 cell: {result[0]}")
    print("DuckDB native h3 extension working")

    con.close()


def test_h3_on_data():
    print("\n[Test] h3 aggregation on Sentinel data")

    con = duckdb.connect(os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb"))
    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")

    result = con.execute(f"""
        SELECT
            h3_latlng_to_cell(latitude, longitude, 6) AS h3_cell,
            COUNT(*) AS pixel_count,
            ROUND(AVG(ch4_column), 2) AS avg_ch4
        FROM bronze.sentinel5p_raw
        WHERE qa_value >= {SENTINEL5P_QA_THRESHOLD_SILVER}
        GROUP BY h3_cell
        ORDER BY pixel_count DESC
        LIMIT 5
    """).fetchdf()

    assert not result.empty, "No h3 aggregation results"
    print(result)
    print("H3 aggregation working")

    con.close()


def main():
    print("=== H3 Tests ===")

    test_h3_python()
    test_h3_native_duckdb()
    test_h3_on_data()


if __name__ == "__main__":
    main()
