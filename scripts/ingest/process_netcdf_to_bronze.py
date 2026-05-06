import os
import re

import duckdb
import numpy as np
import pandas as pd
import xarray as xr
from dotenv import load_dotenv

from config.constants import (
    ALBERTA_BBOX,
    BRONZE_DATA_DIR,
    SENTINEL5P_QA_THRESHOLD_BRONZE,
    SENTINEL5P_RAW_DIR,
)

load_dotenv()

INPUT_DIR = SENTINEL5P_RAW_DIR
OUTPUT_FILE = BRONZE_DATA_DIR / "sentinel5p_ch4.parquet"
DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")

CH4_VAR = "methane_mixing_ratio_bias_corrected"
CH4_PRECISION_VAR = "methane_mixing_ratio_bias_corrected_precision"


def extract_orbit_from_filename(filename: str) -> int:
    """
    Extract orbit number from Sentinel-5P filename.

    Pattern: S5P_OFFL_L2__CH4____YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_ORBIT_...
    Example: S5P_OFFL_L2__CH4____20250714T183522_20250714T201652_36033_03_...
    """
    match = re.search(r"_(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{5,6})_", filename)
    if match:
        return int(match.group(3))
    return None


def extract_file(nc_path) -> pd.DataFrame:
    with xr.open_dataset(nc_path, group="PRODUCT") as ds:
        if CH4_VAR not in ds:
            raise ValueError(f"{CH4_VAR} not found in {nc_path.name}")

        lat = ds["latitude"]
        lon = ds["longitude"]

        bbox_mask = (
            (lat >= ALBERTA_BBOX["min_lat"])
            & (lat <= ALBERTA_BBOX["max_lat"])
            & (lon >= ALBERTA_BBOX["min_lon"])
            & (lon <= ALBERTA_BBOX["max_lon"])
        )

        data = xr.Dataset(
            {
                "ch4": ds[CH4_VAR],
                "ch4_precision": ds[CH4_PRECISION_VAR]
                if CH4_PRECISION_VAR in ds
                else xr.full_like(ds[CH4_VAR], fill_value=float("nan")),
                "qa": ds["qa_value"],
                "lat": lat,
                "lon": lon,
            }
        )

        filtered = data.where(bbox_mask, drop=True)
        filtered = filtered.where(
            (~np.isnan(filtered.ch4))
            & (~np.isnan(filtered.lat))
            & (~np.isnan(filtered.lon))
            & (filtered.qa >= SENTINEL5P_QA_THRESHOLD_BRONZE),
            drop=True,
        )

        if filtered.ch4.size == 0:
            return pd.DataFrame()

        stacked = filtered.stack(pixel=("time", "scanline", "ground_pixel"))

        if stacked.pixel.size == 0:
            return pd.DataFrame()

        df = stacked.to_dataframe().reset_index()
        # xarray where(drop=True) doesn't fully remove NaN rows after to_dataframe()
        df = df.dropna(subset=["ch4", "lat", "lon"])

        df = df[["time", "lat", "lon", "ch4", "ch4_precision", "qa", "scanline", "ground_pixel"]]

        orbit = extract_orbit_from_filename(nc_path.name)
        if orbit is None:
            orbit = ds.attrs.get("orbit", None)

        df["orbit"] = orbit
        df["source_file"] = nc_path.name

    return df


def get_already_loaded_files() -> set:
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        rows = con.execute("SELECT DISTINCT file_path FROM bronze.sentinel5p_raw").fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def process_all() -> pd.DataFrame:
    already_loaded = get_already_loaded_files()
    files = [f for f in sorted(INPUT_DIR.glob("*.nc")) if f.name not in already_loaded]

    if not files:
        print("No new NetCDF files to process — all already loaded in bronze.")
        return pd.DataFrame()

    frames = []

    for file in files:
        try:
            df = extract_file(file)
            if not df.empty:
                frames.append(df)
        except (OSError, ValueError, KeyError) as e:
            print(f"Skipping {file.name}: {e}")

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def main():
    df = process_all()

    if df.empty:
        print("No data extracted")
        return

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(OUTPUT_FILE, engine="pyarrow", compression="zstd", index=False)

    print(f"Saved {len(df):,} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
