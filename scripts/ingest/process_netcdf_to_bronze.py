import re
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

INPUT_DIR = Path("./data/raw/sentinel5p")
OUTPUT_FILE = Path("./data/bronze/sentinel5p_ch4.parquet")

QA_THRESHOLD = 0.1

# Alberta bbox
MIN_LON, MIN_LAT = -120, 49
MAX_LON, MAX_LAT = -110, 60

def extract_orbit_from_filename(filename: str) -> int:
    """
    Extract orbit number from Sentinel-5P filename

    Pattern: S5P_OFFL_L2__CH4____YYYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_ORBIT_...
    Example: S5P_OFFL_L2__CH4____20250714T183522_20250714T201652_36033_03_...

    Returns orbit number (int) or None if not found
    """
    # Pattern: match 5-6 digit orbit number after two timestamps
    # S5P_..._STARTTIME_ENDTIME_ORBIT_...
    match = re.search(r'_(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{5,6})_', filename)

    if match:
        orbit = int(match.group(3))
        return orbit

    # Fallback: try ds.attrs
    return None


def extract_file(nc_path: Path) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path, group="PRODUCT")

    ch4_var = "methane_mixing_ratio_bias_corrected"
    if ch4_var not in ds:
        raise ValueError(f"{ch4_var} not found in {nc_path.name}")

    lat = ds["latitude"]
    lon = ds["longitude"]

    bbox_mask = (
        (lat >= MIN_LAT) &
        (lat <= MAX_LAT) &
        (lon >= MIN_LON) &
        (lon <= MAX_LON)
    )

    data = xr.Dataset(
        {
            "ch4": ds[ch4_var],
            "qa": ds["qa_value"],
            "lat": lat,
            "lon": lon,
        }
    )

    filtered = data.where(bbox_mask, drop=True)

    filtered = filtered.where(
        (~np.isnan(filtered.ch4)) &
        (~np.isnan(filtered.lat)) &
        (~np.isnan(filtered.lon)) &
        (filtered.qa >= QA_THRESHOLD),
        drop=True
    )

    if filtered.ch4.size == 0:
        ds.close()
        return pd.DataFrame()

    stacked = filtered.stack(pixel=("time", "scanline", "ground_pixel"))

    if stacked.pixel.size == 0:
        ds.close()
        return pd.DataFrame()

    df = stacked.to_dataframe().reset_index()
    df = df.dropna()

    df = df[[
        "time",
        "lat",
        "lon",
        "ch4",
        "qa",
        "scanline",
        "ground_pixel"
    ]]

    orbit = extract_orbit_from_filename(nc_path.name)

    if orbit is None:
        orbit = ds.attrs.get("orbit", None)

    df["orbit"] = orbit
    df["source_file"] = nc_path.name

    ds.close()

    return df


def process_all() -> pd.DataFrame:
    files = sorted(INPUT_DIR.glob("*.nc"))

    if not files:
        raise FileNotFoundError(f"No NetCDF files in {INPUT_DIR}")

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

    df.to_parquet(
        OUTPUT_FILE,
        engine="pyarrow",
        compression="zstd",
        index=False
    )

    print(f"Saved {len(df):,} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
