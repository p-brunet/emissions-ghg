import os
import sys

import duckdb
import matplotlib.pyplot as plt
from dotenv import load_dotenv

from config.constants import SENTINEL5P_QA_THRESHOLD_SILVER, VISUALIZATIONS_DIR

load_dotenv()

DB_PATH = os.getenv("DUCKDB_DATABASE_PATH", "./emissions_ghg.duckdb")


def save_plot(fig, filename):
    VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)
    path = VISUALIZATIONS_DIR / filename
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return path


def create_ch4_heatmap(con):
    df = con.execute("""
        SELECT latitude, longitude, ch4_column
        FROM silver.sentinel5p_ch4_cleaned
    """).fetchdf()

    df["lat_grid"] = (df["latitude"] / 0.05).round() * 0.05
    df["lon_grid"] = (df["longitude"] / 0.05).round() * 0.05
    grid = df.groupby(["lat_grid", "lon_grid"])["ch4_column"].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 10))
    sc = ax.scatter(
        grid["lon_grid"],
        grid["lat_grid"],
        c=grid["ch4_column"],
        cmap="RdYlBu_r",
        s=50,
        alpha=0.7,
        edgecolors="none",
    )
    plt.colorbar(sc, ax=ax, label="CH₄ Column (ppb)")
    ax.set(
        xlabel="Longitude (°)",
        ylabel="Latitude (°)",
        title="Sentinel-5P CH₄ Concentrations - Alberta",
    )
    ax.plot([-120, -110, -110, -120, -120], [49, 49, 60, 60, 49], "k--", lw=2)
    ax.grid(True, alpha=0.3)
    return save_plot(fig, "ch4_heatmap.png")


def create_facilities_overlay(con):
    ch4_df = con.execute("""
        SELECT ROUND(latitude, 2) AS lat_grid, ROUND(longitude, 2) AS lon_grid,
               AVG(ch4_column) AS avg_ch4
        FROM silver.sentinel5p_ch4_cleaned
        GROUP BY lat_grid, lon_grid
    """).fetchdf()
    facilities_df = con.execute("""
        SELECT latitude, longitude
        FROM silver.aer_facilities_cleaned
    """).fetchdf()

    fig, ax = plt.subplots(figsize=(14, 12))
    sc = ax.scatter(
        ch4_df["lon_grid"],
        ch4_df["lat_grid"],
        c=ch4_df["avg_ch4"],
        cmap="YlOrRd",
        s=8,
        alpha=0.5,
        edgecolors="none",
        label="CH₄",
    )
    ax.scatter(
        facilities_df["longitude"],
        facilities_df["latitude"],
        c="blue",
        marker="x",
        s=15,
        alpha=0.8,
        linewidths=0.8,
        label="AER Facilities",
    )
    plt.colorbar(sc, ax=ax, label="Avg CH₄ (ppb)")
    ax.set(
        xlabel="Longitude (°)",
        ylabel="Latitude (°)",
        title="CH₄ Concentrations with O&G Facilities - Alberta",
    )
    ax.grid(True, alpha=0.3)
    ax.legend()
    return save_plot(fig, "ch4_facilities_overlay.png")


def create_summary_stats_plot(con):
    qa_df = con.execute("""
        SELECT ROUND(qa_value, 1) AS qa_bin, COUNT(*) AS count
        FROM bronze.sentinel5p_raw
        GROUP BY qa_bin
        ORDER BY qa_bin
    """).fetchdf()
    ch4_df = con.execute("""
        SELECT ch4_column
        FROM silver.sentinel5p_ch4_cleaned
    """).fetchdf()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    ax1.bar(qa_df["qa_bin"], qa_df["count"], width=0.08, color="steelblue", alpha=0.7)
    ax1.set(xlabel="QA Value", ylabel="Pixel Count", title="QA Value Distribution")
    ax1.axvline(
        SENTINEL5P_QA_THRESHOLD_SILVER,
        color="red",
        linestyle="--",
        lw=2,
        label=f"Silver threshold ({SENTINEL5P_QA_THRESHOLD_SILVER})",
    )
    ax1.grid(True, alpha=0.3, axis="y")
    ax1.legend()

    ax2.hist(ch4_df["ch4_column"], bins=50, color="coral", alpha=0.7, edgecolor="black")
    mean_ch4 = ch4_df["ch4_column"].mean()
    ax2.axvline(mean_ch4, color="red", linestyle="--", lw=2, label=f"Mean: {mean_ch4:.1f} ppb")
    ax2.set(
        xlabel="CH₄ Column (ppb)",
        ylabel="Pixel Count",
        title="CH₄ Concentration Distribution (Silver)",
    )
    ax2.grid(True, alpha=0.3, axis="y")
    ax2.legend()

    plt.tight_layout()
    return save_plot(fig, "summary_statistics.png")


def create_monthly_ch4_vs_aer(con):
    # All months per h3_cell — no QUALIFY, keep the full time series
    df = con.execute("""
        SELECT reporting_month, h3_cell,
               AVG(monthly_avg_ch4) AS avg_ch4,
               SUM(total_emissions_1000m3) AS total_emissions
        FROM gold.monthly_emissions_correlation
        WHERE monthly_pixel_count IS NOT NULL
        GROUP BY reporting_month, h3_cell
        ORDER BY h3_cell, reporting_month
    """).fetchdf()

    # Top 5 h3_cells by number of months with data — gives the longest time series
    top_cells = (
        con.execute("""
            SELECT h3_cell, COUNT(DISTINCT reporting_month) AS n_months
            FROM gold.monthly_emissions_correlation
            WHERE monthly_pixel_count IS NOT NULL
              AND total_emissions_1000m3 IS NOT NULL
            GROUP BY h3_cell
            ORDER BY n_months DESC
            LIMIT 5
        """)
        .fetchdf()["h3_cell"]
        .tolist()
    )
    df_top = df[df["h3_cell"].isin(top_cells)]

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax2 = ax1.twinx()

    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for i, cell in enumerate(top_cells):
        cell_df = df_top[df_top["h3_cell"] == cell].sort_values("reporting_month")
        if cell_df.empty:
            continue
        # Use last 6 digits to guarantee uniqueness between cells
        label = f"…{str(cell)[-6:]}"
        color = colors[i % len(colors)]
        ax1.plot(cell_df["reporting_month"], cell_df["avg_ch4"], marker="o", ms=4,
                 label=label, color=color)
        ax2.plot(
            cell_df["reporting_month"],
            cell_df["total_emissions"],
            linestyle="--",
            alpha=0.5,
            color=color,
        )

    ax1.set(xlabel="Month", ylabel="Avg CH₄ (ppb)")
    ax2.set_ylabel("Total Emissions (1000 m³, dashed)", color="grey")
    ax1.set_title("Monthly CH₄ vs AER Reported Emissions — Top 5 H3 Cells")
    ax1.legend(title="H3 cell (CH₄ solid, AER dashed)", fontsize=8, loc="upper left")
    ax1.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    return save_plot(fig, "monthly_ch4_vs_aer.png")


def create_emissions_scatter(con):
    df = con.execute("""
        SELECT total_emissions_1000m3, monthly_avg_ch4, ch4_anomaly_score
        FROM gold.monthly_emissions_correlation
        WHERE total_emissions_1000m3 > 0
          AND monthly_avg_ch4 IS NOT NULL
          AND ch4_anomaly_score IS NOT NULL
        LIMIT 5000
    """).fetchdf()

    import numpy as np

    fig, ax = plt.subplots(figsize=(10, 7))
    sc = ax.scatter(
        df["total_emissions_1000m3"],
        df["monthly_avg_ch4"],
        c=df["ch4_anomaly_score"],
        cmap="RdYlGn_r",
        alpha=0.6,
        s=20,
        edgecolors="none",
    )
    plt.colorbar(sc, ax=ax, label="CH₄ Anomaly Score (z-score)")
    ax.set_xscale("log")
    ax.set(
        xlabel="Reported Emissions (1000 m³, log scale)",
        ylabel="Monthly Avg CH₄ (ppb)",
        title="Satellite CH₄ vs AER Reported Emissions",
    )
    ax.grid(True, alpha=0.3, which="both")
    return save_plot(fig, "emissions_scatter.png")


def create_anomaly_flags_chart(con):
    df = con.execute("""
        SELECT facility_id, operator, anomaly_type, COUNT(*) AS flag_count
        FROM gold.facility_anomaly_flags
        WHERE anomaly_type IS NOT NULL
        GROUP BY facility_id, operator, anomaly_type
        ORDER BY flag_count DESC
        LIMIT 45
    """).fetchdf()

    if df.empty:
        print("  No anomaly flags found — skipping chart")
        return None

    # Top 15 facilities by total flags
    top_facilities = (
        df.groupby("facility_id")["flag_count"]
        .sum()
        .nlargest(15)
        .index.tolist()
    )
    df_top = df[df["facility_id"].isin(top_facilities)]

    flag_types = ["HIGH_CH4_LOW_REPORTED", "LOW_CH4_HIGH_REPORTED", "HIGH_CH4_HIGH_REPORTED"]
    colors = {"HIGH_CH4_LOW_REPORTED": "#d62728", "LOW_CH4_HIGH_REPORTED": "#1f77b4", "HIGH_CH4_HIGH_REPORTED": "#ff7f0e"}

    fig, ax = plt.subplots(figsize=(12, 7))
    bottom = {fid: 0 for fid in top_facilities}

    for flag in flag_types:
        sub = df_top[df_top["anomaly_type"] == flag].set_index("facility_id")
        vals = [sub.loc[fid, "flag_count"] if fid in sub.index else 0 for fid in top_facilities]
        ax.barh(top_facilities, vals, left=[bottom[f] for f in top_facilities],
                label=flag, color=colors[flag], alpha=0.85)
        for i, fid in enumerate(top_facilities):
            bottom[fid] += vals[i]

    ax.set(xlabel="Number of Flagged Months", title="Anomaly Flags — Top 15 Facilities")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(True, alpha=0.3, axis="x")
    plt.tight_layout()
    return save_plot(fig, "anomaly_flags.png")


def main():
    con = duckdb.connect(DB_PATH)
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint='{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}'")
    con.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ACCESS_KEY', 'minioadmin')}'")
    con.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_SECRET_KEY', 'minioadmin')}'")
    con.execute("SET s3_url_style='path'")
    con.execute("SET s3_use_ssl=false")
    con.execute("LOAD iceberg")
    con.execute("SET unsafe_enable_version_guessing=true")
    con.execute("LOAD spatial")
    try:
        visualizations = []
        for func in [
            create_ch4_heatmap,
            create_facilities_overlay,
            create_summary_stats_plot,
            create_monthly_ch4_vs_aer,
            create_emissions_scatter,
            create_anomaly_flags_chart,
        ]:
            try:
                result = func(con)
                if result is not None:
                    visualizations.append(result)
            except Exception as e:
                print(f"ERROR in {func.__name__}: {e}")
    finally:
        con.close()

    print(f"Generated {len(visualizations)} visualization(s):")
    for v in visualizations:
        print(f"  - {v}")
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
