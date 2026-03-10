import os
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

OUTPUT_DIR = Path("./outputs/visualizations")
DB_PATH = os.getenv('DUCKDB_DATABASE_PATH', './emissions_ghg.duckdb')


def save_plot(fig, filename):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    return path


def create_ch4_heatmap():
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT latitude, longitude, ch4_column
        FROM bronze.sentinel5p_raw
        WHERE qa_value >= 0.5
    """).fetchdf()
    con.close()

    df['lat_grid'] = (df['latitude'] / 0.05).round() * 0.05
    df['lon_grid'] = (df['longitude'] / 0.05).round() * 0.05
    grid = df.groupby(['lat_grid', 'lon_grid'])['ch4_column'].mean().reset_index()

    fig, ax = plt.subplots(figsize=(12, 10))
    sc = ax.scatter(grid['lon_grid'], grid['lat_grid'], c=grid['ch4_column'],
                    cmap='RdYlBu_r', s=50, alpha=0.7, edgecolors='none')
    plt.colorbar(sc, ax=ax, label='CH₄ Column (ppb)')
    ax.set(xlabel='Longitude (°)', ylabel='Latitude (°)',
           title='Sentinel-5P CH₄ Concentrations - Alberta')
    ax.plot([-120, -110, -110, -120, -120], [49, 49, 60, 60, 49], 'k--', lw=2)
    ax.grid(True, alpha=0.3)
    return save_plot(fig, "ch4_heatmap.png")


def create_facilities_overlay():
    con = duckdb.connect(DB_PATH)
    ch4_df = con.execute("""
        SELECT ROUND(latitude,1) AS lat_grid, ROUND(longitude,1) AS lon_grid,
               AVG(ch4_column) AS avg_ch4
        FROM bronze.sentinel5p_raw
        WHERE qa_value >= 0.5
        GROUP BY lat_grid, lon_grid
    """).fetchdf()
    facilities_df = con.execute("""
        SELECT latitude, longitude
        FROM bronze.aer_battery_monthly
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT 1000
    """).fetchdf()
    con.close()

    fig, ax = plt.subplots(figsize=(14, 12))
    sc = ax.scatter(ch4_df['lon_grid'], ch4_df['lat_grid'], c=ch4_df['avg_ch4'],
                    cmap='YlOrRd', s=100, alpha=0.6, edgecolors='none', label='CH₄')
    ax.scatter(facilities_df['longitude'], facilities_df['latitude'], c='blue', marker='x', s=30,
               alpha=0.7,
               label='AER Facilities')
    plt.colorbar(sc, ax=ax, label='Avg CH₄ (ppb)')
    ax.set(xlabel='Longitude (°)', ylabel='Latitude (°)',
           title='CH₄ Concentrations with O&G Facilities - Alberta')
    ax.grid(True, alpha=0.3)
    ax.legend()
    return save_plot(fig, "ch4_facilities_overlay.png")


def create_summary_stats_plot():
    con = duckdb.connect(DB_PATH)
    qa_df = con.execute("""
        SELECT ROUND(qa_value,1) AS qa_bin, COUNT(*) AS count
        FROM bronze.sentinel5p_raw
        GROUP BY qa_bin
        ORDER BY qa_bin
    """).fetchdf()
    ch4_df = con.execute("""
        SELECT ch4_column
        FROM bronze.sentinel5p_raw
        WHERE ch4_column BETWEEN 1700 AND 2100
    """).fetchdf()
    con.close()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    ax1.bar(qa_df['qa_bin'], qa_df['count'], width=0.08, color='steelblue', alpha=0.7)
    ax1.set(xlabel='QA Value', ylabel='Pixel Count', title='QA Value Distribution')
    ax1.axvline(0.5, color='red', linestyle='--', lw=2, label='High QA Threshold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.legend()

    ax2.hist(ch4_df['ch4_column'], bins=50, color='coral', alpha=0.7, edgecolor='black')
    mean_ch4 = ch4_df['ch4_column'].mean()
    ax2.axvline(mean_ch4, color='red', linestyle='--', lw=2, label=f'Mean: {mean_ch4:.1f} ppb')
    ax2.set(xlabel='CH₄ Column (ppb)', ylabel='Pixel Count', title='CH₄ Concentration Distribution')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.legend()

    plt.tight_layout()
    return save_plot(fig, "summary_statistics.png")


def main():
    visualizations = []
    for func in [create_ch4_heatmap, create_facilities_overlay, create_summary_stats_plot]:
        try:
            visualizations.append(func())
        except Exception as e:
            print(f"ERROR in {func.__name__}: {e}")
    print(f"Generated {len(visualizations)} visualization(s):")
    for v in visualizations:
        print(f"  - {v}")
    return True


if __name__ == "__main__":
    import sys
    import traceback
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
