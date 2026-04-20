import sys
import calendar
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path("/opt/airflow/project")
sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

DBT_DIR = PROJECT_ROOT / "dbt_emissions_ghg"
DB_PATH = str(PROJECT_ROOT / "emissions_ghg.duckdb")

default_args = {
    "owner": "ghg",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def load_aer_task(**context):
    exec_date = context["execution_date"]
    csv_name = f"ST60_{exec_date.year}-{exec_date.month:02d}.csv"
    csv_path = PROJECT_ROOT / "data" / "raw" / csv_name
    if not csv_path.exists():
        print(f"AER file not found: {csv_name} — skipping")
        return
    from scripts.ingest.load_aer_facilities import load_aer_data

    load_aer_data(str(csv_path), db_path=DB_PATH)


def download_s5p_task(**context):
    exec_date = context["execution_date"]
    year, month = exec_date.year, exec_date.month
    start = datetime(year, month, 1)
    end = datetime(year, month, calendar.monthrange(year, month)[1])
    from scripts.ingest.download_sentinel5p import main as download_main

    download_main(start_date=start, end_date=end)


def process_netcdf_task(**context):
    from scripts.ingest.process_netcdf_to_bronze import main as process_main

    process_main()


def load_s5p_task(**context):
    from scripts.ingest.load_sentinel5p_to_bronze import load_to_bronze

    load_to_bronze()


with DAG(
    "ghg_pipeline",
    default_args=default_args,
    description="GHG Emissions: Bronze ingestion → dbt Silver/Gold → tests",
    schedule_interval="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["ghg", "emissions", "dbt"],
) as dag:
    load_aer = PythonOperator(
        task_id="load_aer_bronze",
        python_callable=load_aer_task,
        pool="duckdb_pool",
    )

    download_s5p = PythonOperator(
        task_id="download_sentinel5p",
        python_callable=download_s5p_task,
    )

    process_s5p = PythonOperator(
        task_id="process_netcdf_to_bronze",
        python_callable=process_netcdf_task,
    )

    load_s5p = PythonOperator(
        task_id="load_sentinel5p_to_bronze",
        python_callable=load_s5p_task,
        pool="duckdb_pool",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command=(
            f"cd {DBT_DIR} && dbt run --no-partial-parse"
            f" --models silver gold --profiles-dir {DBT_DIR}"
        ),
        pool="duckdb_pool",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
        pool="duckdb_pool",
    )

    download_s5p >> process_s5p
    [load_aer, process_s5p] >> load_s5p >> dbt_run >> dbt_test
