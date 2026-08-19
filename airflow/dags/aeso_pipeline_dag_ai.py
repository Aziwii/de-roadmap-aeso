"""
airflow/dags/aeso_pipeline_dag_ai.py
Orchestrates Ingest -> PySpark Transform -> DB Load using custom PostgreSQL credentials.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from dotenv import load_dotenv
from airflow.providers.standard.operators.bash import BashOperator


# Base Project Path
BASE_DIR = os.getenv("AESO_PROJECT_DIR", "/home/zawi/dev/de-roadmap-aeso")
VENV_PYTHON = os.path.join(BASE_DIR, "venv/bin/python")

# Explicitly load .env from the project directory
dotenv_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# Extract API key safely (fallback to empty string if missing)
AESO_API_KEY = os.getenv("AESO_API_KEY", "")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="aeso_battery_arbitrage_pipeline_ai",
    default_args=default_args,
    description="AESO Ingestion, PySpark ETL and Battery Arbitrage Calculation",
    schedule="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aeso", "battery", "pyspark", "postgres"],
) as dag:

    # 1. Ingest Task (AESO API -> data/raw)
    task_ingest = BashOperator(
        task_id="task_ingest_aeso_api",
        bash_command=f"cd {BASE_DIR} && {VENV_PYTHON} {os.path.join(BASE_DIR, 'src/ingest_ai.py')}",
        env={
            "AESO_API_KEY": str(AESO_API_KEY),
            "RAW_DATA_DIR": os.path.join(BASE_DIR, "data/raw"),
            "PATH": f"{os.path.join(BASE_DIR, 'venv/bin')}:{os.environ.get('PATH', '')}",
        },
        append_env=True,
    )

    # 2. PySpark Transform Task (data/raw -> data/clean)
    task_transform = BashOperator(
        task_id="task_pyspark_transform",
        bash_command=f"cd {BASE_DIR} && {VENV_PYTHON} {os.path.join(BASE_DIR, 'src/transform_ai.py')}",
        env={
            "RAW_DATA_DIR": os.path.join(BASE_DIR, "data/raw"),
            "CLEAN_DATA_DIR": os.path.join(BASE_DIR, "data/clean"),
            "PATH": f"{os.path.join(BASE_DIR, 'venv/bin')}:{os.environ.get('PATH', '')}",
        },
        append_env=True,
    )

    # 3. Load Task (data/clean -> Postgres Silver Tables)
    task_load = BashOperator(
        task_id="task_postgres_load",
        bash_command=f"cd {BASE_DIR} && {VENV_PYTHON} {os.path.join(BASE_DIR, 'src/load_ai.py')}",
        env={
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_USER": "aeso_user",
            "POSTGRES_PASSWORD": "aeso_secure_password",
            "POSTGRES_DB": "aeso_market_db",
            "CLEAN_DATA_DIR": os.path.join(BASE_DIR, "data/clean"),
            "PATH": f"{os.path.join(BASE_DIR, 'venv/bin')}:{os.environ.get('PATH', '')}",
        },
        append_env=True,
    )

    # Pipeline sequence
    _ = task_ingest >> task_transform >> task_load