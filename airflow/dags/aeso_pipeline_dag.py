#section 1 imports
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# default arguments for the DAG
default_args = {
    'owner': 'Zach',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 14), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# base paths for the Python executable and project directory
PYTHON_EXEC = "/home/zawi/dev/de-roadmap-aeso/venv/bin/python"
PROJECT_DIR = "/home/zawi/dev/de-roadmap-aeso/src"

with DAG(
    'aeso_grid_pipeline',
    default_args=default_args,
    description='An end-to-end pipeline to ingest, transform, and load AESO grid data daily',
    schedule='0 2 * * *',  # cron expression for daily at 2 AM
    catchup=False,         # prevent catching up on missed runs
) as dag:
    ingest_op = BashOperator(task_id="ingest", bash_command=f"{PYTHON_EXEC} {PROJECT_DIR}/ingest.py")

    transform_op = BashOperator(task_id="transform", bash_command=f"{PYTHON_EXEC} {PROJECT_DIR}/transform.py")

    load_op = BashOperator(task_id="load", bash_command=f"{PYTHON_EXEC} {PROJECT_DIR}/load.py")

    # enforce the order of execution - Ingest -> Transform -> Load
    _ = ingest_op >> transform_op >> load_op 


