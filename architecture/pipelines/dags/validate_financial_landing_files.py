from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.financial_landing import validate_expected_landing_files


def get_landing_context(context: dict) -> tuple[str, str]:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    ingestion_date = conf.get("ingestion_date") or context["ds"]
    landing_run_id = conf.get("landing_run_id") or conf.get("run_id") or context["run_id"]

    return ingestion_date, landing_run_id


def validate_landing_files(**context) -> None:
    ingestion_date, landing_run_id = get_landing_context(context)
    validate_expected_landing_files(ingestion_date, landing_run_id)


with DAG(
    dag_id="validate_financial_landing_files",
    description="Validate required financial CSV files in S3 Landing before Spark Raw ingestion.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
    },
    tags=["financial_crisis", "s3", "landing", "raw", "validation"],
) as dag:
    validate_landing_files_task = PythonOperator(
        task_id="validate_landing_files",
        python_callable=validate_landing_files,
        execution_timeout=timedelta(minutes=15),
    )
