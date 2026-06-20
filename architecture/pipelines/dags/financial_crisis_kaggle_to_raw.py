import json
import os
import re
import shutil
import subprocess
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from common.financial_landing import (
    AWS_CONN_ID,
    BUCKET,
    DOMAIN,
    ENV,
    EXPECTED_LANDING_FILES,
    build_landing_key,
    build_landing_uri,
    validate_expected_landing_files,
)


TMP_ROOT = Path(os.getenv("FINANCIAL_KAGGLE_TMP_ROOT", "/tmp/orion_financial_kaggle"))
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_standalone")
SPARK_BINARY = os.getenv("SPARK_BINARY", "/opt/spark/bin/spark-submit")
SPARK_APPLICATION = os.getenv(
    "LANDING_TO_RAW_SPARK_APPLICATION",
    "/opt/pipelines/spark_jobs/landing_to_raw_financial_crisis.py",
)
SPARK_RAW_TO_STAGING_APP = os.getenv(
    "RAW_TO_STAGING_SPARK_APPLICATION",
    "/opt/pipelines/spark_jobs/raw_to_staging_financial_crisis.py",
)
SPARK_STAGING_TO_INTERMEDIATE_APP = os.getenv(
    "STAGING_TO_INTERMEDIATE_SPARK_APPLICATION",
    "/opt/pipelines/spark_jobs/raw_to_staging_financial_crisis.py",
)
SPARK_INTERMEDIATE_TO_MART_APP = os.getenv(
    "INTERMEDIATE_TO_MART_SPARK_APPLICATION",
    "/opt/pipelines/spark_jobs/intermediate_to_mart_financial_crisis.py",
)
SPARK_PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
)


def safe_path_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value)


def build_landing_context(**context) -> dict:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    ingestion_date = conf.get("ingestion_date") or context["ds"]
    landing_run_id = conf.get("landing_run_id") or conf.get("run_id") or context["run_id"]
    work_dir = TMP_ROOT / safe_path_token(landing_run_id)

    return {
        "ingestion_date": ingestion_date,
        "landing_run_id": landing_run_id,
        "work_dir": str(work_dir),
    }


def configure_kaggle_credentials() -> None:
    api_token = Variable.get("kaggle_api_token", default_var=os.getenv("KAGGLE_API_TOKEN"))
    username = Variable.get("kaggle_username", default_var=os.getenv("KAGGLE_USERNAME"))
    key = Variable.get("kaggle_key", default_var=os.getenv("KAGGLE_KEY"))

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)

    if api_token:
        access_token = kaggle_dir / "access_token"
        access_token.write_text(api_token, encoding="utf-8")
        os.chmod(access_token, 0o600)
        return

    if not username or not key:
        raise AirflowFailException(
            "Kaggle credentials were not found. Configure Airflow Variable "
            "'kaggle_api_token' for the new Kaggle token format, or configure "
            "'kaggle_username' and 'kaggle_key' for the legacy kaggle.json format."
        )

    kaggle_json = kaggle_dir / "kaggle.json"
    kaggle_json.write_text(
        json.dumps({"username": username, "key": key}),
        encoding="utf-8",
    )
    os.chmod(kaggle_json, 0o600)


def download_unzip_upload_sources_to_landing(**context) -> list[dict]:
    # 1. Asegurar credenciales físicas y entorno en este ejecutor
    api_token = Variable.get("kaggle_api_token", default_var=os.getenv("KAGGLE_API_TOKEN"))
    username = Variable.get("kaggle_username", default_var=os.getenv("KAGGLE_USERNAME"))
    key = Variable.get("kaggle_key", default_var=os.getenv("KAGGLE_KEY"))

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)

    env_origen = os.environ.copy()

    if api_token:
        access_token_file = kaggle_dir / "access_token"
        access_token_file.write_text(api_token, encoding="utf-8")
        os.chmod(access_token_file, 0o600)
        env_origen["KAGGLE_API_TOKEN"] = api_token
    elif username and key:
        kaggle_json = kaggle_dir / "kaggle.json"
        kaggle_json.write_text(json.dumps({"username": username, "key": key}), encoding="utf-8")
        os.chmod(kaggle_json, 0o600)
        env_origen["KAGGLE_USERNAME"] = username
        env_origen["KAGGLE_KEY"] = key
    else:
        raise AirflowFailException("No se encontraron credenciales de Kaggle.")

    # 2. Continuar con la descarga normal
    landing_context = context["ti"].xcom_pull(task_ids="build_landing_context")
    work_dir = Path(landing_context["work_dir"])
    ingestion_date = landing_context["ingestion_date"]
    landing_run_id = landing_context["landing_run_id"]

    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    source_dirs = {}
    downloaded_sources = set()
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    uploaded_files = []

    for expected_file in EXPECTED_LANDING_FILES:
        source_system = expected_file["source_system"]

        source_dir = work_dir / source_system
        source_dir.mkdir(parents=True, exist_ok=True)

        if source_system not in downloaded_sources:
            kaggle_source = expected_file["kaggle_source"]

            if expected_file["kaggle_type"] == "competition":
                command = [
                    "kaggle",
                    "competitions",
                    "download",
                    "-c",
                    kaggle_source,
                    "-p",
                    str(source_dir),
                    "--force",
                ]
            else:
                command = [
                    "kaggle",
                    "datasets",
                    "download",
                    "-d",
                    kaggle_source,
                    "-p",
                    str(source_dir),
                    "--force",
                ]

            subprocess.run(command, check=True, env=env_origen)
            source_dirs[source_system] = str(source_dir)
            downloaded_sources.add(source_system)

            for zip_path in source_dir.glob("*.zip"):
                with zipfile.ZipFile(zip_path, "r") as archive:
                    archive.extractall(source_dir)

        file_name = expected_file["file_name"]
        local_path = find_required_file(source_dir, file_name)
        key = build_landing_key(source_system, ingestion_date, landing_run_id, file_name)

        s3_hook.load_file(
            filename=str(local_path),
            key=key,
            bucket_name=BUCKET,
            replace=True,
        )

        uploaded_files.append(
            {
                "source_system": source_system,
                "dataset": expected_file["dataset"],
                "file_name": file_name,
                "size_bytes": local_path.stat().st_size,
                "landing_key": key,
                "landing_uri": build_landing_uri(
                    source_system,
                    ingestion_date,
                    landing_run_id,
                    file_name,
                ),
            }
        )

    shutil.rmtree(work_dir, ignore_errors=True)
    return uploaded_files


def find_required_file(source_dir: Path, file_name: str) -> Path:
    matches = list(source_dir.rglob(file_name))
    if not matches:
        raise AirflowFailException(
            f"Required Kaggle file was not found after download/unzip. "
            f"source_dir={source_dir} file_name={file_name}"
        )

    return matches[0]


def generate_landing_manifest(**context) -> dict:
    landing_context = context["ti"].xcom_pull(task_ids="build_landing_context")
    uploaded_files = context["ti"].xcom_pull(task_ids="download_unzip_upload_sources_to_landing")
    ingestion_date = landing_context["ingestion_date"]
    landing_run_id = landing_context["landing_run_id"]

    manifest_key = (
        f"{ENV}/{DOMAIN}/landing/_manifests/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={landing_run_id}/"
        "landing_manifest.json"
    )
    manifest_uri = f"s3://{BUCKET}/{manifest_key}"
    manifest = {
        "layer": "landing",
        "domain": DOMAIN,
        "environment": ENV,
        "bucket": BUCKET,
        "ingestion_date": ingestion_date,
        "run_id": landing_run_id,
        "source": "kaggle",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": uploaded_files,
    }

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_string(
        string_data=json.dumps(manifest, indent=2),
        key=manifest_key,
        bucket_name=BUCKET,
        replace=True,
    )

    return {"manifest_key": manifest_key, "manifest_uri": manifest_uri}


def generate_raw_manifest(**context) -> dict:
    landing_context = context["ti"].xcom_pull(task_ids="build_landing_context")
    ingestion_date = landing_context["ingestion_date"]
    run_id = landing_context["landing_run_id"]

    sources = []
    for expected_file in EXPECTED_LANDING_FILES:
        source_system = expected_file["source_system"]
        dataset = expected_file["dataset"]
        file_name = expected_file["file_name"]
        sources.append(
            {
                "source_system": source_system,
                "dataset": dataset,
                "format": "parquet",
                "landing_path": build_landing_uri(source_system, ingestion_date, run_id, file_name),
                "raw_path": (
                    f"s3://{BUCKET}/{ENV}/{DOMAIN}/raw/"
                    f"{source_system}/{dataset}/"
                    f"ingestion_date={ingestion_date}/"
                    f"run_id={run_id}/"
                ),
                "status": "loaded",
            }
        )

    manifest_key = (
        f"{ENV}/{DOMAIN}/raw/_manifests/"
        f"raw_manifest_ingestion_date={ingestion_date}_run_id={run_id}.json"
    )
    manifest = {
        "layer": "raw",
        "domain": DOMAIN,
        "environment": ENV,
        "bucket": BUCKET,
        "ingestion_date": ingestion_date,
        "run_id": run_id,
        "format": "parquet",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
    }

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_hook.load_string(
        string_data=json.dumps(manifest, indent=2),
        key=manifest_key,
        bucket_name=BUCKET,
        replace=True,
    )

    manifest_uri = f"s3://{BUCKET}/{manifest_key}"
    return {"manifest_key": manifest_key, "manifest_uri": manifest_uri}


def validate_landing_files_from_context(**context) -> None:
    landing_context = context["ti"].xcom_pull(task_ids="build_landing_context")
    validate_expected_landing_files(
        ingestion_date=landing_context["ingestion_date"],
        landing_run_id=landing_context["landing_run_id"],
    )


with DAG(
    dag_id="financial_crisis_kaggle_to_raw",
    description="Download Kaggle financial datasets, load S3 Landing, validate, and submit Spark Raw ingestion.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
    },
    tags=["financial_crisis", "kaggle", "landing", "raw", "spark"],
) as dag:
    build_landing_context_task = PythonOperator(
        task_id="build_landing_context",
        python_callable=build_landing_context,
    )

    configure_kaggle_credentials_task = PythonOperator(
        task_id="configure_kaggle_credentials",
        python_callable=configure_kaggle_credentials,
    )

    download_unzip_upload_sources_to_landing_task = PythonOperator(
        task_id="download_unzip_upload_sources_to_landing",
        python_callable=download_unzip_upload_sources_to_landing,
        execution_timeout=timedelta(hours=2),
    )

    generate_landing_manifest_task = PythonOperator(
        task_id="generate_landing_manifest",
        python_callable=generate_landing_manifest,
        execution_timeout=timedelta(minutes=10),
    )

    validate_landing_files_task = PythonOperator(
        task_id="validate_landing_files",
        python_callable=validate_landing_files_from_context,
        execution_timeout=timedelta(minutes=15),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    spark_landing_to_raw_task = SparkSubmitOperator(
        task_id="spark_landing_to_raw",
        conn_id=SPARK_CONN_ID,
        spark_binary=SPARK_BINARY,
        application=SPARK_APPLICATION,
        packages=SPARK_PACKAGES,
        application_args=[
            "--bucket",
            BUCKET,
            "--env",
            ENV,
            "--domain",
            DOMAIN,
            "--ingestion-date",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['ingestion_date'] }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['landing_run_id'] }}",
        ],
        conf={
            "spark.driver.port": "7079",
            "spark.blockManager.port": "7080",
            "spark.ui.port": "4040",
        },
        verbose=True,
    )

    generate_raw_manifest_task = PythonOperator(
        task_id="generate_raw_manifest",
        python_callable=generate_raw_manifest,
        execution_timeout=timedelta(minutes=10),
    )

    spark_raw_to_staging_task = SparkSubmitOperator(
        task_id="spark_raw_to_staging",
        conn_id=SPARK_CONN_ID,
        spark_binary=SPARK_BINARY,
        application=SPARK_RAW_TO_STAGING_APP,
        packages=SPARK_PACKAGES,
        application_args=[
            "--bucket",
            BUCKET,
            "--env",
            ENV,
            "--domain",
            DOMAIN,
            "--ingestion-date",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['ingestion_date'] }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['landing_run_id'] }}",
        ],
        conf={
            "spark.driver.port": "7079",
            "spark.blockManager.port": "7080",
            "spark.ui.port": "4040",
        },
        verbose=True,
    )

    spark_staging_to_intermediate_task = SparkSubmitOperator(
        task_id="spark_staging_to_intermediate",
        conn_id=SPARK_CONN_ID,
        spark_binary=SPARK_BINARY,
        application=SPARK_STAGING_TO_INTERMEDIATE_APP,
        packages=SPARK_PACKAGES,
        application_args=[
            "--bucket",
            BUCKET,
            "--env",
            ENV,
            "--domain",
            DOMAIN,
            "--ingestion-date",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['ingestion_date'] }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['landing_run_id'] }}",
        ],
        conf={
            "spark.driver.port": "7079",
            "spark.blockManager.port": "7080",
            "spark.ui.port": "4040",
        },
        verbose=True,
    )

    spark_intermediate_to_mart_task = SparkSubmitOperator(
        task_id="spark_intermediate_to_mart",
        conn_id=SPARK_CONN_ID,
        spark_binary=SPARK_BINARY,
        application=SPARK_INTERMEDIATE_TO_MART_APP,
        packages=SPARK_PACKAGES,
        application_args=[
            "--bucket",
            BUCKET,
            "--env",
            ENV,
            "--domain",
            DOMAIN,
            "--ingestion-date",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['ingestion_date'] }}",
            "--run-id",
            "{{ ti.xcom_pull(task_ids='build_landing_context')['landing_run_id'] }}",
        ],
        conf={
            "spark.driver.port": "7079",
            "spark.blockManager.port": "7080",
            "spark.ui.port": "4040",
        },
        verbose=True,
    )

    (
        build_landing_context_task
        >> configure_kaggle_credentials_task
        >> download_unzip_upload_sources_to_landing_task
        >> generate_landing_manifest_task
        >> validate_landing_files_task
        >> spark_landing_to_raw_task
        >> generate_raw_manifest_task
        >> spark_raw_to_staging_task
        >> spark_staging_to_intermediate_task
        >> spark_intermediate_to_mart_task
    )
