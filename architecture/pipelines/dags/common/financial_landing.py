import logging
import os

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


LOGGER = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("FINANCIAL_AWS_CONN_ID", "aws_orion_s3")
BUCKET = os.getenv(
    "FINANCIAL_S3_BUCKET",
    "orion-financial-crisis-data",
)
ENV = os.getenv("FINANCIAL_ENV", "dev")
DOMAIN = os.getenv("FINANCIAL_S3_DOMAIN", "financial_crisis")

EXPECTED_LANDING_FILES = [
    {
        "source_system": "ieee_cis_fraud_detection",
        "dataset": "train_transaction",
        "file_name": "train_transaction.csv",
        "kaggle_source": "ieee-fraud-detection",
        "kaggle_type": "competition",
    },
    {
        "source_system": "credit_card_fraud_detection",
        "dataset": "transactions",
        "file_name": "creditcard.csv",
        "kaggle_source": "mlg-ulb/creditcardfraud",
        "kaggle_type": "dataset",
    },
    {
        "source_system": "paysim",
        "dataset": "transactions",
        "file_name": "PS_20174392719_1491204439457_log.csv",
        "kaggle_source": "ealaxi/paysim1",
        "kaggle_type": "dataset",
    },
]


def build_landing_key(source_system: str, ingestion_date: str, run_id: str, file_name: str) -> str:
    return (
        f"{ENV}/{DOMAIN}/landing/"
        f"source_system={source_system}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={run_id}/"
        f"{file_name}"
    )


def build_landing_uri(source_system: str, ingestion_date: str, run_id: str, file_name: str) -> str:
    return f"s3://{BUCKET}/{build_landing_key(source_system, ingestion_date, run_id, file_name)}"


def validate_expected_landing_files(ingestion_date: str, landing_run_id: str) -> None:
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn()

    missing_files = []
    empty_files = []

    LOGGER.info(
        "Starting S3 Landing validation. bucket=%s env=%s domain=%s ingestion_date=%s run_id=%s",
        BUCKET,
        ENV,
        DOMAIN,
        ingestion_date,
        landing_run_id,
    )

    for expected_file in EXPECTED_LANDING_FILES:
        source_system = expected_file["source_system"]
        file_name = expected_file["file_name"]
        key = build_landing_key(
            source_system=source_system,
            ingestion_date=ingestion_date,
            run_id=landing_run_id,
            file_name=file_name,
        )
        s3_uri = f"s3://{BUCKET}/{key}"

        LOGGER.info(
            "Checking expected landing file. source_system=%s file_name=%s s3_uri=%s",
            source_system,
            file_name,
            s3_uri,
        )

        if not s3_hook.check_for_key(key=key, bucket_name=BUCKET):
            missing_files.append(
                {
                    "source_system": source_system,
                    "file_name": file_name,
                    "s3_uri": s3_uri,
                }
            )
            continue

        metadata = s3_client.head_object(Bucket=BUCKET, Key=key)
        size_bytes = metadata.get("ContentLength", 0)

        if size_bytes <= 0:
            empty_files.append(
                {
                    "source_system": source_system,
                    "file_name": file_name,
                    "s3_uri": s3_uri,
                    "size_bytes": size_bytes,
                }
            )

    if missing_files or empty_files:
        for item in missing_files:
            LOGGER.error(
                "[LANDING_VALIDATION_ERROR] Missing financial file in S3 Landing. "
                "source_system=%s file_name=%s s3_uri=%s",
                item["source_system"],
                item["file_name"],
                item["s3_uri"],
            )

        for item in empty_files:
            LOGGER.error(
                "[LANDING_VALIDATION_ERROR] Empty financial file in S3 Landing. "
                "source_system=%s file_name=%s s3_uri=%s size_bytes=%s",
                item["source_system"],
                item["file_name"],
                item["s3_uri"],
                item["size_bytes"],
            )

        raise AirflowFailException(
            "S3 Landing validation failed. Missing or empty financial files were found. "
            "The pipeline stops before Spark processing."
        )

    LOGGER.info(
        "[LANDING_VALIDATION_OK] All expected financial files exist and are not empty. "
        "bucket=%s ingestion_date=%s run_id=%s checked_files=%s",
        BUCKET,
        ingestion_date,
        landing_run_id,
        len(EXPECTED_LANDING_FILES),
    )
