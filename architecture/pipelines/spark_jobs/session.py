import os
from typing import Mapping, Optional

from pyspark.sql import SparkSession


DEFAULT_APP_NAME = "Orion-Financial-Crisis-DataLake-Ingestion"


def build_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    extra_conf: Optional[Mapping[str, str]] = None,
) -> SparkSession:
    region = os.getenv("FINANCIAL_AWS_REGION", "us-east-2")
    endpoint = os.getenv("SPARK_S3A_ENDPOINT", f"s3.{region}.amazonaws.com")

    conf = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.endpoint": endpoint,
    }

    if extra_conf:
        conf.update(extra_conf)

    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
