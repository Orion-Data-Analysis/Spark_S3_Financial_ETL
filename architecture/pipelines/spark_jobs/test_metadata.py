# Correr con: PYTHONPATH=. python -m pytest spark_jobs/test_metadata.py -v
import pytest
from pyspark.sql import SparkSession
from spark_jobs.metadata import (
    METADATA_COLUMNS,
    inject_metadata,
    validate_metadata_columns,
)


def get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-metadata")
        .getOrCreate()
    )


def test_inject_agrega_todas_las_columnas():
    spark = get_spark()
    df = spark.createDataFrame([("A", 1)], ["col_original", "valor"])

    result = inject_metadata(
        df=df,
        source_system="ieee_cis_fraud_detection",
        raw_dataset="train_transaction",
        source_file_name="train_transaction.csv",
        landing_path="s3a://bucket/landing/train_transaction.csv",
        ingestion_date="2026-06-01",
        run_id="run_001",
    )

    # Todas las columnas de metadata deben estar presentes
    for col in METADATA_COLUMNS:
        assert col in result.columns, f"Falta columna: {col}"

    # Los valores deben ser los que pasamos
    row = result.first()
    assert row["source_system"]  == "ieee_cis_fraud_detection"
    assert row["ingestion_date"] == "2026-06-01"
    assert row["run_id"]         == "run_001"

    # Las columnas originales no se deben perder
    assert row["col_original"] == "A"
    spark.stop()


def test_validate_pasa_con_todas_las_columnas():
    spark = get_spark()
    df = spark.createDataFrame([("x",)], ["col"])

    enriched = inject_metadata(
        df=df,
        source_system="paysim",
        raw_dataset="transactions",
        source_file_name="PS_log.csv",
        landing_path="s3a://bucket/paysim.csv",
        ingestion_date="2026-06-01",
        run_id="run_001",
    )

    # No debe lanzar ninguna excepción
    validate_metadata_columns(enriched, "paysim")
    spark.stop()


def test_validate_falla_si_falta_columna():
    spark = get_spark()

    # DataFrame sin pasar por inject_metadata — solo tiene una columna
    df = spark.createDataFrame([("x",)], ["source_system"])

    with pytest.raises(ValueError, match="SCRUM-78"):
        validate_metadata_columns(df, "paysim")

    spark.stop()