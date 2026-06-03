from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# SCRUM-76: Esquema oficial de metadata técnica.
# Estas columnas se agregan a TODOS los DataFrames antes de escribir en Raw.
# Si necesitas agregar una columna nueva, hacerlo aquí garantiza que se
# aplique a todas las fuentes por igual.
METADATA_SCHEMA = {
    "source_system":      "Sistema de origen (ej. ieee_cis_fraud_detection).",
    "raw_dataset":        "Nombre del dataset en la capa Raw (ej. train_transaction).",
    "source_file_name":   "Nombre del archivo CSV original descargado de Kaggle.",
    "landing_path":       "Ruta s3a:// completa del archivo en S3 Landing.",
    "ingestion_date":     "Fecha de ingesta en formato YYYY-MM-DD.",
    "run_id":             "Identificador único del DAG run de Airflow.",
    "raw_ingestion_time": "Timestamp UTC en que Spark escribió el registro en Raw.",
}

METADATA_COLUMNS = list(METADATA_SCHEMA.keys())


def inject_metadata(
    df: DataFrame,
    source_system: str,
    raw_dataset: str,
    source_file_name: str,
    landing_path: str,
    ingestion_date: str,
    run_id: str,
) -> DataFrame:
    """
    Agrega columnas de control técnico a un DataFrame de Spark.

    Estas columnas garantizan trazabilidad completa de cada registro
    desde su origen en Landing hasta la capa Raw.
    """
    return (
        df
        .withColumn("source_system",      F.lit(source_system))
        .withColumn("raw_dataset",        F.lit(raw_dataset))
        .withColumn("source_file_name",   F.lit(source_file_name))
        .withColumn("landing_path",       F.lit(landing_path))
        .withColumn("ingestion_date",     F.lit(ingestion_date))
        .withColumn("run_id",             F.lit(run_id))
        .withColumn("raw_ingestion_time", F.current_timestamp())
    )


def validate_metadata_columns(df: DataFrame, source_system: str) -> None:
    """
    Valida que el DataFrame tenga todas las columnas de metadata esperadas.

    Llama a esta función después de inject_metadata() y antes de escribir
    el Parquet en S3 Raw. Si falta alguna columna lanza ValueError para
    detener el job con un mensaje claro en lugar de escribir datos incompletos.
    """
    existing = set(df.columns)
    missing  = [col for col in METADATA_COLUMNS if col not in existing]

    if missing:
        raise ValueError(
            f"[SCRUM-78] Columnas de metadata ausentes en {source_system}: "
            f"{missing}. Verificar inject_metadata()."
        )

    print(
        f"[METADATA_OK] {source_system}: "
        f"todas las {len(METADATA_COLUMNS)} columnas de metadata presentes."
    )