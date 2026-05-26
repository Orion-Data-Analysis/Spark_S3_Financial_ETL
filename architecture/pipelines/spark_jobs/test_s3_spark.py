import argparse
import os

from spark_jobs.session import build_spark_session


DEFAULT_TARGET_PATH = (
    "s3a://orion-financial-crisis-data-395840094505-us-east-2-an/"
    "dev/financial_crisis/logs/test_spark_e2e"
)


def check_s3a_class(spark) -> None:
    class_name = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.sparkContext._jvm.java.lang.Class.forName(class_name)
    print(f"OK: Spark cargo la clase {class_name}")


def run_s3_e2e(spark, target_path: str) -> None:
    data = [
        ("digital_accounts", "success", "2026-05-26"),
        ("virtual_wallets", "pending", "2026-05-26"),
    ]
    columns = ["source_system", "status", "date"]

    df = spark.createDataFrame(data, columns)

    print(f"Escribiendo dataset de prueba en {target_path}")
    df.write.mode("overwrite").parquet(target_path)

    print("Leyendo dataset de prueba desde S3")
    spark.read.parquet(target_path).show(truncate=False)
    print("OK: prueba end-to-end Spark S3A exitosa")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test para validar Spark con Hadoop AWS S3A."
    )
    parser.add_argument(
        "--s3-e2e",
        action="store_true",
        help="Ejecuta escritura y lectura real en S3 usando s3a://.",
    )
    parser.add_argument(
        "--target-path",
        default=os.getenv("SPARK_S3A_TEST_PATH", DEFAULT_TARGET_PATH),
        help="Ruta s3a:// para la prueba end-to-end.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session("Orion-Spark-S3A-Smoke-Test")

    try:
        check_s3a_class(spark)
        if args.s3_e2e:
            run_s3_e2e(spark, args.target_path)
        else:
            print("OK: validacion local completada. Use --s3-e2e en la EC2 para probar S3.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
