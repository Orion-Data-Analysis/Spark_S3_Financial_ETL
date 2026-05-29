import argparse

from pyspark.sql import functions as F

from spark_jobs.session import build_spark_session


SOURCES = [
    {
        "source_system": "ieee_cis_fraud_detection",
        "dataset": "train_transaction",
        "file_name": "train_transaction.csv",
    },
    {
        "source_system": "credit_card_fraud_detection",
        "dataset": "transactions",
        "file_name": "creditcard.csv",
    },
    {
        "source_system": "paysim",
        "dataset": "transactions",
        "file_name": "PS_20174392719_1491204439457_log.csv",
    },
]


def landing_path(bucket: str, env: str, domain: str, source_system: str, ingestion_date: str, run_id: str, file_name: str) -> str:
    return (
        f"s3a://{bucket}/{env}/{domain}/landing/"
        f"source_system={source_system}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={run_id}/"
        f"{file_name}"
    )


def raw_path(bucket: str, env: str, domain: str, source_system: str, dataset: str, ingestion_date: str, run_id: str) -> str:
    return (
        f"s3a://{bucket}/{env}/{domain}/raw/"
        f"{source_system}/{dataset}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={run_id}/"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load financial CSV files from S3 Landing to S3 Raw as Parquet."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--ingestion-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append", "errorifexists", "ignore"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session("Orion-Landing-To-Raw-Financial-Crisis")

    try:
        for source in SOURCES:
            input_path = landing_path(
                bucket=args.bucket,
                env=args.env,
                domain=args.domain,
                source_system=source["source_system"],
                ingestion_date=args.ingestion_date,
                run_id=args.run_id,
                file_name=source["file_name"],
            )
            output_path = raw_path(
                bucket=args.bucket,
                env=args.env,
                domain=args.domain,
                source_system=source["source_system"],
                dataset=source["dataset"],
                ingestion_date=args.ingestion_date,
                run_id=args.run_id,
            )

            print(f"Reading Landing CSV: {input_path}")
            df = (
                spark.read.option("header", "true")
                .option("inferSchema", "false")
                .option("mode", "PERMISSIVE")
                .csv(input_path)
            )

            enriched_df = (
                df.withColumn("source_system", F.lit(source["source_system"]))
                .withColumn("raw_dataset", F.lit(source["dataset"]))
                .withColumn("source_file_name", F.lit(source["file_name"]))
                .withColumn("landing_path", F.lit(input_path))
                .withColumn("ingestion_date", F.lit(args.ingestion_date))
                .withColumn("run_id", F.lit(args.run_id))
                .withColumn("raw_ingestion_time", F.current_timestamp())
            )

            print(f"Writing Raw Parquet: {output_path}")
            enriched_df.write.mode(args.write_mode).parquet(output_path)

        print("OK: Landing to Raw Spark job completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
