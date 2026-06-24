import argparse
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from spark_jobs.session import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute Gold layer enriched datasets from clean Staging events."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--ingestion-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--write-mode", default="overwrite")
    return parser.parse_args()


def compute_customer_risk_profile(df: DataFrame) -> DataFrame:
    # Filter out null origin accounts (like Credit Card data without explicit account IDs)
    filtered_df = df.filter(F.col("origin_account_id").isNotNull())

    profile_df = filtered_df.groupBy("origin_account_id").agg(
        F.sum("amount").alias("total_transaction_amount"),
        F.count("event_id").alias("transaction_count"),
        F.avg("amount").alias("average_transaction_amount"),
        F.sum("is_fraud").alias("total_fraud_events"),
        F.coalesce(F.sum("is_fraud") / F.count("event_id"), F.lit(0.0)).alias("fraud_rate")
    )
    return profile_df


def compute_transaction_velocity(df: DataFrame) -> DataFrame:
    # Filter out null origin accounts
    filtered_df = df.filter(F.col("origin_account_id").isNotNull())

    # Window definitions using event_timestamp cast to epoch seconds
    # Range is in seconds: -3600 is 1 hour ago, -86400 is 24 hours ago
    time_col = F.col("event_timestamp").cast("long")
    window_1h = Window.partitionBy("origin_account_id").orderBy(time_col).rangeBetween(-3600, 0)
    window_24h = Window.partitionBy("origin_account_id").orderBy(time_col).rangeBetween(-86400, 0)

    velocity_df = filtered_df.select(
        F.col("event_id"),
        F.col("source_system"),
        F.col("transaction_id"),
        F.col("origin_account_id"),
        F.col("event_timestamp"),
        F.col("amount"),
        F.col("is_fraud"),
        F.count("event_id").over(window_1h).alias("tx_count_1h"),
        F.sum("amount").over(window_1h).alias("tx_amount_1h"),
        F.count("event_id").over(window_24h).alias("tx_count_24h"),
        F.sum("amount").over(window_24h).alias("tx_amount_24h")
    )
    return velocity_df


def register_table_in_catalog(spark, df: DataFrame, db_name: str, table_name: str, s3_path: str, write_mode: str) -> None:
    try:
        print(f"Registering table {db_name}.{table_name} in Glue Catalog...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.write.mode(write_mode).option("path", s3_path).saveAsTable(f"{db_name}.{table_name}")
        print(f"Table {db_name}.{table_name} registered successfully.")
    except Exception as e:
        print(f"Warning: Could not register table in Glue Catalog: {e}. Writing to S3 directly...")


def main() -> None:
    args = parse_args()
    spark = build_spark_session("Orion-Staging-To-Intermediate-Financial-Crisis")

    try:
        # Read clean staging events
        staging_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/staging/financial_fraud_events/"
        print(f"Reading staging events from: {staging_path}")

        # Spark reads the partitioned dataset and we prune partitions using filter
        staging_df = spark.read.parquet(staging_path).filter(
            (F.col("ingestion_date") == args.ingestion_date) & 
            (F.col("run_id") == args.run_id) & 
            (F.col("quality_status") == "valid")
        )

        # 1. Compute Customer Risk Profile
        risk_profile_df = compute_customer_risk_profile(staging_df)
        risk_profile_df = risk_profile_df.withColumn("ingestion_date", F.lit(args.ingestion_date))
        risk_profile_df = risk_profile_df.withColumn("run_id", F.lit(args.run_id))

        # Save Customer Risk Profile to S3
        profile_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/intermediate/int_customer_risk_profile/"
        print(f"Writing customer risk profile to: {profile_path}")
        risk_profile_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(profile_path)
        
        # Register in Glue Catalog if possible
        db_name = f"financial_crisis_{args.env}_intermediate"
        register_table_in_catalog(
            spark=spark,
            df=risk_profile_df,
            db_name=db_name,
            table_name="int_customer_risk_profile",
            s3_path=profile_path,
            write_mode=args.write_mode
        )

        # 2. Compute Transaction Velocity
        velocity_df = compute_transaction_velocity(staging_df)
        velocity_df = velocity_df.withColumn("ingestion_date", F.lit(args.ingestion_date))
        velocity_df = velocity_df.withColumn("run_id", F.lit(args.run_id))

        # Save Transaction Velocity to S3
        velocity_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/intermediate/int_transaction_velocity/"
        print(f"Writing transaction velocity to: {velocity_path}")
        velocity_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(velocity_path)

        # Register in Glue Catalog if possible
        register_table_in_catalog(
            spark=spark,
            df=velocity_df,
            db_name=db_name,
            table_name="int_transaction_velocity",
            s3_path=velocity_path,
            write_mode=args.write_mode
        )

        print("OK: Staging to Intermediate Spark job completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
