import argparse
import json
import boto3
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark_jobs.session import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform raw financial Parquet files to clean Staging layer."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--ingestion-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--write-mode", default="overwrite")
    return parser.parse_args()


def get_s3_client(region: str = "us-east-2"):
    return boto3.client("s3", region_name=region)


def write_quality_report(bucket: str, key: str, report: dict) -> None:
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2),
        ContentType="application/json"
    )
    print(f"Quality report uploaded to s3://{bucket}/{key}")


def process_paysim(spark, bucket: str, env: str, domain: str, ingestion_date: str, run_id: str) -> DataFrame:
    path = f"s3a://{bucket}/{env}/{domain}/raw/paysim/transactions/ingestion_date={ingestion_date}/run_id={run_id}/"
    print(f"Processing PaySim Raw from: {path}")
    raw_df = spark.read.parquet(path)

    # Cast step to int and calculate event_timestamp starting from ingestion_date at 00:00:00
    df = raw_df.withColumn(
        "event_timestamp",
        F.expr(f"to_timestamp('{ingestion_date} 00:00:00', 'yyyy-MM-dd HH:mm:ss') + cast(step as int) * INTERVAL 1 HOUR")
    )

    # Map to standardized schema
    df = df.select(
        F.expr("uuid()").alias("event_id"),
        F.lit("paysim").alias("source_system"),
        F.concat_ws("_", F.lit("paysim"), F.lit(run_id), F.monotonically_increasing_id()).alias("transaction_id"),
        F.col("event_timestamp"),
        F.col("amount").cast("double").alias("amount"),
        F.col("type").alias("transaction_type"),
        F.col("nameOrig").alias("origin_account_id"),
        F.col("nameDest").alias("destination_account_id"),
        F.col("isFraud").cast("int").alias("is_fraud"),
        F.col("isFlaggedFraud").cast("int").alias("is_flagged_fraud")
    )
    return df


def process_credit_card(spark, bucket: str, env: str, domain: str, ingestion_date: str, run_id: str) -> DataFrame:
    path = f"s3a://{bucket}/{env}/{domain}/raw/credit_card_fraud_detection/transactions/ingestion_date={ingestion_date}/run_id={run_id}/"
    print(f"Processing Credit Card Raw from: {path}")
    raw_df = spark.read.parquet(path)

    # Use a baseline timestamp of 2026-01-01 00:00:00 UTC and add seconds from 'Time'
    df = raw_df.withColumn(
        "event_timestamp",
        F.expr("to_timestamp('2026-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') + cast(Time as int) * INTERVAL 1 SECOND")
    )

    df = df.select(
        F.expr("uuid()").alias("event_id"),
        F.lit("credit_card_fraud_detection").alias("source_system"),
        F.concat_ws("_", F.lit("cc"), F.lit(run_id), F.monotonically_increasing_id()).alias("transaction_id"),
        F.col("event_timestamp"),
        F.col("Amount").cast("double").alias("amount"),
        F.lit("CREDIT_CARD").alias("transaction_type"),
        F.lit(None).cast("string").alias("origin_account_id"),
        F.lit(None).cast("string").alias("destination_account_id"),
        F.col("Class").cast("int").alias("is_fraud"),
        F.lit(0).cast("int").alias("is_flagged_fraud")
    )
    return df


def process_ieee_cis(spark, bucket: str, env: str, domain: str, ingestion_date: str, run_id: str) -> DataFrame:
    path = f"s3a://{bucket}/{env}/{domain}/raw/ieee_cis_fraud_detection/train_transaction/ingestion_date={ingestion_date}/run_id={run_id}/"
    print(f"Processing IEEE-CIS Raw from: {path}")
    raw_df = spark.read.parquet(path)

    # Use baseline timestamp of 2026-01-01 00:00:00 UTC and add seconds from 'TransactionDT'
    df = raw_df.withColumn(
        "event_timestamp",
        F.expr("to_timestamp('2026-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') + cast(TransactionDT as int) * INTERVAL 1 SECOND")
    )

    df = df.select(
        F.expr("uuid()").alias("event_id"),
        F.lit("ieee_cis_fraud_detection").alias("source_system"),
        F.col("TransactionID").cast("string").alias("transaction_id"),
        F.col("event_timestamp"),
        F.col("TransactionAmt").cast("double").alias("amount"),
        F.col("ProductCD").alias("transaction_type"),
        F.lit(None).cast("string").alias("origin_account_id"),
        F.lit(None).cast("string").alias("destination_account_id"),
        F.col("isFraud").cast("int").alias("is_fraud"),
        F.lit(0).cast("int").alias("is_flagged_fraud")
    )
    return df


def apply_quality_rules(df: DataFrame) -> DataFrame:
    errors = []
    # 1. Amount should be positive
    errors.append(F.when(~(F.col("amount") > 0), "invalid_amount").otherwise(None))
    # 2. is_fraud must be 0 or 1
    errors.append(F.when(~F.col("is_fraud").isin(0, 1), "invalid_fraud_label").otherwise(None))
    # 3. transaction_id must not be null
    errors.append(F.when(F.col("transaction_id").isNull(), "missing_transaction_id").otherwise(None))
    # 4. event_timestamp must not be null
    errors.append(F.when(F.col("event_timestamp").isNull(), "missing_event_timestamp").otherwise(None))

    df = df.withColumn("quality_errors", F.array([e for e in errors]))
    # Filter out null values from array
    df = df.withColumn("quality_errors", F.expr("filter(quality_errors, x -> x is not null)"))
    
    # Determine quality status based on whether errors exist
    df = df.withColumn(
        "quality_status",
        F.when(F.size(F.col("quality_errors")) > 0, "quarantine").otherwise("valid")
    )
    return df


def main() -> None:
    args = parse_args()
    spark = build_spark_session("Orion-Raw-To-Staging-Financial-Crisis")

    quality_stats = []

    try:
        # Process each system
        paysim_df = process_paysim(spark, args.bucket, args.env, args.domain, args.ingestion_date, args.run_id)
        cc_df = process_credit_card(spark, args.bucket, args.env, args.domain, args.ingestion_date, args.run_id)
        ieee_df = process_ieee_cis(spark, args.bucket, args.env, args.domain, args.ingestion_date, args.run_id)

        # Union all sources
        union_df = paysim_df.union(cc_df).union(ieee_df)

        # Apply Quality Rules
        enriched_df = apply_quality_rules(union_df)

        # Add partitions for Spark write
        enriched_df = enriched_df.withColumn("ingestion_date", F.lit(args.ingestion_date))
        enriched_df = enriched_df.withColumn("run_id", F.lit(args.run_id))

        # Split valid and quarantined rows
        valid_df = enriched_df.filter(F.col("quality_status") == "valid")
        quarantine_df = enriched_df.filter(F.col("quality_status") == "quarantine")

        # Count records for reporting
        for system in ["paysim", "credit_card_fraud_detection", "ieee_cis_fraud_detection"]:
            sys_df = enriched_df.filter(F.col("source_system") == system)
            total_count = sys_df.count()
            valid_count = sys_df.filter(F.col("quality_status") == "valid").count()
            quarantine_count = sys_df.filter(F.col("quality_status") == "quarantine").count()
            
            quality_stats.append({
                "source_system": system,
                "input_rows": total_count,
                "valid_rows": valid_count,
                "quarantine_rows": quarantine_count
            })

        # Save Valid Events
        valid_out_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/staging/financial_fraud_events/"
        print(f"Writing clean staging events to: {valid_out_path}")
        valid_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(valid_out_path)

        # Save Quarantined Events
        if quarantine_df.count() > 0:
            quar_out_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/staging/quality/quarantined_events/"
            print(f"Writing quarantined staging events to: {quar_out_path}")
            quarantine_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(quar_out_path)
        else:
            print("No quarantined events found for this execution.")

        # Write Quality Report JSON to S3
        report_key = f"{args.env}/{args.domain}/staging/quality/quality_report_ingestion_date={args.ingestion_date}_run_id={args.run_id}.json"
        report = {
            "layer": "staging",
            "ingestion_date": args.ingestion_date,
            "run_id": args.run_id,
            "sources": quality_stats
        }
        write_quality_report(args.bucket, report_key, report)

        print("OK: Raw to Staging Spark job completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
