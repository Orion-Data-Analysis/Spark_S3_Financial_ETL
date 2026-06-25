import argparse
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from spark_jobs.session import build_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute Platinum layer Mart analytical tables from clean Staging and Intermediate data."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--env", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--ingestion-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--write-mode", default="overwrite")
    return parser.parse_args()


def compute_daily_fraud_metrics(staging_df: DataFrame) -> DataFrame:
    # Truncate timestamp to date
    df = staging_df.withColumn("event_date", F.to_date("event_timestamp"))
    
    metrics_df = df.groupBy("source_system", "event_date").agg(
        F.count("event_id").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.sum("is_fraud").alias("fraud_transactions"),
        F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0.0)).alias("fraud_amount"),
        F.coalesce(F.sum("is_fraud") / F.count("event_id"), F.lit(0.0)).alias("fraud_rate"),
        F.sum(F.when((F.col("is_fraud") == 1) & (F.col("is_flagged_fraud") == 1), F.col("amount")).otherwise(0.0)).alias("loss_prevention_amount")
    )
    return metrics_df


def compute_high_risk_alerts(velocity_df: DataFrame, profile_df: DataFrame) -> DataFrame:
    # Join velocity and profile datasets on origin_account_id
    alerts_df = velocity_df.join(profile_df, on=["origin_account_id", "ingestion_date", "run_id"], how="inner")

    # Define high-risk conditions
    is_high_velocity = F.col("tx_count_24h") > 10
    is_suspicious_customer = F.col("fraud_rate") > 0.2
    is_confirmed_fraud = F.col("is_fraud") == 1

    # Apply filter for high risk
    alerts_df = alerts_df.filter(is_high_velocity | is_suspicious_customer | is_confirmed_fraud)

    # Compute descriptive alert reason
    alerts_df = alerts_df.withColumn(
        "alert_reason",
        F.concat_ws("; ",
            F.when(is_high_velocity, "high_velocity_24h").otherwise(None),
            F.when(is_suspicious_customer, "high_customer_historic_fraud_rate").otherwise(None),
            F.when(is_confirmed_fraud, "confirmed_fraud_transaction").otherwise(None)
        )
    )

    alerts_df = alerts_df.select(
        F.col("event_id"),
        F.col("source_system"),
        F.col("transaction_id"),
        F.col("origin_account_id"),
        F.col("event_timestamp"),
        F.col("amount"),
        F.col("tx_count_24h"),
        F.col("tx_amount_24h"),
        F.col("fraud_rate").alias("customer_historic_fraud_rate"),
        F.col("is_fraud"),
        F.col("alert_reason")
    )
    return alerts_df


def compute_reconciliation_summary(spark, bucket: str, env: str, domain: str, ingestion_date: str, run_id: str) -> DataFrame:
    print("Computing audit reconciliation metrics across data layers...")
    
    # 1. Count Raw rows (we sum count of paysim, credit card and ieee)
    raw_total = 0
    raw_sources = [
        ("paysim", "transactions"),
        ("credit_card_fraud_detection", "transactions"),
        ("ieee_cis_fraud_detection", "train_transaction")
    ]
    for sys, dataset in raw_sources:
        try:
            path = f"s3a://{bucket}/{env}/{domain}/raw/{sys}/{dataset}/ingestion_date={ingestion_date}/run_id={run_id}/"
            raw_total += spark.read.parquet(path).count()
        except Exception as e:
            print(f"Warning: Could not read raw path for {sys}/{dataset}: {e}")

    # 2. Count Staging Valid rows
    staging_valid = 0
    try:
        staging_valid_path = f"s3a://{bucket}/{env}/{domain}/staging/financial_fraud_events/"
        staging_valid = spark.read.parquet(staging_valid_path).filter(
            (F.col("ingestion_date") == ingestion_date) & 
            (F.col("run_id") == run_id) & 
            (F.col("quality_status") == "valid")
        ).count()
    except Exception as e:
        print(f"Warning: Could not read staging valid path: {e}")

    # 3. Count Staging Quarantine rows
    staging_quarantine = 0
    try:
        staging_quarantine_path = f"s3a://{bucket}/{env}/{domain}/staging/quality/quarantined_events/"
        staging_quarantine = spark.read.parquet(staging_quarantine_path).filter(
            (F.col("ingestion_date") == ingestion_date) & 
            (F.col("run_id") == run_id)
        ).count()
    except Exception as e:
        print(f"Warning: Could not read staging quarantine path: {e}")

    # Calculate reconciliation difference and status
    difference = raw_total - (staging_valid + staging_quarantine)
    status = "MATCH" if difference == 0 else "MISMATCH"

    # Construct small DataFrame
    reconciliation_data = [(ingestion_date, run_id, raw_total, staging_valid, staging_quarantine, difference, status)]
    schema = ["ingestion_date", "run_id", "raw_total_rows", "staging_valid_rows", "staging_quarantine_rows", "reconciliation_difference", "reconciliation_status"]
    
    return spark.createDataFrame(reconciliation_data, schema=schema)


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
    spark = build_spark_session("Orion-Intermediate-To-Mart-Financial-Crisis")

    db_name = f"financial_crisis_{args.env}_mart"

    try:
        # 1. Read clean staging events
        staging_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/staging/financial_fraud_events/"
        staging_df = spark.read.parquet(staging_path).filter(
            (F.col("ingestion_date") == args.ingestion_date) & 
            (F.col("run_id") == args.run_id) & 
            (F.col("quality_status") == "valid")
        )

        # 2. Read intermediate velocity events
        velocity_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/intermediate/int_transaction_velocity/"
        velocity_df = spark.read.parquet(velocity_path).filter(
            (F.col("ingestion_date") == args.ingestion_date) & 
            (F.col("run_id") == args.run_id)
        )

        # 3. Read intermediate customer profiles
        profile_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/intermediate/int_customer_risk_profile/"
        profile_df = spark.read.parquet(profile_path).filter(
            (F.col("ingestion_date") == args.ingestion_date) & 
            (F.col("run_id") == args.run_id)
        )

        # 1. Compute Daily Fraud Metrics
        daily_metrics_df = compute_daily_fraud_metrics(staging_df)
        daily_metrics_df = daily_metrics_df.withColumn("ingestion_date", F.lit(args.ingestion_date))
        daily_metrics_df = daily_metrics_df.withColumn("run_id", F.lit(args.run_id))

        daily_out_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/mart/mart_daily_fraud_metrics/"
        print(f"Writing daily metrics to: {daily_out_path}")
        daily_metrics_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(daily_out_path)
        register_table_in_catalog(spark, daily_metrics_df, db_name, "mart_daily_fraud_metrics", daily_out_path, args.write_mode)

        # 2. Compute High Risk Alerts
        alerts_df = compute_high_risk_alerts(velocity_df, profile_df)
        # Note: alerts_df already contains 'is_fraud' which we can check. Add partition columns
        alerts_df = alerts_df.withColumn("ingestion_date", F.lit(args.ingestion_date))
        alerts_df = alerts_df.withColumn("run_id", F.lit(args.run_id))

        alerts_out_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/mart/mart_high_risk_alerts/"
        print(f"Writing high-risk alerts to: {alerts_out_path}")
        alerts_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(alerts_out_path)
        register_table_in_catalog(spark, alerts_df, db_name, "mart_high_risk_alerts", alerts_out_path, args.write_mode)

        # 3. Compute Data Reconciliation Summary
        recon_df = compute_reconciliation_summary(spark, args.bucket, args.env, args.domain, args.ingestion_date, args.run_id)
        
        recon_out_path = f"s3a://{args.bucket}/{args.env}/{args.domain}/mart/mart_data_reconciliation/"
        print(f"Writing reconciliation summary to: {recon_out_path}")
        recon_df.write.mode(args.write_mode).partitionBy("ingestion_date", "run_id").parquet(recon_out_path)
        register_table_in_catalog(spark, recon_df, db_name, "mart_data_reconciliation", recon_out_path, args.write_mode)

        print("OK: Intermediate to Mart Spark job completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
