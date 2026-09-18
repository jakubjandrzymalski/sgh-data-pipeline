import os
import sys
import json
import shutil
import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load environment variables from the .env file
load_dotenv()

# Define S3 prefixes and temporary local directory
S3_RAW_PREFIX = "raw/"
# Silver: cleaned, standardized OHLCV data (before technical indicators)
S3_PROCESSED_SILVER_DIR = "processed/silver/combined_assets_daily.parquet"
# Gold: Silver + SMA-14/EMA-14, ready for the Athena/Power BI analytical layer
S3_PROCESSED_GOLD_DIR = "processed/gold/combined_assets_daily.parquet"
TEMP_PARQUET_DIR = "data/temp_parquet"

def get_spark_session():
    """Initialize and configure a local PySpark Session."""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    return SparkSession.builder \
        .appName("SGH_Financial_Data_Transform") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def write_parquet_to_s3(spark_df, s3_client, bucket_name, s3_prefix, local_filename):
    """Write a Spark DataFrame to local Parquet, then upload it to the given S3 prefix."""
    if os.path.exists(TEMP_PARQUET_DIR):
        shutil.rmtree(TEMP_PARQUET_DIR)
    os.makedirs(TEMP_PARQUET_DIR, exist_ok=True)

    parquet_file_path = os.path.join(TEMP_PARQUET_DIR, local_filename)
    spark_df.toPandas().to_parquet(parquet_file_path, index=False)

    s3_key = f"{s3_prefix}/{local_filename}"
    print(f"Uploading Parquet file to S3: s3://{bucket_name}/{s3_key}")
    s3_client.upload_file(parquet_file_path, bucket_name, s3_key)

    if os.path.exists(TEMP_PARQUET_DIR):
        shutil.rmtree(TEMP_PARQUET_DIR)

def read_parquet_from_s3(spark, s3_client, bucket_name, s3_prefix, local_filename):
    """Download a Parquet file from the given S3 prefix and load it into a Spark DataFrame."""
    if os.path.exists(TEMP_PARQUET_DIR):
        shutil.rmtree(TEMP_PARQUET_DIR)
    os.makedirs(TEMP_PARQUET_DIR, exist_ok=True)

    s3_key = f"{s3_prefix}/{local_filename}"
    parquet_file_path = os.path.join(TEMP_PARQUET_DIR, local_filename)
    print(f"Downloading Parquet file from S3: s3://{bucket_name}/{s3_key}")
    s3_client.download_file(bucket_name, s3_key, parquet_file_path)

    return spark.createDataFrame(pd.read_parquet(parquet_file_path))

def get_s3_client():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not all([aws_access_key, aws_secret_key, bucket_name]):
        raise ValueError("AWS Credentials or Bucket Name missing in .env file!")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    return s3_client, bucket_name

def transform_to_silver():
    """Task 1: extract raw JSON from S3, clean/standardize it and write it to the Silver layer."""
    # Connect to AWS S3 client
    s3_client, bucket_name = get_s3_client()

    # List all objects in the raw/ folder in S3
    print(f"Reading file list from S3: s3://{bucket_name}/{S3_RAW_PREFIX}")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=S3_RAW_PREFIX)
    
    if 'Contents' not in response:
        print("No raw files found in S3 bucket!")
        return

    all_records = []

    # Iterate over raw JSON files
    for obj in response['Contents']:
        file_key = obj['Key']

        if not file_key.endswith('.json'):
            continue

        print(f"Reading from S3: s3://{bucket_name}/{file_key}")

        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        raw_data = json.loads(file_obj['Body'].read().decode('utf-8'))

        # Dynamically locate metadata and time series keys inside the JSON structure
        meta_key = next((k for k in raw_data.keys() if "Meta Data" in k), None)
        time_series_key = next((k for k in raw_data.keys() if "Time Series" in k), None)

        if not meta_key or not time_series_key:
            print(f"Skipping {file_key}: missing valid metadata or time series key")
            continue

        # Extract symbol name
        symbol = raw_data[meta_key].get("2. Symbol") or raw_data[meta_key].get("2. Digital Currency Code")
        time_series = raw_data[time_series_key]

        # Flatten nested JSON data
        for date_str, metrics in time_series.items():
            all_records.append({
                "date": date_str,
                "symbol": symbol,
                "open": float(metrics.get("1. open", 0)),
                "high": float(metrics.get("2. high", 0)),
                "low": float(metrics.get("3. low", 0)),
                "close": float(metrics.get("4. close", 0)),
                "volume": float(metrics.get("5. volume", 0))
            })

    print(f"Extracted {len(all_records)} total records. Initializing PySpark engine...")

    # Start PySpark Session
    spark = get_spark_session()
    
    # Create distributed PySpark DataFrame
    spark_df = spark.createDataFrame(all_records)

    # Data type conversion
    spark_df = spark_df.withColumn("date", F.to_date(F.col("date")))

    # --- SILVER LAYER: cleaned and standardized data, before technical indicators ---
    write_parquet_to_s3(spark_df, s3_client, bucket_name, S3_PROCESSED_SILVER_DIR, "combined_assets.parquet")

    print("Successfully finished Silver layer transformation and S3 Parquet upload!")

    # Stop Spark Session
    spark.stop()

def enrich_to_gold():
    """Task 2: read the Silver layer back from S3, calculate SMA-14/EMA-14 and write the Gold layer."""
    # Connect to AWS S3 client
    s3_client, bucket_name = get_s3_client()

    # Start PySpark Session
    spark = get_spark_session()

    # Read the Silver layer back from S3
    spark_df = read_parquet_from_s3(spark, s3_client, bucket_name, S3_PROCESSED_SILVER_DIR, "combined_assets.parquet")

    #  --- CALCULATING MOVING AVERAGES (SMA-14 and EMA-14) ---
    window_14 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
    window_ordered = Window.partitionBy("symbol").orderBy("date")

    # CALCULATING SMA-14
    spark_df = spark_df.withColumn("sma_14", F.round(F.avg("close").over(window_14), 4))

    # CALCULATING EMA-14
    alpha = 2 / (14 + 1)
    spark_df = spark_df.withColumn("prev_sma", F.lag("sma_14", 1).over(window_ordered))
    spark_df = spark_df.withColumn(
        "ema_14",
        F.when(
            F.col("prev_sma").isNotNull(),
            F.round((F.col("close") * alpha) + (F.col("prev_sma") * (1 - alpha)), 4)
        ).otherwise(F.col("sma_14"))
    ).drop("prev_sma")

    # Final sorting
    spark_df = spark_df.sort(F.col("symbol"), F.col("date").desc())

    # --- GOLD LAYER: Silver + SMA-14/EMA-14, ready for Athena/Power BI ---
    write_parquet_to_s3(spark_df, s3_client, bucket_name, S3_PROCESSED_GOLD_DIR, "combined_assets.parquet")

    print("Successfully finished Gold layer transformation and S3 Parquet upload!")

    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    transform_to_silver()
    enrich_to_gold()