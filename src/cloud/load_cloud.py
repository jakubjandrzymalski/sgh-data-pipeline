import os
import io
import sqlite3 
import pandas as pd
import boto3
from dotenv import load_dotenv

# load environment variables from the .env file
load_dotenv()

DB_PATH = "data/financial_data.db"
S3_KEY = "processed/combined_assets_daily.csv"

def load_cloud_to_sqlite():
    # retrieve AWS credentials from the .env file
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not all([aws_access_key, aws_secret_key, bucket_name]):
        raise ValueError("AWS login credentials or S3_BUCKET_NAME are missing from the .env file!")

    # connection with the S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    # loading data from S3 into RAM
    try:
        print(f"Downloading processed data from S3: s3://{bucket_name}/{S3_KEY}")
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=S3_KEY)
        
        # io.BytesIO allows Pandas to read a byte stream from S3 without writing it to disk
        df = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        print(f"{len(df)} lines were read from the S3 file")

    except Exception as e:
        print(f"Error while downloading a file from S3: {e}")
        return

    # make sure that the local “data/” folder exists for the SQLite database
    os.makedirs("data", exist_ok=True)

    #  writing to the SQLite database
    connect = sqlite3.connect(DB_PATH)

    try:
        df.to_sql('daily_assets', connect, if_exists='replace', index=False)
        print("The data was successfully loaded from S3 into SQLite!")

    except Exception as e:
        print(f'Error while loading into the database: {e}')

    finally:
        connect.close()

if __name__ == "__main__":
    load_cloud_to_sqlite()