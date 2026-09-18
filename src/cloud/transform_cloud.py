import os
import json
import io
import pandas as pd
import boto3
from dotenv import load_dotenv

# load environment variables from the .env file
load_dotenv()

def transform_data_s3():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not all([aws_access_key, aws_secret_key, bucket_name]):
        raise ValueError("AWS Credentials or Bucket Name missing in .env file!")

    # Connect to AWS S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

    # List all objects in the raw/ folder in S3
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="raw/")
    
    if 'Contents' not in response:
        print("No raw files found in S3 bucket!")
        return

    dfs = []

    for obj in response['Contents']:
        file_key = obj['Key']

        # process only JSON files
        if not file_key.endswith('_daily.json'):
            continue

        print(f"Reading from S3: s3://{bucket_name}/{file_key}")

        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        raw_data = json.loads(file_obj['Body'].read().decode('utf-8'))

    # skip the metadata and extract only the stock market data
        time_series_key = None
        for key in raw_data.keys():
            if "Time Series" in key:
                time_series_key = key
                break
        
        time_series = raw_data.get(time_series_key, {})

        # if no data inside
        if not time_series:
            print(f"Skipping the file {file_key}: no valid time series data!!!")
            continue


        # load data into pandas + transposition
        df = pd.DataFrame(time_series).T

        # reset index dispite date
        df =df.reset_index()

        # columns name cleaning 
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

        # data test
        # print(df.head())

        # converting data from a string to a number
        df["open"] = df["open"].astype(float)
        df["high"] = df["high"].astype(float)
        df["low"] = df["low"].astype(float)
        df["close"] = df["close"].astype(float)
        df["volume"] = df["volume"].astype(float)

        # data type test
        # print(df.dtypes)


        # adding a column to identify the company
        base_name = file_key.split("/")[-1]
        symbol = base_name.split("_")[0]
        df["symbol"] = symbol

        # rearranging the order of the columns
        df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

        # print(df.head())

        # export do bufora pamięci RAM i wysyłka do S3
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        output_key = f"processed/{symbol}_daily.csv"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )

        dfs.append(df)
        print(f"Successfully processed and uploaded: s3://{bucket_name}/{output_key}")

    # POPRAWKA 4: Łączenie zbioru i zapis zbiorczego CSV do S3 wewnątrz funkcji
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)

        combined_buffer = io.StringIO()
        combined_df.to_csv(combined_buffer, index=False)
        combined_key = "processed/combined_assets_daily.csv"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=combined_key,
            Body=combined_buffer.getvalue(),
            ContentType='text/csv'
        )

        print(f"Combined dataset saved to: s3://{bucket_name}/{combined_key}")

if __name__ == '__main__':
    transform_data_s3()