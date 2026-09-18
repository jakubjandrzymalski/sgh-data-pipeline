import os
import json
import requests
from dotenv import load_dotenv
import boto3

# load environment variables from the .env file into memory

load_dotenv()

# helper function to convert data and to send temn to cloud

def raw_to_s3(response, symbol, s3_client, bucket_name):

    if response.status_code == 200:

        data = response.json()

        s3_key = f'raw/{symbol}_daily.json'
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
            )

        print(f'Request for data {symbol} saved to { s3_key}')

    else:
        print(f'Error for {symbol}: Status code {response.status_code}')


def data_extraction_alpha_vantage():
    # credentials from environment variables
    API_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    API_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")
    bucket_name = os.getenv("S3_BUCKET_NAME")

    # checking if variables exist
    if not API_key:
        raise ValueError("API key missing! Make sure the .env file contains ALPHA_VANTAGE_API_KEY.")

    if not all([aws_access_key, aws_secret_key, bucket_name]):
        raise ValueError("AWS Credentials or Bucket Name missing in .env file!")    
    
    # make object to save raw data in cloud
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )


# ____company____


    # company symbols we want too look for
    company_symbol = ["AAPL", "MSFT", "GOOGL"]

    # URL build company
    for CompSymb in company_symbol:

        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={CompSymb}&apikey={API_key}'

        print(f'Request for data {CompSymb} sent')

        # get company data and added the parameter timeout=10 (network security measure)
        try:
            response = requests.get(url, timeout=10)

            raw_to_s3(response, CompSymb, s3_client, bucket_name)

        except requests.exceptions.RequestException as e:
            print(f'Network error for {CompSymb}: {e}')



#____crypto____


    # crypto symbols we want too look for
    crypto_symbol = ["BTC", "ETH"]

    # URL build company
    for CrypSymb in crypto_symbol:

        url = f'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={CrypSymb}&market=USD&apikey={API_key}'

        print(f'Request for data {CrypSymb} sent')

        # get crypto data
        try:
            response = requests.get(url, timeout=10)

            raw_to_s3(response, CrypSymb, s3_client, bucket_name)

        except requests.exceptions.RequestException as e:
            print(f'Network error for {CrypSymb}: {e}')





# ___code execution _____________________________________________________________________________________

if __name__ == '__main__':

    data_extraction_alpha_vantage()
