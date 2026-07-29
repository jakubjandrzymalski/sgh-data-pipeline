import os
import json
import requests
from dotenv import load_dotenv

# load environment variables from the .env file into memory

load_dotenv()

# helper function to convert raw data to JSON

def raw_to_json(response, symbol):

    if response.status_code == 200:
        data = response.json()
        file_path = f'data/raw/{symbol}_daily.json'

        with open(file_path, "w", encoding = 'utf-8') as f:
            json.dump(data, f, indent=4)

        print(f'Request for data {symbol} saved to {file_path}')

    else:
        print(f'Error for {symbol}: Status code {response.status_code}')


def data_extraction_alpha_vantage():

    # key to www.alphavantage.co from  the environment variables
    API_key = os.getenv("ALPHA_VANTAGE_API_KEY")

    # checking if key exist
    if not API_key:
        raise ValueError("API key missing! Make sure the .env file contains ALPHA_VANTAGE_API_KEY.")

    # make a directory to save raw data
    os.makedirs("data/raw", exist_ok=True)


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
            raw_to_json(response , CompSymb)

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

            raw_to_json(response , CrypSymb)

        except requests.exceptions.RequestException as e:
            print(f'Network error for {CompSymb}: {e}')





# ___code execution _____________________________________________________________________________________

if __name__ == '__main__':

    data_extraction_alpha_vantage()
