import os 
import json
import pandas as pd

# open the file for reading with right encoding
with open('data/raw/AAPL_daily.json', 'r', encoding='utf-8') as f:
    raw_data = json.load(f)

# skip the metadata and extract only the stock market data
time_series = raw_data.get("Time Series (Daily)", {})


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

df["volume"] = df["volume"].astype(int)

# data type test
# print(df.dtypes)


# adding a column to identify the company
df["symbol"] = "AAPL"

# rearranging the order of the columns
df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

print(df.head())

# check if destination folder exists. If it not, we create it
os.makedirs('data/processed/AAPL_daily.csv',  exist_ok=False)

