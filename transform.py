import os 
import json
import glob 
import pandas as pd

# search for files that match the pattern
input_files = glob.glob("data/raw/*_daily.json")


for file_path in input_files:

    # open the file for reading with right encoding
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # skip the metadata and extract only the stock market data
    time_series_key = None

    for key in raw_data.keys():
        if "Time Series" in key:
            time_series_key = key
            break
    
    time_series = raw_data.get(time_series_key, {})

    # if no data inside
    if not time_series:
        print(f"Skipping the file {file_path}: no valid time series data!!!")
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
    base_name = os.path.basename(file_path)
    symbol = base_name.split("_")[0]
    df["symbol"] = symbol

    # rearranging the order of the columns
    df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

    # print(df.head())

    # check if destination folder exists. If it not, we create it
    os.makedirs('data/processed', exist_ok=True)
    output_path = f"data/processed/{symbol}_daily.csv"
    df.to_csv(output_path, index=False)

    print(f"Successfully processed data: {symbol} -> {output_path}")

