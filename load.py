import os
import sqlite3 
import pandas as pd

DB_PATH = "data/financial_data.db"
COMBINED_CSV = "data/processed/combined_assets_daily.csv"

def load_to_sqlite():

    # check if the file exists
    if not os.path.exists(COMBINED_CSV):
        print(f'Error: {COMBINED_CSV} do not exist')
        return
    
    # load data from a CSV file into a Pandas DataFrame
    df = pd.read_csv(COMBINED_CSV)
    print(f'{len(df)} rows were loaded from the CSV file')

    # connecting to the database and creating a file
    connect = sqlite3.connect(DB_PATH)

    try:
    # saving a data frame to an SQL table
        df.to_sql('daily_assets', connect, if_exists = 'replace', index=False)
        print("Data uploaded to SQLite")

    except Exception as e:
        print(f'error while loding of {e}')

    finally:
        # closing the connection to the database
        connect.close()

    
if __name__ == "__main__":
    load_to_sqlite()