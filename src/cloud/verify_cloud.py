import sqlite3
import pandas as pd

DB_PATH = "data/financial_data.db"

def verify_database():
    print("--- VERIFYING AN SQLITE DATABASE ---")
    
    # conection
    conn = sqlite3.connect(DB_PATH)
    
    # check if table exist
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
    print(f"\nTables in Data Base: {tables['name'].tolist()}")
    
    # data summary for each symbol
    query_summary = """
        SELECT 
            symbol,
            COUNT(*) as records_number,
            MIN(date) as oldest_date,
            MAX(date) as recent_date,
            ROUND(AVG(close), 2) as average_closing_price
        FROM daily_assets
        GROUP BY symbol;
    """
    df_summary = pd.read_sql_query(query_summary, conn)
    print("\n--- Summary of Data by Asset ---")
    print(df_summary.to_string(index=False))

    # 5 first rows display
    df_head = pd.read_sql_query("SELECT * FROM daily_assets LIMIT 5;", conn)
    print("\n--- 5 rows from the table ---")
    print(df_head.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    verify_database()