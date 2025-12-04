import sqlite3 
import pandas as pd 
import json 
import os 
from datetime import datetime, timedelta 
 
def export_data_for_viz(symbol: str, timespan: str, days_back: int = 30): 
    db_path = "data/market_data.db" 
    table_name = f"{symbol.lower()}_{timespan}" 
    export_dir = "web_dashboard/public/data" 
    os.makedirs(export_dir, exist_ok=True) 
    output_filepath = os.path.join(export_dir, f"{symbol.lower()}_{timespan}_viz.json") 
 
    conn = sqlite3.connect(db_path) 
    try: 
        start_date_str = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S') 
        query = f"SELECT timestamp, open, high, low, close, volume, sma20, ema20 FROM {table_name} WHERE timestamp >= '{start_date_str}' ORDER BY timestamp ASC" 
         
        df = pd.read_sql_query(query, conn) 
        df['timestamp'] = pd.to_datetime(df['timestamp']).astype(int) / 10**6  # Unix ms for JS 
         
        json_data = df.to_json(orient='records', date_format='iso') 
         
        with open(output_filepath, 'w') as f: 
            f.write(json_data) 
         
        print(f"Exported {len(df)} records for {symbol} to {output_filepath}") 
    except Exception as e: 
        print(f"Error: {e}") 
    finally: 
        conn.close() 
 
if __name__ == "__main__": 
    export_data_for_viz("AAPL", "minute", days_back=30) 
    export_data_for_viz("MSFT", "minute", days_back=30)