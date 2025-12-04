import requests 
import pandas as pd 
import numpy as np 
import sqlite3 
from datetime import datetime, timedelta 
import os 
import time 
import argparse 
from typing import Optional, List 
import logging 
 
# Configure logging 
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__) 
 
class PolygonETL: 
    def __init__(self, api_key: str = "jKMu5ABxUXWZAZy8uEoZuHlujdVFn3us"): 
        self.api_key = api_key 
        self.base_url = "https://api.polygon.io" 
        os.makedirs("data", exist_ok=True) 
        self.db_path = "data/market_data.db" 
        self.api_call_count = 0 
        self.last_api_call_time = time.time() 
     
    def _wait_for_rate_limit(self): 
        """Ensures API calls stay within the 5 calls/minute limit.""" 
        current_time = time.time() 
        elapsed_time = current_time - self.last_api_call_time 
         
        # If we've made 5 calls in less than a minute, wait the remainder 
        if self.api_call_count >= 5 and elapsed_time < 60: 
            wait_time = 60 - elapsed_time 
            logger.warning(f"Rate limit hit. Waiting for {wait_time:.2f} seconds.") 
            time.sleep(wait_time) 
            self.api_call_count = 0 # Reset count after waiting 
            self.last_api_call_time = time.time() # Reset timer 
        elif elapsed_time >= 60: 
            self.api_call_count = 0 # Reset count if a minute has passed 
            self.last_api_call_time = current_time # Reset timer 
         
        self.api_call_count += 1 # Increment call count for the current request 
         
    def extract(self, symbol: str, multiplier: int, timespan: str, days_back: int) -> Optional[pd.DataFrame]: # Changed return type to Optional 
        """Single bulk API call - NO loops to respect 5/min limit""" 
        self._wait_for_rate_limit() # Call rate limit checker before each API request 
 
        end_date = datetime.now().date() - timedelta(days=1)  # Yesterday only 
        start_date = end_date - timedelta(days=days_back) 
         
        from_date = start_date.strftime("%Y-%m-%d") 
        to_date = end_date.strftime("%Y-%m-%d") 
         
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}" 
        params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": self.api_key} 
         
        logger.info(f"Extracting {symbol} {multiplier}{timespan} data from {from_date} to {to_date}") 
         
        try: 
            response = requests.get(url, params=params) 
            response.raise_for_status() 
            data = response.json() 
             
            if 'results' not in data or not data['results']: 
                logger.warning(f"No data found for {symbol} for the specified range.") 
                return None # Return None if no data 
             
            df = pd.DataFrame(data['results']) 
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms') 
            df = df[['timestamp', 'o', 'h', 'l', 'c', 'v']].rename(columns={ 
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume' 
            }) 
            return df 
             
        except requests.exceptions.RequestException as e: 
            logger.error(f"API request failed for {symbol}: {e}") 
            return None 
        except Exception as e: 
            logger.error(f"Extraction failed for {symbol}: {e}") 
            return None 
     
    def transform(self, df: pd.DataFrame, resample_to: Optional[str] = None) -> pd.DataFrame: 
        """Clean, engineer features, optional resampling""" 
        # Ensure 'timestamp' is a datetime object before setting index 
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']): 
            df['timestamp'] = pd.to_datetime(df['timestamp']) 
 
        # Clean missing values 
        df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume']) # Drop rows with NaNs in core OHLCV 
        df = df[df['close'] > 0]  # Remove invalid prices 
         
        if resample_to: 
            # Resample using OHLCV aggregation rules 
            df = df.set_index('timestamp') # Set index for resampling 
            rule = resample_to.upper()  # e.g., '5T' for 5 minutes, '1H' for 1 hour 
             
            # Ensure proper handling of empty groups during resampling 
            resampled_df = df.resample(rule).agg( 
                open=('open', 'first'), 
                high=('high', 'max'), 
                low=('low', 'min'), 
                close=('close', 'last'), 
                volume=('volume', 'sum') 
            ) 
            # Drop rows where all OHLCV are NaN (e.g., periods with no data) 
            df = resampled_df.dropna(subset=['open', 'high', 'low', 'close', 'volume']).reset_index() 
        else: 
            df = df.reset_index(drop=True) # Reset index if it was set for resampling and no resampling occurred 
 
        if df.empty: 
            logger.warning("DataFrame is empty after transformation steps. Cannot calculate indicators.") 
            return df 
             
        # Engineer features 
        df['returns_pct'] = df['close'].pct_change() * 100 
        df['sma20'] = df['close'].rolling(20).mean() 
        df['sma50'] = df['close'].rolling(50).mean() 
        df['ema20'] = df['close'].ewm(span=20, adjust=False).mean() # adjust=False for typical EMA 
        df['ema50'] = df['close'].ewm(span=50, adjust=False).mean() # adjust=False for typical EMA 
         
        # Calculate rolling volatility based on percentage returns 
        if not df['returns_pct'].isnull().all(): # Only calculate if there are actual returns 
            df['volatility'] = df['returns_pct'].rolling(20).std() 
        else: 
            df['volatility'] = np.nan # Assign NaN if no returns to calculate std dev from 
 
        # Fill NaN for indicators (use ffill then bfill to propagate values or fill with 0) 
        df.fillna(method='ffill', inplace=True) # Forward fill 
        df.fillna(method='bfill', inplace=True) # Backward fill 
        df.fillna(0, inplace=True) # Fill remaining NaNs (e.g., at very start) with 0 
         
        return df 
     
    def load(self, df: pd.DataFrame, symbol: str, timespan: str): 
        """UPSERT to SQLite - no duplicates""" 
        if df.empty: 
            logger.warning(f"No data to load for {symbol}_{timespan}. Skipping.") 
            return 
 
        table_name = f"{symbol.lower()}_{timespan}" 
         
        conn = sqlite3.connect(self.db_path) 
         
        # Ensure timestamp column is correctly formatted for SQLite 
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S') 
 
        # Create table if not exists with correct data types 
        create_table_sql = f""" 
            CREATE TABLE IF NOT EXISTS {table_name} ( 
                timestamp TEXT PRIMARY KEY, 
                open REAL, high REAL, low REAL, close REAL, volume INTEGER, 
                returns_pct REAL, sma20 REAL, sma50 REAL,  
                ema20 REAL, ema50 REAL, volatility REAL 
            ) 
        """ 
        conn.execute(create_table_sql) 
        conn.commit() 
 
        # Using a more robust UPSERT approach for SQLite, leveraging 'INSERT OR REPLACE' 
        # Convert DataFrame to a list of tuples for batch insertion 
        cols = df.columns.tolist() 
        data_tuples = [tuple(x) for x in df[cols].values] 
 
        # Prepare the INSERT OR REPLACE statement 
        placeholders = ', '.join(['?'] * len(cols)) 
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders})" 
 
        try: 
            conn.executemany(insert_sql, data_tuples) 
            conn.commit() 
            logger.info(f"Loaded {len(df)} rows to {table_name} for symbol {symbol}.") 
        except sqlite3.Error as e: 
            logger.error(f"Error during UPSERT for {table_name}: {e}") 
        finally: 
            conn.close() 
     
    def run_pipeline(self, symbols: List[str], multiplier: int, timespan: str,  
                    days_back: int = 7, resample_to: Optional[str] = None): 
        """Complete ETL pipeline for multiple symbols.""" 
        for symbol in symbols: 
            logger.info(f"--- Running ETL for {symbol} ---") 
            try: 
                df = self.extract(symbol, multiplier, timespan, days_back) 
                if df is not None and not df.empty: 
                    df_transformed = self.transform(df, resample_to) 
                    if not df_transformed.empty: 
                        self.load(df_transformed, symbol, timespan) 
                        logger.info(f"ETL completed for {symbol} ({timespan}s)") 
                    else: 
                        logger.warning(f"Transformed data for {symbol} is empty. Skipping load.") 
                else: 
                    logger.warning(f"No data extracted for {symbol}. Skipping transformation and load.") 
            except Exception as e: 
                logger.error(f"ETL failed for {symbol}: {e}") 
            logger.info(f"--- Finished ETL for {symbol} ---")