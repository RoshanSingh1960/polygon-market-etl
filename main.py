#!/usr/bin/env python3 
import argparse 
from etl_pipeline import PolygonETL 
import logging 
 
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__) 
 
def main(): 
    parser = argparse.ArgumentParser(description="Polygon.io Market Data ETL") 
    parser.add_argument("--symbols", required=True,  
                        help="Comma-separated list of stock symbols (e.g., AAPL,MSFT,GOOG)") 
    parser.add_argument("--multiplier", type=int, default=1, help="Multiplier (default: 1)") 
    parser.add_argument("--timespan", choices=['minute', 'hour', 'day'],  
                       default='minute', help="Timespan") 
    parser.add_argument("--days_back", type=int, default=7,  
                       help="Days of historical data (5-60)") 
    parser.add_argument("--resample_to", help="Resample to (e.g., 5T, 1H)") 
     
    args = parser.parse_args() 
     
    symbols_list = [s.strip().upper() for s in args.symbols.split(',')] 
 
    etl = PolygonETL() 
    etl.run_pipeline(symbols_list, args.multiplier, args.timespan,  
                    args.days_back, args.resample_to) 
     
    logger.info(f"All requested ETL processes completed.") 
 
if __name__ == "__main__": 
    main()