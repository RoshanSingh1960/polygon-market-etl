import schedule 
import time 
from etl_pipeline import PolygonETL 
from datetime import datetime 
import logging 
 
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__) 
 
# List of symbols you want to update daily 
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOG"]  
 
def daily_etl_job(): 
    """Run ETL after market close (e.g., 4:30 PM ET)""" 
    logger.info(f"Scheduler: Starting daily ETL job at {datetime.now()}") 
    if datetime.now().weekday() < 5:  # Mon-Fri only 
        etl = PolygonETL() 
        # Fetch 1 day of historical data for each symbol 
        # This will make N API calls, where N is len(DEFAULT_SYMBOLS), with delays in between. 
        etl.run_pipeline(DEFAULT_SYMBOLS, 1, "minute", days_back=1)   
        logger.info(f"Scheduler: Daily ETL job finished for {len(DEFAULT_SYMBOLS)} symbols.") 
    else: 
        logger.info("Scheduler: Skipping daily ETL job as it's a weekend.") 
 
# Schedule after market hours 
schedule.every().day.at("16:30").do(daily_etl_job) 
logger.info("Scheduler started. Daily ETL job scheduled for 16:30 ET.") 
 
if __name__ == "__main__": 
    print("Scheduler running... Press Ctrl+C to stop") 
    while True: 
        schedule.run_pending() 
        time.sleep(60)  # Check every minute