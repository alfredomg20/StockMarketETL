from datetime import datetime, date, timedelta
from utils.time import get_last_market_close_date
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_extraction_params(latest_date: str |datetime | date) -> tuple[date | None, str | None, bool]:
    """ Define start date, period, and whether to update existing tickers
    based on the latest date in the table and current date. """

    # Ensure latest_date is a date object
    if isinstance(latest_date, str):
        latest_date = date.fromisoformat(latest_date)
    if isinstance(latest_date, datetime):
        latest_date = latest_date.date()
    
    if not latest_date:
        logging.info("No existing data found. Full extraction will be performed.")
        return None, 'max', False

    last_market_close_date = get_last_market_close_date()
    if latest_date < last_market_close_date:
        start_date = latest_date + timedelta(days=1)
        logging.info(f"Update needed: {latest_date} < {last_market_close_date}. Start: {start_date}")
        return start_date, None, True
    
    logging.info(f"Data is up to date. Latest date: {latest_date}, Last market close date: {last_market_close_date}.")
    return None, None, False