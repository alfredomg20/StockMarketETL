import logging
from datetime import datetime, timedelta
import pytz  # Add this import
from config.assets import TICKERS
from config.settings import (
    CREDENTIALS_DICT, PROJECT_ID, DATASET_ID, STOCKS_TABLE_ID, SECTORS_TABLE_ID,
    REQUIRED_ENV_VARS, TIMEZONE
)
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from utils.validations import get_latest_data_date, check_existing_tickers, check_env_variables
from utils.google_cloud import get_bigquery_client

# Logger configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Starting ETL process...")

    # Check if all required environment variables are set
    missing_env_vars = check_env_variables(REQUIRED_ENV_VARS)
    if missing_env_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_env_vars)}")

    # Configure BigQuery client
    client = get_bigquery_client(CREDENTIALS_DICT, PROJECT_ID)

    # Check if each ticker exists in the sector table
    logging.info("Checking if each ticker exists in the sector table...")
    try:
        existing_tickers = check_existing_tickers(
            f"{PROJECT_ID}.{DATASET_ID}.{SECTORS_TABLE_ID}",
            client,
            TICKERS
        )
        missing_tickers = [ticker for ticker in TICKERS if ticker not in existing_tickers]
    except Exception as e:
        logging.error(f"Error validating tickers: {e}")
        missing_tickers = TICKERS  # If fails, assume all tickers are missing
    if missing_tickers:
        logging.info(f'Missing tickers to add: {missing_tickers}')
    else:
        logging.info("All tickers are included in the dataset.")

    # Determine the date range for incremental update
    logging.info("Checking latest stock data date...")
    latest_date = get_latest_data_date(
        f"{PROJECT_ID}.{DATASET_ID}.{STOCKS_TABLE_ID}",
        client,
        date_column="date"
    )
    
    # Get current time in New York timezone 
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    today = now.date()
    
    # Check if it's before 4 PM New York time (market close)
    market_close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    is_before_market_close = now < market_close_time

    start_date = None
    if latest_date:
        # Determine the target date for updates
        if is_before_market_close:
            # If before 4 PM NY time, don't try to get today's data
            target_date = today - timedelta(days=1)
            logging.info(f"Before market close (4 PM NY time). Target date set to: {target_date}")
        else:
            target_date = today
            logging.info(f"After market close (4 PM NY time). Target date set to: {target_date}")

        # Update only if the latest date is before target date and target date is not weekend
        if latest_date.date() < target_date and target_date.weekday() not in [5, 6]:
            # Update from the business day after the latest date
            if latest_date.weekday() == 4:
                days_delta = 3
            else:
                days_delta = 1
            start_date = (latest_date + timedelta(days=days_delta)).strftime('%Y-%m-%d')
            logging.info(f"Latest date in the table: {latest_date.date()}. Starting update from: {start_date}")
        else:
            # If the latest date is up to date or target date is weekend, no need to update
            existing_tickers = []
            logging.info("Existing tickers are up to date. No need to update.")
    else:
        # If no data exists, set start_date to None for full extraction
        start_date = None
        logging.info("No existing data found. Full extraction will be performed.")
    
    if not existing_tickers and not missing_tickers:
        return

    # Extract data for required tickers
    try:
        period = None if start_date else 'max'
        raw_stock_df, sector_df = extract_data(
            existing_tickers=existing_tickers,
            missing_tickers=missing_tickers,
            period=period,
            interval='1d',
            start_date=start_date
        )
        if raw_stock_df.empty and sector_df.empty:
            logging.warning("No stock or sector data extracted.")
            return
        logging.info("Data extracted successfully.")
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        return

    # Transform the extracted stock data
    try:
        transformed_stock_df = transform_data(raw_stock_df)
        if transformed_stock_df.empty:
            logging.warning("No data after transformation.")
            return
        logging.info("Data transformed successfully.")
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return

    # Load the transformed data and sector data into BigQuery
    dataframes = [transformed_stock_df, sector_df]
    table_ids = [STOCKS_TABLE_ID, SECTORS_TABLE_ID]
    try:
        status = load_data(dataframes, CREDENTIALS_DICT, PROJECT_ID, DATASET_ID, table_ids)
        if status == 'failure':
            return
        if status == 'success':
            logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        return

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()