import logging
from config.assets import TICKERS
from config.settings import (
    CREDENTIALS_DICT, PROJECT_ID, DATASET_ID, STOCKS_TABLE_ID, SECTORS_TABLE_ID,
    REQUIRED_ENV_VARS,
)
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from utils.bigquery import get_latest_data_date
from utils.extract_helpers import get_extraction_params
from utils.google_cloud import get_bigquery_client
from utils.validations import check_existing_tickers, check_env_variables

# Logger configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    logging.info("Starting ETL process...")

    # Validate environment variables
    missing_env_vars = check_env_variables(REQUIRED_ENV_VARS)
    if missing_env_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_env_vars)}")

    client = get_bigquery_client(CREDENTIALS_DICT, PROJECT_ID)

    # Validate tickers
    logging.info("Checking tickers in sector table...")
    try:
        existing_tickers = check_existing_tickers(
            f"{PROJECT_ID}.{DATASET_ID}.{SECTORS_TABLE_ID}",
            client,
            TICKERS
        )
        missing_tickers = [t for t in TICKERS if t not in existing_tickers]
    except Exception as e:
        logging.error(f"Error validating tickers: {e}")
        missing_tickers = TICKERS
        existing_tickers = []

    # Determine extraction parameters for define update strategy (full vs incremental)
    latest_date = get_latest_data_date(
            f"{PROJECT_ID}.{DATASET_ID}.{STOCKS_TABLE_ID}",
            client,
            date_column="date"
        )
    start_date, period, should_update = get_extraction_params(latest_date)

    # Decide whether to execute the ETL process
    # Run if there are new tickers OR if existing ones need updating
    if not should_update and not missing_tickers:
        logging.info("Everything is up to date. No execution needed.")
        return

    # If no update is needed for existing tickers, clear them for extract_data
    tickers_to_update = existing_tickers if should_update else []

    # Extract Phase
    try:
        raw_stock_df, sector_df = extract_data(
            existing_tickers=tickers_to_update,
            missing_tickers=missing_tickers,
            period=period,
            interval='1d',
            start_date=start_date
        )
        
        if raw_stock_df.empty and sector_df.empty:
            logging.warning("No data extracted.")
            return
            
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")
        return

    # Transform Phase
    try:
        transformed_stock_df = transform_data(raw_stock_df)
        if transformed_stock_df.empty and not sector_df.empty:
             logging.info("Only sector data to load.")
        elif transformed_stock_df.empty:
            logging.warning("No data to transform.")
            return
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return

    # Load Phase
    try:
        status = load_data(
            [transformed_stock_df, sector_df], 
            CREDENTIALS_DICT, PROJECT_ID, DATASET_ID, 
            [STOCKS_TABLE_ID, SECTORS_TABLE_ID]
        )
        if status == 'success':
            logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Error during data loading: {e}")

if __name__ == "__main__":
    main()