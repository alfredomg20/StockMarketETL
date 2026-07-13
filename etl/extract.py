import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

def extract_data(
    existing_tickers: list = None,
    missing_tickers: list = None,
    period: str = 'max',
    interval: str = '1d',
    start_date: str = None,
    end: str = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts stock and sector data for a list of tickers.
    If a ticker is not found in the sector table, it will be fetched separately.

    Args:
        tickers_to_update (list): List of stock tickers to update.
        missing_tickers (list): List of tickers that are missing from the sector table.
        period (str): Time period for stock data (e.g., '5d', '1mo', '1y', 'max'). Default is 'max'.
        interval (str): Interval for stock data (e.g., '1d', '1h'). Default is '1d'.
        start_date (str): Start date for stock data in 'YYYY-MM-DD' format. Default is None.
        end (str): End date for stock data in 'YYYY-MM-DD' format. Default is None.
    
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Tuple containing two DataFrames:
            - stock_df: DataFrame with stock data.
            - sector_df: DataFrame with sector data.
    """
    existing_tickers = existing_tickers or []
    missing_tickers = missing_tickers or []
    
    all_stock_data = []
    all_sector_data = []
    failed_tickers = []

    logger.info("Fetching stock data...")
    try:
        if existing_tickers:
            if start_date:
                stock_data = fetch_stock_data(existing_tickers, interval=interval, start=start_date, end=end)
            else:
                stock_data = fetch_stock_data(existing_tickers, period=period, interval=interval)
            if not stock_data.empty:
                all_stock_data.append(stock_data)

        if missing_tickers:
            stock_data = fetch_stock_data(missing_tickers, period=period, interval=interval)
            if not stock_data.empty:
                all_stock_data.append(stock_data)
                
    except Exception as e:
        logger.error(f"Error fetching bulk stock data: {e}")

    # Fetch sector data only for missing tickers using parallel execution
    if missing_tickers:
        logger.info("Fetching sector data for missing tickers...")
        max_workers = min(15, len(missing_tickers))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ticker = {
                executor.submit(fetch_sector_data, ticker): ticker 
                for ticker in missing_tickers
            }
            
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    sector_data = future.result()
                    all_sector_data.append(sector_data)
                except Exception as e:
                    logger.error(f"Error fetching sector data for {ticker}: {e}")
                    failed_tickers.append(ticker)
                    all_sector_data.append({'ticker': ticker, 'sector': 'N/A'})

    # Combine all stock data
    stock_df = pd.concat(all_stock_data, ignore_index=True) if all_stock_data else pd.DataFrame()

    # Combine all sector data
    sector_df = pd.DataFrame(all_sector_data) if all_sector_data else pd.DataFrame()

    # Log failed tickers
    if failed_tickers:
        logger.warning(f"Failed to fetch data for the following tickers: {failed_tickers}")

    return stock_df, sector_df

def fetch_stock_data(ticker: str | list, period: str = 'max', interval: str = '1d', start: str = None, end: str = None) -> pd.DataFrame:
    """
    Fetches historical stock data for a given ticker.

    Args:
        ticker (str | list): Stock ticker symbol or a list of ticker symbols.
        period (str): Time period for stock data (e.g., '5d', '1mo', '1y', 'max'). Default is 'max'.
        interval (str): Interval for stock data (e.g., '1d', '1h'). Default is '1d'.
        start (str): Start date for stock data in 'YYYY-MM-DD' format. Default is None.
        end (str): End date for stock data in 'YYYY-MM-DD' format. Default is None.

    Returns:
        pd.DataFrame: DataFrame containing the stock data. Returns an empty DataFrame if no data is found.
    """
    try:
        if start:
            # Use start and end dates if provided
            df = yf.download(ticker, start=start, end=end, interval=interval, auto_adjust=True, group_by='ticker')
        else:
            # Use period if no start date is provided
            df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, group_by='ticker')

        if df.empty:
            logger.warning(f"No data found for {ticker}")
            return df
        
        # Reshape multi-ticker DataFrame into standard tabular format
        if isinstance(df.columns, pd.MultiIndex):
            df = df.stack(level=0, future_stack=True)
            df.index.names = ['Date', 'Ticker']
            df.reset_index(inplace=True)
        else:
            df.reset_index(inplace=True)
            df['Ticker'] = ticker if isinstance(ticker, str) else ticker[0]

        df.rename(columns={'Date': 'date'}, inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error during data fetch for {ticker}: {e}")
        return pd.DataFrame()

def fetch_sector_data(ticker: str) -> dict:
    """
    Fetches sector information for a given ticker.

    Args:
        ticker (str): Stock ticker symbol.

    Returns:
        dict: Dictionary containing the ticker and its sector information.
              If the sector information is unavailable, 'N/A' is returned as the sector.
    """
    try:
        info = yf.Ticker(ticker).info
        sector_data = {
            'ticker': ticker,
            'sector': info.get('sector', 'N/A'),
        }
        return sector_data
    except Exception as e:
        logger.error(f"Error fetching sector data for {ticker}: {e}")
        return {'ticker': ticker, 'sector': 'N/A'}