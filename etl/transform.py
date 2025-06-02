import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw stock data by renaming columns to lowercase and converting the 'date' column to datetime.

    Args:
        raw_df (pd.DataFrame): Raw DataFrame containing stock data with columns to be transformed.

    Returns:
        pd.DataFrame: Transformed DataFrame with lowercase column names and a datetime 'date' column.
    """
    try:
        rename_dict = {col: col.lower() for col in raw_df.columns}
        transformed_df = raw_df.rename(columns=rename_dict)
        transformed_df['date'] = pd.to_datetime(transformed_df['date'])
        return transformed_df
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return pd.DataFrame()