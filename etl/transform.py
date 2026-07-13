import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw stock data by renaming columns to lowercase, 
    converting types, and handling potential null values.
    """
    try:
        logger.info("Starting data transformation...")
        if raw_df.empty:
            logger.warning("Received empty DataFrame for transformation.")
            return pd.DataFrame()

        # Convert columns to lowercase
        transformed_df = raw_df.rename(columns=str.lower)
        
        # Cast to appropriate types
        if 'date' in transformed_df.columns:
            transformed_df['date'] = pd.to_datetime(transformed_df['date'])

        num_cols = ['open', 'high', 'low', 'close', 'volume']
        existing_num_cols = [col for col in num_cols if col in transformed_df.columns]
        
        if existing_num_cols:
            transformed_df[existing_num_cols] = transformed_df[existing_num_cols].apply(
                pd.to_numeric, errors='coerce'
            )
        
        logger.info("Data transformation completed successfully.")
        return transformed_df

    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        return pd.DataFrame()