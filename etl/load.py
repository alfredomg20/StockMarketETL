from typing import List
import pandas as pd
from google.cloud import bigquery
from utils.google_cloud import get_bigquery_client
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_data(dataframes: List[pd.DataFrame], credentials_dict: dict, project_id: str, dataset_id: str, table_ids: List[str], write_disposition: str = "WRITE_APPEND") -> str:
    """
    Loads the transformed stock and sector data into BigQuery.

    Args:
        dataframes (List[pd.DataFrame]): List of DataFrames to load into BigQuery.
        credentials_dict (dict): Dictionary containing service account credentials.
        project_id (str): Google Cloud project ID.
        dataset_id (str): The ID of the dataset in BigQuery.
        table_ids (List[str]): List of table IDs in BigQuery corresponding to the DataFrames.
        write_disposition (str): BigQuery write disposition (e.g., 'WRITE_APPEND', 'WRITE_TRUNCATE').

    Returns:
        str: 'success' if all dataframes are loaded successfully, 'failure' otherwise.
    """
    if not dataframes or not table_ids:
        raise ValueError("DataFrames and table IDs cannot be empty.")
    if len(dataframes) != len(table_ids):
        raise ValueError("The number of DataFrames must match the number of table IDs.")

    client = get_bigquery_client(credentials_dict, project_id)
    create_dataset(client, dataset_id)

    for df, table_id in zip(dataframes, table_ids):
        if df.empty:
            continue
        try:
            table_id_full = f"{project_id}.{dataset_id}.{table_id}"
            load_df_to_bigquery(df, table_id_full, client, write_disposition=write_disposition)
        except Exception as e:
            logging.error(f"Error loading data into table {table_id}: {e}")
            continue
    return 'success'

def load_df_to_bigquery(df: pd.DataFrame, table_id: str, client: bigquery.Client, write_disposition: str = "WRITE_APPEND") -> None:
    """
    Loads a DataFrame directly into a BigQuery table.

    Args:
        df (pd.DataFrame): DataFrame to load into BigQuery.
        table_id (str): The ID of the BigQuery table to load data into.
        client (bigquery.Client): An authenticated BigQuery client instance.
        write_disposition (str): BigQuery write disposition (e.g., 'WRITE_APPEND', 'WRITE_TRUNCATE').

    Returns:
        None
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

def create_dataset(client: bigquery.Client, dataset_id: str) -> None:
    """
    Creates a BigQuery dataset if it does not already exist.

    Args:
        client (bigquery.Client): An authenticated BigQuery client instance.
        dataset_id (str): The ID of the dataset to create.
     
    Returns:
        None
    """
    datasets = list(client.list_datasets())
    if any(dataset.dataset_id == dataset_id for dataset in datasets):
        return
    
    try:
        dataset_ref = client.dataset(dataset_id)
        client.create_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} created.")
    except Exception as e:
        logging.error(f"Error creating dataset {dataset_id}: {e}")