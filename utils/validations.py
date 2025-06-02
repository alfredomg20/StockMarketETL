import os
from google.cloud import bigquery

def get_latest_data_date(table_id: str, client: bigquery.Client, date_column: str) -> str:
    """
    Retrieves the most recent date from a BigQuery table, optionally filtered by ticker.

    Args:
        table_id (str): Full table ID in BigQuery (e.g., `project.dataset.table`).
        client (bigquery.Client): Authenticated BigQuery client.
        date_column (str): Name of the date column in the table.

    Returns:
        str: The most recent date in (YYYY-MM-DD format) from the specified column, or None if no data is found.
    """
    try:
        query = f"SELECT MAX({date_column}) AS max_date FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            return row.max_date
        return None
    except Exception:
        return None

def check_existing_tickers(table_id: str, client: bigquery.Client, tickers: list) -> list:
    """
    Checks wich tickers exist in a BigQuery table.

    Args:
        table_id (str): Full table ID in BigQuery (e.g., `project.dataset.table`).
        client (bigquery.Client): Authenticated BigQuery client.
        tickers (list): List of tickers to check.

    Returns:
        list: List of tickers that exist in the table.
    """
    try:
        query = f"""
            SELECT DISTINCT ticker
            FROM `{table_id}`
            WHERE ticker IN UNNEST(@tickers)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("tickers", "STRING", tickers)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        return [row.ticker for row in results]
    except Exception:
        return []
    
def check_env_variables(env_vars: list) -> list:
    """
    Checks if all required environment variables are set.

    Args:
        env_vars (list): List of required environment variable names.

    Returns:
        list: List of missing environment variables.
    """
    return [var for var in env_vars if not os.getenv(var)]