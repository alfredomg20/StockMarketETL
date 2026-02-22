from datetime import datetime
from google.cloud import bigquery

def get_latest_data_date(table_id: str, client: bigquery.Client, date_column: str) -> str | datetime:
    """ Retrieves the latest date from a specified date column in a BigQuery table."""
    try:
        query = f"SELECT MAX({date_column}) AS max_date FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.result()
        for row in result:
            return row.max_date
        return None
    except Exception:
        return None