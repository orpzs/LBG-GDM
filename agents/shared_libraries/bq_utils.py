from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone
import json
from config.settings import Settings

def get_bq_client():
    """Initializes the BigQuery client."""
    try:
        config = Settings.get_settings()
        client = bigquery.Client(project=config.PROJECT_ID)
        return client
    except Exception as e:
        print(f"Could not connect to BigQuery. Please check your GCP authentication. Error: {e}")
        return None

def insert_sql_extract_to_bq(sql_id: str, raw_sql_text: str, parser_output: dict, processing_status: str):
    """Inserts a record into the raw_sql_extracts table."""
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping insert.")
        return False

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.RAW_SQL_EXTRACTS_DATASET}.{config.RAW_SQL_EXTRACTS_TABLE}"

    rows_to_insert = [
        {
            "sql_id": sql_id,
            "raw_sql_text": raw_sql_text,
            "parser_output": json.dumps(parser_output),
            "processing_status": processing_status,
            "inserted_at": datetime.now(timezone.utc).isoformat(),
        }
    ]

    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            print(f"Successfully inserted record with sql_id: {sql_id}")
            return True
        else:
            print(f"Failed to insert record into BigQuery: {errors}")
            return False
    except NotFound:
        print(f"Table {table_id} not found. Please create it.")
        # Here you could add logic to create the table if it doesn't exist.
        # For now, we just print an error.
        return False
    except Exception as e:
        print(f"An error occurred during the BigQuery insert operation: {e}")
        return False
