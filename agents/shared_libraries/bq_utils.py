from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone
import json
import os
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

def insert_sql_extract_to_bq(q_id: str, raw_sql_path: str, parser_output: dict, processing_status: str):
    """Inserts a record into the raw_sql_extracts table."""
    client = get_bq_client()
    if not client:
        print("BigQuery client not available. Skipping insert.")
        return False

    config = Settings.get_settings()
    table_id = f"{config.PROJECT_ID}.{config.RAW_SQL_EXTRACTS_DATASET}.{config.RAW_SQL_EXTRACTS_TABLE}"
    if raw_sql_path:
        file_name = os.path.basename(raw_sql_path)
    else:
        file_name = "ad-hoc-query"
        raw_sql_path = "ad-hoc-query"

    # Extract details from parser_output
    file_summary = parser_output.get("file_summary", {})
    query_inferred_detail = file_summary.get("inferred_detail")
    dependencies = file_summary.get("dependencies", [])

    query = f"""
    INSERT INTO `{table_id}` (q_id, raw_sql_path, file_name, parser_output, processing_status, query_inferred_detail, dependencies, inserted_at)
    VALUES (@q_id, @raw_sql_path, @file_name, @parser_output, @processing_status, @query_inferred_detail, @dependencies, @inserted_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("q_id", "STRING", q_id),
            bigquery.ScalarQueryParameter("raw_sql_path", "STRING", raw_sql_path),
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
            bigquery.ScalarQueryParameter("parser_output", "JSON", json.dumps(parser_output)),
            bigquery.ScalarQueryParameter("processing_status", "STRING", processing_status),
            bigquery.ScalarQueryParameter("query_inferred_detail", "STRING", query_inferred_detail),
            bigquery.ArrayQueryParameter("dependencies", "STRING", dependencies),
            bigquery.ScalarQueryParameter("inserted_at", "TIMESTAMP", datetime.now(timezone.utc).isoformat()),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        print(f"Successfully inserted record with q_id: {q_id}")
        return True
    except NotFound:
        print(f"Table {table_id} not found. Please create it.")
        # Here you could add logic to create the table if it doesn't exist.
        # For now, we just print an error.
        return False
    except Exception as e:
        print(f"An error occurred during the BigQuery insert operation: {e}")
        return False

def delete_analysis_data(q_id: str):
    """Deletes all analysis data for a given q_id from the related tables."""
    client = get_bq_client()
    if not client:
        print("BigQuery client not available.")
        return

    config = Settings.get_settings()
    project_id = config.PROJECT_ID
    dataset_id = config.RAW_SQL_EXTRACTS_DATASET

    tables_to_delete_from = [
        "query_statements",
        "statement_sources",
        "column_lineage",
        "statement_joins",
        "statement_filters",
    ]

    for table_name in tables_to_delete_from:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        query = f"DELETE FROM `{table_id}` WHERE q_id = @q_id"

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("q_id", "STRING", q_id),
            ]
        )

        try:
            client.query(query, job_config=job_config).result()
            print(f"Deleted data from {table_name} for q_id: {q_id}")
        except Exception as e:
            # Don't fail if a table doesn't exist
            if "Not found: Table" in str(e):
                print(f"Table {table_id} not found, skipping delete.")
                continue
            print(f"Failed to delete data from {table_name}: {e}")