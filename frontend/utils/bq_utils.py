
import streamlit as st, pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone


GCP_PROJECT_ID = st.session_state["project_id"]
BQ_DATASET = st.session_state["cc_bq_dataset"]
BQ_TABLE = st.session_state["cc_bq_table"]
BQ_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


def get_bq_client():
    """Initializes and caches the BigQuery client."""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        try:
            client.get_dataset(BQ_DATASET)
        except NotFound:
            st.toast(f"Creating BigQuery dataset: {BQ_DATASET}")
            client.create_dataset(BQ_DATASET, exists_ok=True)
        return client
    except Exception as e:
        st.error(f"Could not connect to BigQuery. Please check your GCP authentication. Error: {e}")
        return None

def export_to_bigquery(df: pd.DataFrame, batch_id: str):
    """Exports the results DataFrame to a BigQuery table."""
    client = get_bq_client()
    if not client:
        st.warning("BigQuery client not available. Skipping export.")
        return

    df_to_load = df.copy()
    df_to_load["batch_id"] = batch_id
    df_to_load["run_timestamp"] = datetime.now(timezone.utc)

    schema = [
        bigquery.SchemaField("Compliance Query", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Response", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Explanation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Sources", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source_filename", "STRING"),
        bigquery.SchemaField("batch_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND", 
        create_disposition="CREATE_IF_NEEDED", 
    )

    try:
        st.toast("Exporting results to BigQuery...")
        job = client.load_table_from_dataframe(
            df_to_load, BQ_TABLE_ID, job_config=job_config
        )
        job.result() 
        st.success(f"Successfully exported results to BigQuery table: `{BQ_TABLE_ID}`")
        get_run_history.clear()
    except Exception as e:
        st.error(f"Failed to export data to BigQuery: {e}")


@st.cache_data(ttl=3600) 
def get_run_history():
    """Fetches a summary of all past runs from BigQuery."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    query = f"""
           SELECT
        batch_id,
        MAX(run_timestamp) as run_time,
        source_filename 
    FROM `{BQ_TABLE_ID}`
    GROUP BY
        batch_id,
        source_filename 
    ORDER BY run_time DESC
    LIMIT 100;
    """
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_history_details(batch_id: str):
    """Fetches the full details for a specific batch run."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    query = f"""
        SELECT
            `Compliance Query`,
            Response,
            Explanation,
            Sources,
            Status,
            source_filename,
            run_timestamp
        FROM `{BQ_TABLE_ID}`
        WHERE batch_id = @batch_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )
    try:
        query_job = client.query(query, job_config=job_config)
        return query_job.to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch history details: {e}")
        return pd.DataFrame()

def insert_entity_to_bq(project_id: str, dataset_id: str, table_name: str, entity_name: str, xml_block: str, embedding: list[float]) -> bool:
    """Inserts or updates an entity in a BigQuery table using a MERGE statement."""
    client = get_bq_client()
    if not client:
        st.error("BigQuery client not available.")
        return False

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    schema = [
        bigquery.SchemaField("complex_type_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("xml_block", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP", mode="REQUIRED"),
    ]

    # Create dataset and table if they don't exist
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        st.info(f"Dataset '{dataset_id}' not found. Creating it.")
        try:
            client.create_dataset(dataset_id, exists_ok=True)
            st.success(f"Dataset '{dataset_id}' created.")
        except Exception as e:
            st.error(f"Failed to create BigQuery dataset: {e}")
            return False
    try:
        client.get_table(table_id)
    except NotFound:
        st.info(f"Table {table_id} not found. Creating it.")
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.create_table(table)
            st.success(f"Table {table_id} created.")
        except Exception as e:
            st.error(f"Failed to create BigQuery table: {e}")
            return False

    # Use MERGE statement for upsert logic
    merge_query = f"""
    MERGE `{table_id}` T
    USING (
        SELECT
            @entity_name AS complex_type_name,
            @xml_block AS xml_block,
            @embedding AS embedding,
            @load_timestamp AS load_timestamp
    ) S
    ON T.complex_type_name = S.complex_type_name
    WHEN MATCHED THEN
        UPDATE SET
            xml_block = S.xml_block,
            embedding = S.embedding,
            load_timestamp = S.load_timestamp
    WHEN NOT MATCHED THEN
        INSERT (complex_type_name, xml_block, embedding, load_timestamp)
        VALUES (complex_type_name, xml_block, embedding, load_timestamp)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("entity_name", "STRING", entity_name),
            bigquery.ScalarQueryParameter("xml_block", "STRING", xml_block),
            bigquery.ArrayQueryParameter("embedding", "FLOAT64", embedding),
            bigquery.ScalarQueryParameter("load_timestamp", "TIMESTAMP", datetime.now(timezone.utc)),
        ]
    )

    try:
        query_job = client.query(merge_query, job_config=job_config)
        query_job.result()  # Wait for the job to complete

        if query_job.num_dml_affected_rows is not None and query_job.num_dml_affected_rows > 0:
            st.success(f"Successfully loaded/updated entity '{entity_name}' in BigQuery.")
        else:
            st.info(f"Entity '{entity_name}' data is already up-to-date in BigQuery.")
        return True

    except Exception as e:
        st.error(f"An error occurred during the BigQuery MERGE operation: {e}")
        return False
