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


def get_all_xml_blocks(project_id: str, dataset_id: str, table_name: str) -> pd.DataFrame:
    """Fetches all XML blocks from the specified BigQuery table."""
    client = get_bq_client()
    if not client:
        st.error("BigQuery client not available.")
        return pd.DataFrame()

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    query = f"SELECT complex_type_name, xml_block FROM `{table_id}`"

    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        # If the table doesn't exist, return an empty DataFrame
        if "Not found: Table" in str(e):
            return pd.DataFrame(columns=['complex_type_name', 'xml_block'])
        st.error(f"Failed to fetch XML blocks from BigQuery: {e}")
        return pd.DataFrame()

def get_guidelines_table_id():
    """Constructs the full BigQuery table ID for guidelines."""
    project_id = st.session_state.get("project_id", "default-project")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_name = st.session_state.get("guidelines_bq_table", "guidelines")
    return f"{project_id}.{dataset_id}.{table_name}"

def create_guidelines_table_if_not_exists():
    """Creates the guidelines table in BigQuery if it doesn't already exist."""
    client = get_bq_client()
    if not client:
        return

    table_id = get_guidelines_table_id()
    try:
        client.get_table(table_id)
    except NotFound:
        st.toast(f"Creating BigQuery table for guidelines: {table_id}")
        schema = [
            bigquery.SchemaField("guideline_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("guideline_text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_active", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        st.success(f"Successfully created table: `{table_id}`")

def get_all_guidelines() -> pd.DataFrame:
    """Fetches all guidelines from the BigQuery table."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    table_id = get_guidelines_table_id()
    query = f"""SELECT guideline_id, guideline_text, is_active, created_at, updated_at
    FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY guideline_text ORDER BY updated_at DESC) as rn
        FROM `{table_id}`
    ) WHERE rn = 1"""
    
    try:
        return client.query(query).to_dataframe()
    except Exception:
        # Return empty dataframe if table doesn't exist
        return pd.DataFrame()


def add_guideline(guideline_text: str):
    """Adds a new guideline to the BigQuery table."""
    client = get_bq_client()
    if not client:
        return

    table_id = get_guidelines_table_id()
    now = datetime.now(timezone.utc)
    guideline_id = f"guideline_{int(now.timestamp() * 1000)}"

    query = f"""
    INSERT INTO `{table_id}` (guideline_id, guideline_text, is_active, created_at, updated_at)
    VALUES (@guideline_id, @guideline_text, @is_active, @created_at, @updated_at)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("guideline_id", "STRING", guideline_id),
            bigquery.ScalarQueryParameter("guideline_text", "STRING", guideline_text),
            bigquery.ScalarQueryParameter("is_active", "BOOLEAN", True),
            bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", now.isoformat()),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now.isoformat()),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        st.success("Guideline added successfully!")
    except Exception as e:
        st.error(f"Failed to add guideline: {e}")

def update_guideline(guideline_id: str, new_text: str, is_active: bool):
    """Updates an existing guideline in the BigQuery table."""
    client = get_bq_client()
    if not client:
        return

    table_id = get_guidelines_table_id()
    now = datetime.now(timezone.utc)

    query = f"""
    UPDATE `{table_id}`
    SET guideline_text = @new_text, is_active = @is_active, updated_at = @updated_at
    WHERE guideline_id = @guideline_id
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("new_text", "STRING", new_text),
            bigquery.ScalarQueryParameter("is_active", "BOOL", is_active),
            bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", now.isoformat()),
            bigquery.ScalarQueryParameter("guideline_id", "STRING", guideline_id),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        st.success("Guideline updated successfully!")
    except Exception as e:
        st.error(f"Failed to update guideline: {e}")

def delete_guideline(guideline_id: str):
    """Deletes a guideline from the BigQuery table."""
    client = get_bq_client()
    if not client:
        return

    table_id = get_guidelines_table_id()
    query = f"DELETE FROM `{table_id}` WHERE guideline_id = @guideline_id"
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("guideline_id", "STRING", guideline_id),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        st.success("Guideline deleted successfully!")
    except Exception as e:
        st.error(f"Failed to delete guideline: {e}")

def delete_analysis_data(q_id: str):
    """Deletes all analysis data for a given q_id from the related tables."""
    client = get_bq_client()
    if not client:
        st.error("BigQuery client not available.")
        return

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    
    tables_to_delete_from = [
        "query_statements",
        "statement_sources",
        "column_lineage",
        "statement_joins",
        "statement_filters",
         "raw_sql_extracts"
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
            st.toast(f"Deleted data from {table_name} for q_id: {q_id}")
        except Exception as e:
            # Don't fail if a table doesn't exist
            if "Not found: Table" in str(e):
                st.warning(f"Table {table_id} not found, skipping delete.")
                continue
            st.error(f"Failed to delete data from {table_name}: {e}")


def delete_raw_sql_extract(q_id: str):
    """Deletes a raw_sql_extract for a given q_id."""
    client = get_bq_client()
    if not client:
        st.error("BigQuery client not available.")
        return

    table_id = get_raw_sql_extracts_table_id()
    query = f"DELETE FROM `{table_id}` WHERE q_id = @q_id"
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("q_id", "STRING", q_id),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
        st.toast(f"Deleted data from raw_sql_extracts for q_id: {q_id}")
    except Exception as e:
        st.error(f"Failed to delete data from raw_sql_extracts: {e}")


def get_raw_sql_extracts_table_id():
    """Constructs the full BigQuery table ID for raw_sql_extracts."""
    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_name = st.session_state.get("raw_sql_extracts_bq_table", "raw_sql_extracts")
    return f"{project_id}.{dataset_id}.{table_name}"

def get_sql_extract(file_name: str):
    """Fetches the processing status and parser output for a given file name from the raw_sql_extracts table."""
    client = get_bq_client()
    if not client:
        return None

    table_id = get_raw_sql_extracts_table_id()
    query = f"""
        SELECT
            processing_status,
            parser_output
        FROM `{table_id}`
        WHERE file_name = @file_name
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        results = query_job.to_dataframe()
        if not results.empty:
            return results.to_dict('records')[0]
        return None
    except Exception as e:
        st.error(f"Could not fetch SQL extract details: {e}")
        return None

def insert_df_to_bq(df: pd.DataFrame, table_id: str) -> bool:
    """Inserts a DataFrame into a BigQuery table."""
    client = get_bq_client()
    if not client:
        st.warning("BigQuery client not available. Skipping insert.")
        return False

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        return True
    except Exception as e:
        st.error(f"Failed to insert data into BigQuery: {e}")
        return False

def update_processing_status(q_id: str, status: str):
    """Updates the processing status of a record in the raw_sql_extracts table."""
    client = get_bq_client()
    if not client:
        return

    table_id = get_raw_sql_extracts_table_id()
    query = f"""
        UPDATE `{table_id}`
        SET processing_status = @status, processed_at = @processed_at
        WHERE q_id = @q_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("processed_at", "TIMESTAMP", datetime.now(timezone.utc)),
            bigquery.ScalarQueryParameter("q_id", "STRING", q_id),
        ]
    )

    try:
        client.query(query, job_config=job_config).result()
    except Exception as e:
        st.error(f"Failed to update processing status: {e}")

@st.cache_data(ttl=3600)
def get_all_sql_extracts():
    """Fetches all records from the raw_sql_extracts table."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    table_id = get_raw_sql_extracts_table_id()
    # Assuming 'inserted_at' is a column in your table for ordering
    query = f"""
        SELECT
          file_name,
          q_id,
          query_inferred_detail,
          processing_status,
          processed_at,
          dependencies
        FROM `{table_id}`
        ORDER BY file_name
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch SQL extracts: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_tables_for_qid(q_ids: list[str]) -> pd.DataFrame:
    """Fetches all tables for a given list of q_ids."""
    client = get_bq_client()
    if not client or not q_ids:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_id = f"{project_id}.{dataset_id}.query_statements"
    raw_sql_extracts_table_id = get_raw_sql_extracts_table_id()

    query = f"""
        SELECT DISTINCT
            qs.q_id,
            rse.file_name,
            qs.target_database_name,
            qs.target_schema_name,
            qs.target_table_name,
            qs.inferred_target_type
        FROM `{table_id}` AS qs
        LEFT JOIN `{raw_sql_extracts_table_id}` AS rse ON qs.q_id = rse.q_id
        WHERE qs.q_id IN UNNEST(@q_ids)
        ORDER BY
            qs.inferred_target_type,
            qs.target_table_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("q_ids", "STRING", q_ids),
        ]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch tables for q_ids {q_ids}: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_statements_for_qids(q_ids: list[str]) -> pd.DataFrame:
    """Fetches all statements for a given list of q_ids."""
    client = get_bq_client()
    if not client or not q_ids:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_id = f"{project_id}.{dataset_id}.query_statements"

    query = f"""
        SELECT
            q_id,
            s_id,
            statement_type,
            target_table_name
        FROM `{table_id}`
        WHERE q_id IN UNNEST(@q_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("q_ids", "STRING", q_ids),
        ]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch statements: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_sources_for_sids(q_ids: list[str], s_ids: list[str]) -> pd.DataFrame:
    """Fetches all sources for a given list of s_ids."""
    client = get_bq_client()
    if not client or not s_ids or not q_ids:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_id = f"{project_id}.{dataset_id}.statement_sources"

    query = f"""
        SELECT
            q_id,
            s_id,
            source_id,
            source_database_name,
            source_table_name,
            source_alias,
            source_type
        FROM `{table_id}`
        WHERE q_id IN UNNEST(@q_ids) AND s_id IN UNNEST(@s_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("q_ids", "STRING", q_ids),
            bigquery.ArrayQueryParameter("s_ids", "STRING", s_ids),
        ]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch sources: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_column_lineage_for_sids(q_ids: list[str], s_ids: list[str]) -> pd.DataFrame:
    """Fetches all column lineage for a given list of s_ids."""
    client = get_bq_client()
    if not client or not s_ids or not q_ids:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_id = f"{project_id}.{dataset_id}.column_lineage"

    query = f"""
        SELECT
            q_id,
            s_id,
            output_column_name,
            transformation_logic,
            source_references
        FROM `{table_id}`
        WHERE q_id IN UNNEST(@q_ids) AND s_id IN UNNEST(@s_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("q_ids", "STRING", q_ids),
            bigquery.ArrayQueryParameter("s_ids", "STRING", s_ids),
        ]
    )
    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch column lineage: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_recursive_lineage_for_tables(selected_target_tables: list, selected_qids: list[str]) -> pd.DataFrame:
    """Fetches the recursive column lineage for a given list of tables."""
    client = get_bq_client()
    if not client or not selected_target_tables:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")

    struct_query_params = []
    for table in selected_target_tables:
        db_name = table.get("target_database_name")
        schema_name = table.get("target_schema_name")
        table_name = table.get("target_table_name")

        # Create ScalarQueryParameter for each field in the struct
        db_param = bigquery.ScalarQueryParameter("target_database_name", "STRING", db_name if pd.notna(db_name) else None)
        schema_param = bigquery.ScalarQueryParameter("target_schema_name", "STRING", schema_name if pd.notna(schema_name) else None)
        table_param = bigquery.ScalarQueryParameter("target_table_name", "STRING", table_name if pd.notna(table_name) else None)

        # Create a StructQueryParameter for the current table entry
        struct_query_params.append(
            bigquery.StructQueryParameter(None, db_param, schema_param, table_param)
        )

    query = f"""        WITH RECURSIVE
        all_column_links AS (
          -- This CTE flattens all known column links from your metadata
          SELECT
            q.q_id,
            q.s_id,
            q.target_database_name,
            q.target_schema_name,
            q.target_table_name,
            l.output_column_name AS target_column_name,
            l.transformation_logic,
            s.source_database_name,
            s.source_schema_name,
            s.source_table_name,
            source_ref.column_name AS source_column_name,
            s.source_type
          FROM
            `{project_id}.{dataset_id}.query_statements` AS q
          JOIN
            `{project_id}.{dataset_id}.column_lineage` AS l
            ON q.q_id = l.q_id AND q.s_id = l.s_id
          LEFT JOIN
            UNNEST(l.source_references) AS source_ref
          LEFT JOIN
            `{project_id}.{dataset_id}.statement_sources` AS s
            ON q.q_id = s.q_id
            AND q.s_id = s.s_id
            AND source_ref.source_id = s.source_id
          WHERE q.q_id IN UNNEST(@selected_qids)
        ),

        selected_tables AS (
          -- This CTE unnests the array of tables selected by the user in the UI
          SELECT t.*
          FROM UNNEST(@selected_target_tables) AS t
          -- @selected_target_tables will be an ARRAY<STRUCT<...>>
        ),

        lineage_trace AS (
          -- === ANCHOR MEMBER (Level 1) ===
          -- This starts the trace from the tables the user selected
          SELECT
            1 AS depth,
            lnk.q_id,
            lnk.s_id,
            lnk.target_database_name,
            lnk.target_schema_name,
            lnk.target_table_name,
            lnk.target_column_name,
            lnk.transformation_logic,
            lnk.source_database_name,
            lnk.source_schema_name,
            lnk.source_table_name,
            lnk.source_column_name,
            lnk.source_type,
            -- This path array is used to prevent infinite loops
            [
              COALESCE(lnk.target_database_name, '') || '.' ||
              COALESCE(lnk.target_table_name, '') || '.' ||
              COALESCE(lnk.target_column_name, '')
            ] AS trace_path
          FROM
            all_column_links AS lnk
          -- This JOIN is the dynamic part
          JOIN
            selected_tables AS s
            ON lnk.target_database_name = s.target_database_name
            AND lnk.target_table_name = s.target_table_name
            -- IS NOT DISTINCT FROM safely handles NULL schema names
            AND lnk.target_schema_name IS NOT DISTINCT FROM s.target_schema_name

          UNION ALL

          -- === RECURSIVE MEMBER (Level 2+) ===
          -- This "hops" from the previous step's source to the next step's target
          SELECT
            prev_hop.depth + 1,
            next_hop.q_id,
            next_hop.s_id,
            next_hop.target_database_name,
            next_hop.target_schema_name,
            next_hop.target_table_name,
            next_hop.target_column_name,
            next_hop.transformation_logic,
            next_hop.source_database_name,
            next_hop.source_schema_name,
            next_hop.source_table_name,
            next_hop.source_column_name,
            next_hop.source_type,
            -- Add the current hop to our path
            ARRAY_CONCAT(
              prev_hop.trace_path,
              [
                COALESCE(next_hop.target_database_name, '') || '.' ||
                COALESCE(next_hop.target_table_name, '') || '.' ||
                COALESCE(next_hop.target_column_name, '')
              ]
            ) AS trace_path
          FROM
            all_column_links AS next_hop
          JOIN
            lineage_trace AS prev_hop
            -- This is the "HOP"
            ON next_hop.target_database_name = prev_hop.source_database_name
            AND next_hop.target_table_name = prev_hop.source_table_name
            AND next_hop.target_column_name = prev_hop.source_column_name
            AND next_hop.target_schema_name IS NOT DISTINCT FROM prev_hop.source_schema_name
          WHERE
            -- Cycle Prevention
            (
              COALESCE(next_hop.target_database_name, '') || '.' ||
              COALESCE(next_hop.target_table_name, '') || '.' ||
              COALESCE(next_hop.target_column_name, '')
            )
            NOT IN UNNEST(prev_hop.trace_path)
        )

        /*
        Step 3: Select the final, complete lineage trace
        */
        SELECT
          depth,
          q_id,
          s_id,
          target_database_name,
          target_schema_name,
          target_table_name,
          target_column_name AS target_column,
          transformation_logic,
          source_database_name,
          source_schema_name,
          source_table_name,
          source_column_name AS source_column,
          source_type,
          trace_path
        FROM
          lineage_trace
        -- Order by the path and depth to see each trace from start to finish
        ORDER BY
          depth;
    """

    # Define the type of the STRUCTs within the ARRAY
    struct_type = bigquery.StructQueryParameterType(
        bigquery.ScalarQueryParameterType('STRING', name='target_database_name'),
        bigquery.ScalarQueryParameterType('STRING', name='target_schema_name'),
        bigquery.ScalarQueryParameterType('STRING', name='target_table_name')
    )

    # Create the ArrayQueryParameter
    selected_tables_param = bigquery.ArrayQueryParameter(
        'selected_target_tables',
        struct_type,
        struct_query_params  # Use the list of StructQueryParameter objects
    )

    qids_param = bigquery.ArrayQueryParameter(
        'selected_qids',
        'STRING',
        selected_qids
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[selected_tables_param, qids_param]
    )

    try:
        return client.query(query, job_config=job_config).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch recursive lineage: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_all_source_tables() -> pd.DataFrame:
    """Fetches all distinct source tables from the statement_sources table."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    table_id = f"{project_id}.{dataset_id}.statement_sources"

    query = f"""
        SELECT DISTINCT
            source_database_name,
            source_table_name
        FROM `{table_id}`
        WHERE source_table_name IS NOT NULL
        ORDER BY 1, 2
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch source tables: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_source_column_usage() -> pd.DataFrame:
    """Calculates the usage count for each source column."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    lineage_table = f"{project_id}.{dataset_id}.column_lineage"
    sources_table = f"{project_id}.{dataset_id}.statement_sources"

    query = f"""
        SELECT
            s.source_database_name,
            s.source_table_name,
            ref.column_name,
            COUNT(*) AS usage_count
        FROM
            `{lineage_table}` AS l,
            UNNEST(l.source_references) AS ref
        JOIN
            `{sources_table}` AS s
            ON l.q_id = s.q_id AND l.s_id = s.s_id AND ref.source_id = s.source_id
        WHERE
            s.source_table_name IS NOT NULL AND ref.column_name IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch source column usage: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_all_joins() -> pd.DataFrame:
    """Fetches all joins from the statement_joins table and counts their occurrences."""
    client = get_bq_client()
    if not client:
        return pd.DataFrame()

    project_id = st.session_state.get("project_id", "r2d2-00")
    dataset_id = st.session_state.get("guidelines_bq_dataset", "gdm")
    joins_table = f"{project_id}.{dataset_id}.statement_joins"
    sources_table = f"{project_id}.{dataset_id}.statement_sources"

    query = f"""
        SELECT
            ls.source_database_name AS left_database_name,
            ls.source_table_name AS left_table_name,
            jc.left_column,
            rs.source_database_name AS right_database_name,
            rs.source_table_name AS right_table_name,
            jc.right_column,
            j.join_type,
            jc.operator,
            COUNT(*) AS usage_count
        FROM
            `{joins_table}` AS j,
            UNNEST(j.join_conditions) AS jc
        JOIN
            `{sources_table}` AS ls
            ON j.q_id = ls.q_id AND j.s_id = ls.s_id AND j.left_source_id = ls.source_id
        JOIN
            `{sources_table}` AS rs
            ON j.q_id = rs.q_id AND j.s_id = rs.s_id AND j.right_source_id = rs.source_id
        WHERE
            ls.source_table_name IS NOT NULL AND rs.source_table_name IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ORDER BY usage_count DESC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Could not fetch joins: {e}")
        return pd.DataFrame()

def insert_raw_sql_extract_placeholder(q_id: str, file_name: str):
    """Inserts a placeholder record into the raw_sql_extracts table with 'PARSING' status."""
    client = get_bq_client()
    if not client:
        st.warning("BigQuery client not available. Skipping placeholder insert.")
        return False

    table_id = get_raw_sql_extracts_table_id()

    # Use MERGE to avoid errors if the record already exists
    merge_query = f"""
    MERGE `{table_id}` T
    USING (SELECT @q_id AS q_id) S
    ON T.q_id = S.q_id
    WHEN NOT MATCHED THEN
      INSERT (q_id, raw_sql_path, file_name, processing_status, inserted_at)
      VALUES (@q_id, @file_name, @file_name, 'PARSING', @inserted_at)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("q_id", "STRING", q_id),
            bigquery.ScalarQueryParameter("file_name", "STRING", file_name),
            bigquery.ScalarQueryParameter("inserted_at", "TIMESTAMP", datetime.now(timezone.utc)),
        ]
    )

    try:
        client.query(merge_query, job_config=job_config).result()
        return True
    except Exception as e:
        st.error(f"Failed to insert placeholder for {file_name}: {e}")
        return False