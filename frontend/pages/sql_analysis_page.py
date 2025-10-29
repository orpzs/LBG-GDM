import streamlit as st
from utils.init import init_session_state

init_session_state()
import requests
import os
import json
import pandas as pd
import hashlib
import time
import re
from utils.bq_utils import get_sql_extract, insert_df_to_bq, update_processing_status, delete_analysis_data, insert_raw_sql_extract_placeholder

st.set_page_config(layout="wide")

st.title("SQL Analysis")

# Custom CSS for Lloyds Banking Group colors
st.markdown("""
<style>
    .stButton>button {
        background-color: #006A4D;
        color: white;
    }
    .stButton>button:disabled {
        background-color: #F4F4F4;
        color: #A9A9A9;
    }
</style>
""", unsafe_allow_html=True)


def sanitize_filename(name):
    """
    Sanitizes a filename by replacing special characters with spaces.
    Keeps letters, numbers, dots, hyphens, and underscores.
    """
    return re.sub(r'[^a-zA-Z0-9._-]', ' ', name)

def get_q_id(file_name):
    return hashlib.sha256(file_name.encode()).hexdigest()

def remove_duplicates(dict_list):
    return [json.loads(s) for s in {json.dumps(d, sort_keys=True) for d in dict_list}]

def parse_and_load_data():
    parser_output = st.session_state.parser_output
    q_id = st.session_state.q_id

    if not parser_output or not q_id:
        st.error("No parser output to process.")
        return

    if isinstance(parser_output, str):
        try:
            parser_output = json.loads(parser_output)
        except json.JSONDecodeError:
            st.error(f"Failed to parse parser_output: {parser_output}")
            return

    statements = parser_output.get("statements", [])
    if not statements:
        st.info("No DML statements found in the SQL file.")
        return

    st.info("Parsing analysis output and loading data into BigQuery tables...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    total_steps = len(statements) * 5 # 5 tables per statement
    current_step = 0
    all_success = True

    for i, statement in enumerate(statements):
        s_id = statement.get("s_id")

        def process_table(table_name, df):
            nonlocal current_step
            status_text.text(f"Processing statement {i+1}/{len(statements)}: Populating {table_name}...")
            success = insert_df_to_bq(df, f"r2d2-00.gdm.{table_name}")
            current_step += 1
            progress_bar.progress(current_step / total_steps)
            return success

        # 1. query_statements
        query_statements_df = pd.DataFrame([{
            "q_id": q_id,
            "s_id": s_id,
            "inferred_detail": statement.get("inferred_detail"),
            "statement_type": statement.get("statement_type"),
            "target_database_name": statement.get("target_table", {}).get("database_name"),
            "target_schema_name": statement.get("target_table", {}).get("schema_name"),
            "target_table_name": statement.get("target_table", {}).get("table_name"),
            "target_table_alias": statement.get("target_table", {}).get("alias"),
            "inferred_target_type": statement.get("target_table", {}).get("inferred_target_type"),
        }])
        if not process_table("query_statements", query_statements_df):
            all_success = False
            break

        # 2. statement_sources
        sources = statement.get("sources", [])
        if sources:
            sources_df = pd.DataFrame(remove_duplicates(sources))
            sources_df.rename(columns={
                "alias": "source_alias", 
                "table_name": "source_table_name",
                "database_name": "source_database_name",
                "schema_name": "source_schema_name"
            }, inplace=True)
            sources_df["q_id"] = q_id
            sources_df["s_id"] = s_id
            if not process_table("statement_sources", sources_df):
                all_success = False
                break
        else: 
            current_step +=1
            progress_bar.progress(current_step / total_steps)

        # 3. column_lineage
        column_lineage = statement.get("column_lineage", [])
        if column_lineage:
            lineage_df = pd.DataFrame(remove_duplicates(column_lineage))
            lineage_df.rename(columns={"ordinal_position": "output_column_ordinal"}, inplace=True)
            lineage_df["q_id"] = q_id
            lineage_df["s_id"] = s_id
            if not process_table("column_lineage", lineage_df):
                all_success = False
                break
        else: 
            current_step +=1
            progress_bar.progress(current_step / total_steps)

        # 4. statement_joins
        joins = statement.get("joins", [])
        if joins:
            joins_df = pd.DataFrame(remove_duplicates(joins))
            joins_df["q_id"] = q_id
            joins_df["s_id"] = s_id
            if not process_table("statement_joins", joins_df):
                all_success = False
                break
        else: 
            current_step +=1
            progress_bar.progress(current_step / total_steps)

        # 5. statement_filters
        filters = statement.get("filters", [])
        if filters:
            filters_df = pd.DataFrame(remove_duplicates(filters))
            filters_df["q_id"] = q_id
            filters_df["s_id"] = s_id
            if not process_table("statement_filters", filters_df):
                all_success = False
                break
        else: 
            current_step +=1
            progress_bar.progress(current_step / total_steps)

    if all_success:
        status_text.success("All statements processed and data loaded into BigQuery.")
        progress_bar.progress(1.0)
        update_processing_status(q_id, "PROCESSED")
        st.session_state.processing_status = "PROCESSED"
    else:
        status_text.error("An error occurred during data loading. Please check the logs.")

def handle_analysis(uploaded_file):
    sanitized_name = sanitize_filename(uploaded_file.name)
    st.session_state.analysis_running = True
    st.session_state.parser_output = None
    st.session_state.error = None
    st.session_state.uploaded_file_name = sanitized_name
    st.session_state.q_id = get_q_id(sanitized_name)
    st.session_state.processing_status = None
    st.session_state.uploaded_file = uploaded_file

    try:
        with st.spinner(f"Checking for existing analysis of {sanitized_name}..."):
            extract = get_sql_extract(sanitized_name)
            if extract:
                st.session_state.processing_status = extract.get("processing_status")
                parser_output_str = extract.get("parser_output")
                loaded_output = None
                try:
                    loaded_output = json.loads(parser_output_str)
                    if isinstance(loaded_output, str):
                        loaded_output = json.loads(loaded_output)
                except (json.JSONDecodeError, TypeError):
                    loaded_output = parser_output_str # Keep as string if parsing fails

                if st.session_state.processing_status == "ERROR":
                    if isinstance(loaded_output, dict):
                        st.session_state.error = loaded_output.get("error", "Unknown error")
                    else:
                        st.session_state.error = str(loaded_output)
                else:
                    st.session_state.parser_output = loaded_output
                st.session_state.analysis_running = False
                return

        # Insert placeholder to prevent race conditions
        with st.spinner(f"Initiating analysis for {sanitized_name}..."):
            insert_raw_sql_extract_placeholder(st.session_state.q_id, sanitized_name)

        with st.spinner(f"Performing new analysis of {sanitized_name}..."):
            fastapi_url = os.environ.get("API_BASE_URL", "http://localhost:8000")
            files = {"file": (sanitized_name, uploaded_file.getvalue(), "text/plain")}
            response = requests.post(f"{fastapi_url}/sql_analysis_from_file", files=files)
            response.raise_for_status()
            
            for _ in range(3):
                extract = get_sql_extract(sanitized_name)
                if extract:
                    st.session_state.processing_status = extract.get("processing_status")
                    if st.session_state.processing_status == "ERROR":
                        st.session_state.error = json.loads(extract.get("parser_output")).get("error")
                    elif st.session_state.processing_status != "PARSING":
                        st.session_state.parser_output = json.loads(extract.get("parser_output"))
                    st.session_state.analysis_running = False
                    return
                time.sleep(2)
        
        st.session_state.error = "Could not retrieve analysis results after multiple attempts."

    except requests.exceptions.RequestException as e:
        st.session_state.error = f"API error: {e}"
    except Exception as e:
        st.session_state.error = f"An error occurred: {e}"
    
    st.session_state.analysis_running = False

def handle_reanalysis():
    st.session_state.analysis_running = True
    st.session_state.parser_output = None
    st.session_state.error = None
    
    try:
        with st.spinner(f"Deleting existing analysis data for {st.session_state.uploaded_file_name}..."):
            delete_analysis_data(st.session_state.q_id)
            st.session_state.processing_status = "NEW"

        with st.spinner(f"Performing new analysis of {st.session_state.uploaded_file_name}..."):
            uploaded_file = st.session_state.uploaded_file
            sanitized_name = st.session_state.uploaded_file_name
            fastapi_url = os.environ.get("API_BASE_URL", "http://localhost:8000")
            files = {"file": (sanitized_name, uploaded_file.getvalue(), "text/plain")}
            response = requests.post(f"{fastapi_url}/sql_analysis_from_file", files=files)
            response.raise_for_status()
            
            for _ in range(3):
                extract = get_sql_extract(st.session_state.uploaded_file_name)
                if extract and extract.get("processing_status") == "NEW":
                    st.session_state.parser_output = json.loads(extract.get("parser_output"))
                    st.session_state.processing_status = extract.get("processing_status")
                    st.session_state.analysis_running = False
                    return
                time.sleep(2)

        st.session_state.error = "Could not retrieve analysis results after multiple attempts."

    except requests.exceptions.RequestException as e:
        st.session_state.error = f"API error: {e}"
    except Exception as e:
        st.session_state.error = f"An error occurred during re-analysis: {e}"
    
    st.session_state.analysis_running = False

def clear_analysis_state():
    st.session_state.parser_output = None
    st.session_state.analysis_running = False
    st.session_state.q_id = None
    st.session_state.error = None
    st.session_state.processing_status = None
    st.session_state.uploaded_file_name = None
    st.session_state.uploaded_file = None
    st.session_state.file_uploader_key += 1
    st.rerun()

# --- Main UI ---

uploaded_files = st.file_uploader("Choose .sql files", type="sql", key=f"file_uploader_{st.session_state.file_uploader_key}", accept_multiple_files=True)

selected_file = None
if uploaded_files:
    file_map = {sanitize_filename(f.name): f for f in uploaded_files}
    sanitized_file_names = list(file_map.keys())

    if len(sanitized_file_names) != len(set(sanitized_file_names)):
        st.warning("Warning: Some files have the same name after sanitization. Please rename them to avoid conflicts.")

    selected_sanitized_name = st.radio("Select a file to analyze:", sanitized_file_names)
    selected_file = file_map.get(selected_sanitized_name)


# Top-level buttons

col1, col2 = st.columns(2)

with col1:
    if st.button("Analyze SQL", key="analyze_sql", use_container_width=True, disabled=(selected_file is None or st.session_state.analysis_running or st.session_state.parser_output is not None)):
        handle_analysis(selected_file)

with col2:
    if st.button("Analyze another file", key="analyze_another", use_container_width=True, disabled=(st.session_state.parser_output is None)):
        clear_analysis_state()

if st.session_state.analysis_running:
    st.info("Analysis in progress...")

if st.session_state.error:
    st.error(st.session_state.error)
    st.session_state.error = None # Clear error after displaying

if st.session_state.parser_output:
    expanded = st.session_state.processing_status in ["NEW", "ERROR"]

    if st.session_state.processing_status == "NEW":
        st.info("Found new analysis. Please verify the output below.")
        
        b1, b2 = st.columns(2)
        with b1:
            if st.button("Parse and Load", key="parse_and_load", use_container_width=True):
                parse_and_load_data()
        with b2:
            if st.button("Re-analyze", key="reanalyze_new", use_container_width=True):
                handle_reanalysis()

    elif st.session_state.processing_status == "PROCESSED":
        st.success(f"Analysis of {st.session_state.uploaded_file_name} complete.")
        st.info("This file has already been processed.")
        if st.button("Re-analyze", key="reanalyze_processed", use_container_width=True):
            handle_reanalysis()

    elif st.session_state.processing_status == "ERROR":
        st.error(f"Analysis of {st.session_state.uploaded_file_name} failed.")
        st.error(st.session_state.error)

    with st.expander("View Raw Parser Output", expanded=expanded):
        st.json(st.session_state.parser_output)
