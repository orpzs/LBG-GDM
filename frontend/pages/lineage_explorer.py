import streamlit as st
import pandas as pd
from frontend.utils.bq_utils import (
    get_all_sql_extracts,
    get_tables_for_qid,
    get_statements_for_qids,
    get_sources_for_sids,
    get_column_lineage_for_sids,
)

st.set_page_config(layout="wide")
st.title("Lineage Explorer")

# Get all SQL extracts
extracts_df = get_all_sql_extracts()

if extracts_df.empty:
    st.warning("No SQL extracts found. Please analyze some SQL files first.")
    st.stop()

files_df = extracts_df.copy()
files_df["dependencies"] = files_df["dependencies"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
files_df["Select"] = False

# Display the files in a table with checkboxes
st.write("Select files to trace:")
edited_df = st.data_editor(
    files_df.rename(columns={
        "file_name": "File Name",
        "dependencies": "Dependencies",
        "query_inferred_detail": "Inferred Details",
        "processing_status": "Processing Status",
    }), 
    use_container_width=True, 
    hide_index=True,
    column_order=("Select", "File Name", "Dependencies", "Inferred Details", "Processing Status"),
    disabled=["File Name", "q_id", "Dependencies", "Inferred Details", "Processing Status"]
)

if st.button("Trace"):
    selected_files_df = edited_df[edited_df["Select"]]

    if not selected_files_df.empty:
        for index, row in selected_files_df.iterrows():
            selected_file = row["File Name"]
            selected_qid = row["q_id"]

            st.subheader(f"Lineage for {selected_file}")

            # Get tables for the selected file
            tables_df = get_tables_for_qid(selected_qid)

            if not tables_df.empty:
                selected_qids = [selected_qid]
                
                # Get statements for selected files
                statements_df = get_statements_for_qids(selected_qids)
                
                if not statements_df.empty:
                    st.header("Statements")
                    st.dataframe(statements_df)

                    selected_sids = statements_df["s_id"].tolist()

                    # Get sources for selected statements
                    sources_df = get_sources_for_sids(selected_qids, selected_sids)
                    if not sources_df.empty:
                        st.header("Source Tables")
                        st.dataframe(sources_df)

                    # Get column lineage for selected statements
                    column_lineage_df = get_column_lineage_for_sids(selected_qids, selected_sids)
                    if not column_lineage_df.empty:
                        st.header("Column Lineage")
                        st.dataframe(column_lineage_df)
                else:
                    st.info("No statements found for the selected file.")
            else:
                st.info("No tables found for the selected file.")
    else:
        st.warning("Please select at least one SQL file.")
