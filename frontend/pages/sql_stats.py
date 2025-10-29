import streamlit as st
import pandas as pd
from utils.init import init_session_state
from utils.bq_utils import get_all_source_tables, get_source_column_usage, get_all_joins

init_session_state()

st.set_page_config(layout="wide")
st.title("SQL Stats")

# Custom CSS for the green button
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

st.header("Source Column Usage")

@st.cache_data
def get_all_tables():
    """Fetches all unique source tables."""
    return get_all_source_tables()

all_tables_df = get_all_tables()

if all_tables_df.empty:
    st.warning("No source tables found. Please analyze some SQL files first.")
    st.stop()

# Rename columns for display
rename_map = {
    "source_database_name": "Database",
    "source_table_name": "Table",
}
display_tables_df = all_tables_df.rename(columns=rename_map)

if 'Select' not in display_tables_df.columns:
    display_tables_df.insert(0, 'Select', False)

col1, col2 = st.columns([0.8, 0.2])
with col1:
    select_all = st.checkbox("Select All")

if select_all:
    display_tables_df['Select'] = True

edited_tables_df = st.data_editor(
    display_tables_df,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Select": st.column_config.CheckboxColumn(
            "Select",
            default=False,
            width="small"
        )
    },
    disabled=["Database", "Table"],
    key="tables_selector"
)

if st.button("Find Columns Used", use_container_width=True):
    selected_tables = edited_tables_df[edited_tables_df['Select']]

    if selected_tables.empty:
        st.warning("Please select at least one table.")
    else:
        # Get the usage counts for all source columns
        source_column_usage_df = get_source_column_usage()

        if source_column_usage_df.empty:
            st.info("No column usage information found.")
        else:
            # Prepare the selected table names for filtering
            selected_tables_list = selected_tables.to_dict('records')
            
            # Create a unique identifier for each table (db.table)
            selected_identifiers = {f"{row['Database']}.{row['Table']}" for row in selected_tables_list}
            
            source_column_usage_df['identifier'] = source_column_usage_df['source_database_name'] + "." + source_column_usage_df['source_table_name']
            
            # Filter the usage dataframe
            filtered_usage_df = source_column_usage_df[source_column_usage_df['identifier'].isin(selected_identifiers)]
            
            if filtered_usage_df.empty:
                st.info("No column usage found for the selected tables.")
            else:
                # Rename columns for display
                display_usage_df = filtered_usage_df.rename(columns={
                    "source_database_name": "Database",
                    "source_table_name": "Table",
                    "column_name": "Column Name",
                    "usage_count": "Usage Count"
                })
                
                # Select and order columns for display
                display_usage_df = display_usage_df[["Database", "Table", "Column Name", "Usage Count"]]
                display_usage_df = display_usage_df.reset_index(drop=True)
                
                st.dataframe(display_usage_df, use_container_width=True, hide_index=True)

                csv = display_usage_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥",
                    data=csv,
                    file_name="source_column_usage.csv",
                    mime="text/csv",
                    key="download_csv",
                    help="Download as CSV"
                )

st.divider()
st.header("Common Joins")

# Get the list of tables for dropdowns
if not all_tables_df.empty:
    table_list = [f"{row.source_database_name}.{row.source_table_name}" for row in all_tables_df.itertuples()]
    
    col1, col2 = st.columns(2)
    with col1:
        left_table = st.selectbox("Select Left Table", options=table_list, index=0)
    with col2:
        right_table = st.selectbox("Select Right Table", options=table_list, index=1 if len(table_list) > 1 else 0)

    if st.button("Find Join Usage", use_container_width=True):
        if left_table and right_table:
            all_joins_df = get_all_joins()

            if not all_joins_df.empty:
                left_db, left_tbl = left_table.split('.', 1)
                right_db, right_tbl = right_table.split('.', 1)

                # Filter for joins between the two tables, in either direction
                condition1 = (all_joins_df['left_database_name'] == left_db) & (all_joins_df['left_table_name'] == left_tbl) & (all_joins_df['right_database_name'] == right_db) & (all_joins_df['right_table_name'] == right_tbl)
                condition2 = (all_joins_df['left_database_name'] == right_db) & (all_joins_df['left_table_name'] == right_tbl) & (all_joins_df['right_database_name'] == left_db) & (all_joins_df['right_table_name'] == left_tbl)
                
                filtered_joins = all_joins_df[condition1 | condition2]

                if not filtered_joins.empty:
                    st.dataframe(filtered_joins.reset_index(drop=True), use_container_width=True, hide_index=True)
                else:
                    st.info(f"No direct joins found between {left_table} and {right_table}.")
            else:
                st.info("No join information available.")
