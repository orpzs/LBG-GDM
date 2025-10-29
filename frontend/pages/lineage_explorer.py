import streamlit as st
from utils.init import init_session_state

init_session_state()
import pandas as pd
try:
    import graphviz
except ImportError as e:
    st.error("Graphviz not found. Please install it by running `pip install graphviz` and also make sure you have graphviz installed on your system. For debian/ubuntu, run `sudo apt-get install graphviz`")
    st.stop()

from utils.bq_utils import (
    get_all_sql_extracts,
    get_tables_for_qid,
    get_recursive_lineage_for_tables,
)

st.set_page_config(layout="wide")

col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.title("Lineage Explorer")
with col2:
    if st.button("🔄", help="Refresh Data"):
        st.cache_data.clear()
        st.rerun()

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

def generate_lineage_graph(lineage_df: pd.DataFrame) -> graphviz.Digraph:
    """
    Converts the lineage DataFrame into a Graphviz Digraph.
    """
    try:
        dot = graphviz.Digraph(comment='End-to-End Lineage')
        dot.attr(rankdir='LR')  # Left-to-Right layout
        dot.attr('graph', size='25,25!')  # Increase graph size for better readability

        def get_full_name(row, prefix):
            # Use .get() to avoid KeyErrors if a column is missing
            db = row.get(f'{prefix}_database_name')
            table = row.get(f'{prefix}_table_name')
            parts = [db, table]
            return '.'.join([part for part in parts if pd.notna(part) and part])

        # Create a unique set of all nodes
        nodes = set()
        for _, row in lineage_df.iterrows():
            # Define target node
            target_table = get_full_name(row, 'target')
            target_node_id = f'{target_table}.{row.get("target_column")}'
            nodes.add((target_node_id, target_table))  # Store with table name for styling

            # Define source node
            if pd.notna(row.get("source_table_name")):
                source_table = get_full_name(row, 'source')
                source_node_id = f'{source_table}.{row.get("source_column")}'
                nodes.add((source_node_id, source_table))  # Store with table name

        # Add all nodes to the graph with styling
        for node_id, table_name in nodes:
            # Style base/final tables differently from work tables
            if table_name and ('WK_' in table_name or 'TEMP_' in table_name):
                dot.node(node_id, label=node_id, shape='box', style='filled', fillcolor='lightyellow')
            else:
                dot.node(node_id, label=node_id, shape='box', style='filled', fillcolor='lightblue')
        
        # Create nodes for each file
        file_nodes = lineage_df["file_name"].unique()
        for file_name in file_nodes:
            if pd.notna(file_name):
                dot.node(file_name, file_name, shape='ellipse', style='filled', fillcolor='lightgrey')


        # Add all edges
        for _, row in lineage_df.iterrows():
            if pd.notna(row.get("source_table_name")):
                target_table = get_full_name(row, 'target')
                source_table = get_full_name(row, 'source')
                target_node_id = f'{target_table}.{row.get("target_column")}'
                source_node_id = f'{source_table}.{row.get("source_column")}'
                
                edge_label = f'Depth: {row.get("depth", "N/A")}'

                dot.edge(
                    source_node_id, 
                    target_node_id, 
                    label=edge_label
                )

                file_name = row.get("file_name")
                if pd.notna(file_name):
                    dot.edge(
                        file_name,
                        source_node_id,
                        style='dotted',
                        arrowhead='none'
                    )
                
        return dot
    except Exception as e:
        st.error(f"An error occurred while generating the graph: {e}")
        return None

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
    column_config={
        "Select": st.column_config.CheckboxColumn(
            "Select",
            default=False,
            width="small"
        )
    },
    disabled=["File Name", "q_id", "Dependencies", "Inferred Details", "Processing Status"]
)

if st.button("Trace", key="trace_files", use_container_width=True):
    selected_files_df = edited_df[edited_df["Select"]]
    st.session_state.show_tables = False  # Reset on button click

    if not selected_files_df.empty:
        selected_qids = selected_files_df["q_id"].tolist()
        st.session_state.selected_qids = selected_qids
        tables_df = get_tables_for_qid(selected_qids)

        if not tables_df.empty:
            st.session_state.show_tables = True
            target_tables_df = tables_df.copy()
            target_tables_df = target_tables_df.sort_values(by=["file_name"])
            target_tables_df['Select'] = False
            st.session_state.target_tables_df = target_tables_df
        else:
            st.info("No tables found for the selected files.")
    else:
        st.warning("Please select at least one SQL file.")

if st.session_state.get('show_tables', False):
    st.subheader("Select Target Tables")

    # Check if schema column should be displayed
    if 'target_schema_name' in st.session_state.target_tables_df.columns and st.session_state.target_tables_df['target_schema_name'].notna().any():
        column_order = ("Select", "File Name", "Table Name", "Schema", "Database", "Type")
        rename_columns = {
            "file_name": "File Name",
            "target_database_name": "Database",
            "target_schema_name": "Schema",
            "target_table_name": "Table Name",
            "inferred_target_type": "Type"
        }
        disabled_columns = ["File Name", "Table Name", "Schema", "Database", "Type"]
    else:
        column_order = ("Select", "File Name", "Table Name", "Database", "Type")
        rename_columns = {
            "file_name": "File Name",
            "target_database_name": "Database",
            "target_table_name": "Table Name",
            "inferred_target_type": "Type"
        }
        disabled_columns = ["File Name", "Table Name", "Database", "Type"]

    edited_tables_df = st.data_editor(
        st.session_state.target_tables_df.rename(columns=rename_columns),
        use_container_width=True,
        hide_index=True,
        column_order=column_order,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                default=False,
                width="small"
            )
        },
        disabled=disabled_columns,
        key="tables_editor"
    )

    st.session_state.target_tables_df['Select'] = edited_tables_df['Select']

    selected_target_tables_df = st.session_state.target_tables_df[st.session_state.target_tables_df['Select']]

    if not selected_target_tables_df.empty:
        if st.button("Track Column Lineage", key="track_columns", use_container_width=True):
            
            selected_target_tables_list = selected_target_tables_df[[
                "target_database_name",
                "target_schema_name",
                "target_table_name"
            ]].to_dict('records')

            selected_qids = st.session_state.get("selected_qids", [])
            # Get column lineage for selected statements
            lineage_trace_df = get_recursive_lineage_for_tables(selected_target_tables_list, selected_qids)
            if not lineage_trace_df.empty:
                st.header("End-to-End Column Lineage")

                # --- Prepare Data for Tabs ---
                
                # 1. Data for Graph Tab (Full Lineage)
                full_lineage_for_graph = pd.merge(
                    lineage_trace_df.copy(),
                    extracts_df[['q_id', 'file_name']],
                    on='q_id',
                    how='left'
                )

                # 2. Data for Detailed View Tab (Collapsed Lineage)
                lineage_df_copy = lineage_trace_df.copy()

                def is_intermediate(table_name):
                    if not isinstance(table_name, str): return False
                    return 'WK_' in table_name or 'TEMP_' in table_name

                parent_of = {}
                for _, row in lineage_df_copy.iterrows():
                    target_id = (row['target_table_name'], row['target_column'])
                    source_id = (row['source_table_name'], row['source_column'])
                    parent_of[target_id] = (source_id, row.to_dict())

                @st.cache_data
                def find_ultimate_parent(child_id_tuple):
                    if child_id_tuple not in parent_of:
                        return (None, None), None
                    
                    parent_id, parent_row_dict = parent_of[child_id_tuple]
                    
                    if not parent_id[0] or not is_intermediate(parent_id[0]):
                        return parent_id, parent_row_dict
                    else:
                        return find_ultimate_parent(parent_id)

                collapsed_rows = []
                for _, row in lineage_df_copy.iterrows():
                    if not is_intermediate(row['target_table_name']):
                        child_id = (row['target_table_name'], row['target_column'])
                        ultimate_parent_id, ultimate_parent_row_dict = find_ultimate_parent(child_id)
                        
                        new_row = row.to_dict()
                        if ultimate_parent_id and ultimate_parent_id[0] and ultimate_parent_row_dict:
                            new_row['source_table_name'] = ultimate_parent_row_dict['source_table_name']
                            new_row['source_column'] = ultimate_parent_row_dict['source_column']
                            new_row['source_database_name'] = ultimate_parent_row_dict['source_database_name']
                            new_row['source_schema_name'] = ultimate_parent_row_dict['source_schema_name']
                            new_row['depth'] = 'Collapsed'
                        
                        collapsed_rows.append(new_row)

                collapsed_df = pd.DataFrame()
                if collapsed_rows:
                    df = pd.DataFrame(collapsed_rows)
                    subset_cols = [
                        'target_database_name', 'target_schema_name', 'target_table_name', 'target_column',
                        'source_database_name', 'source_schema_name', 'source_table_name', 'source_column'
                    ]
                    existing_subset_cols = [col for col in subset_cols if col in df.columns]
                    collapsed_df = df.drop_duplicates(subset=existing_subset_cols).reset_index(drop=True)

                collapsed_lineage_for_display = pd.merge(
                    collapsed_df,
                    extracts_df[['q_id', 'file_name']],
                    on='q_id',
                    how='left'
                )

                # --- Create Tabs ---
                tab1, tab2 = st.tabs(["📊 Lineage Graph", "📋 Detailed View"])

                with tab1:
                    st.subheader("Visual Lineage Graph")
                    if not full_lineage_for_graph.empty:
                        lineage_graph = generate_lineage_graph(full_lineage_for_graph)
                        if lineage_graph:
                            st.graphviz_chart(lineage_graph, use_container_width=False)
                    else:
                        st.info("No lineage data to graph.")

                with tab2:
                    st.subheader("Collapsed Lineage Table")
                    
                    display_df = collapsed_lineage_for_display
                    
                    if not display_df.empty:
                        # Define columns to display
                        cols_to_display = [
                            "file_name",
                            "target_database_name", "target_schema_name", "target_table_name", "target_column",
                            "source_database_name", "source_schema_name", "source_table_name", "source_column",
                            "transformation_logic"
                        ]
                        
                        # Create a list of columns that exist in the dataframe
                        existing_cols_original_names = [col for col in cols_to_display if col in display_df.columns]
                        
                        # Create a mapping for renaming
                        rename_map = {
                            "file_name": "File Name",
                            "target_database_name": "Target DB",
                            "target_schema_name": "Target Schema",
                            "target_table_name": "Target Table",
                            "target_column": "Target Column",
                            "source_database_name": "Source DB",
                            "source_schema_name": "Source Schema",
                            "source_table_name": "Source Table",
                            "source_column": "Source Column",
                            "transformation_logic": "Logic",
                        }
                        
                        # Rename the columns that exist
                        display_df_renamed = display_df.rename(columns=rename_map)
                        
                        # Get the new names of the existing columns
                        existing_cols_new_names = [rename_map.get(col, col) for col in existing_cols_original_names]

                        # Display the dataframe
                        st.dataframe(display_df_renamed[existing_cols_new_names], use_container_width=True)
                    else:
                        st.info("No lineage details to display.")
                
                # --- DOWNLOAD OPTIONS ---
                st.subheader("Download Lineage Data")
                
                csv_data = collapsed_lineage_for_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download Collapsed Lineage as CSV",
                    data=csv_data,
                    file_name="collapsed_column_lineage.csv",
                    mime="text/csv",
                )

            else:
                st.warning("No lineage trace found.")
