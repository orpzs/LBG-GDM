import streamlit as st
import pandas as pd
try:
    import graphviz
except ImportError as e:
    st.error("Graphviz not found. Please install it by running `pip install graphviz` and also make sure you have graphviz installed on your system. For debian/ubuntu, run `sudo apt-get install graphviz`")
    st.stop()

from frontend.utils.bq_utils import (
    get_all_sql_extracts,
    get_tables_for_qid,
    get_recursive_lineage_for_tables,
)

st.set_page_config(layout="wide")
st.title("Lineage Explorer")

# Custom CSS for the green button
st.markdown("""
<style>
    div.stButton > button {
        background-color: #4CAF50;
        color: white;
        font-size: 20px;
        padding: 10px 24px;
        border-radius: 8px;
    }
</style>""", unsafe_allow_html=True)

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
            schema = row.get(f'{prefix}_schema_name')
            table = row.get(f'{prefix}_table_name')
            parts = [db, schema, table]
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
    disabled=["File Name", "q_id", "Dependencies", "Inferred Details", "Processing Status"]
)

if st.button("Trace", key="trace_files", use_container_width=True):
    selected_files_df = edited_df[edited_df["Select"]]
    st.session_state.show_tables = False  # Reset on button click

    if not selected_files_df.empty:
        selected_qids = selected_files_df["q_id"].tolist()
        tables_df = get_tables_for_qid(selected_qids)

        if not tables_df.empty:
            st.session_state.show_tables = True
            target_tables_df = tables_df.copy()
            target_tables_df['Select'] = False
            st.session_state.target_tables_df = target_tables_df
        else:
            st.info("No tables found for the selected files.")
    else:
        st.warning("Please select at least one SQL file.")

if st.session_state.get('show_tables', False):
    st.subheader("Select Target Tables")

    edited_tables_df = st.data_editor(
        st.session_state.target_tables_df.rename(columns={
            "file_name": "File Name",
            "target_database_name": "Database",
            "target_schema_name": "Schema",
            "target_table_name": "Table Name",
            "inferred_target_type": "Type"
        }),
        use_container_width=True,
        hide_index=True,
        column_order=("Select", "File Name", "Table Name", "Schema", "Database", "Type"),
        disabled=["File Name", "Table Name", "Schema", "Database", "Type"],
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

            # Get column lineage for selected statements

            print(selected_target_tables_list)
            lineage_trace_df = get_recursive_lineage_for_tables(selected_target_tables_list)
            if not lineage_trace_df.empty:
                st.header("End-to-End Column Lineage")

                # Add file_name to the lineage trace
                lineage_trace_df = pd.merge(
                    lineage_trace_df,
                    extracts_df[['q_id', 'file_name']],
                    on='q_id',
                    how='left'
                )

                # Create the tabs
                tab1, tab2 = st.tabs(["📊 Lineage Graph", "📋 Detailed View"])

                with tab1:
                    st.subheader("Visual Lineage Graph")
                    if not lineage_trace_df.empty:
                        lineage_graph = generate_lineage_graph(lineage_trace_df)
                        if lineage_graph:
                            st.graphviz_chart(lineage_graph, use_container_width=False)
                    else:
                        st.info("No lineage data to graph.")

                with tab2:
                    st.subheader("Detailed Lineage Table")
                    # This is your existing table view
                    display_df = lineage_trace_df.rename(columns={
                        "file_name": "File Name",
                        "depth": "Depth",
                        "target_database_name": "Downstream Database",
                        "target_schema_name": "Downstream Schema",
                        "target_table_name": "Downstream Table",
                        "target_column": "Downstream Column",
                        "transformation_logic": "Logic",
                        "source_type": "Source Type",
                        "source_database_name": "Upstream Database",
                        "source_schema_name": "Upstream Schema",
                        "source_table_name": "Upstream Table",
                        "source_column": "Upstream Column",
                    })
                    
                    # Reorder columns to have File Name first
                    cols_to_display = [
                        "File Name", "Depth", 
                        "Downstream Database", "Downstream Schema", "Downstream Table", "Downstream Column",
                        "Logic", "Source Type", 
                        "Upstream Database", "Upstream Schema", "Upstream Table", "Upstream Column"
                    ]
                    existing_cols = [col for col in cols_to_display if col in display_df.columns]
                    
                    st.dataframe(display_df[existing_cols], use_container_width=True)
                
                # --- DOWNLOAD OPTIONS ---
                st.subheader("Download Lineage Data")
                
                # 1. Download as CSV (for Analysts)
                csv_data = lineage_trace_df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv_data,
                    file_name="column_lineage.csv",
                    mime="text/csv",
                )

            else:
                st.warning("No lineage trace found.")