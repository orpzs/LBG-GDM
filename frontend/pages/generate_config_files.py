import time
import streamlit as st
import docx
from io import BytesIO
import json
import logging
import os
import re
from utils.multimodal_utils import encode_data, upload_files_to_gcs
from utils.api_utils import generate_config
import pandas as pd


def main():
    st.subheader("Configuration File Generator", divider="gray")

    if "inferred_schema" not in st.session_state:
        st.session_state.inferred_schema = None
    if "edited_schema" not in st.session_state:
        st.session_state.edited_schema = None
    if "gcs_bucket" not in st.session_state:
        st.session_state.gcs_bucket = "hld-proc-bucket"
    if "gcs_uri_data" not in st.session_state:
        st.session_state.gcs_uri_data = None
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0
    if "generated_config_df" not in st.session_state:
        st.session_state.generated_config_df = None

    st.markdown("""
    This tool helps you generate necessary configuration files by analyzing your data's schema and content.

    **How to use:**
    1.  **Upload Schema or Data:** Provide a CSV file that either defines the schema directly or contains sample data from which to infer the schema.
    2.  **Infer Schema:** Click the "Infer Schema" button.
    3.  **Review & Edit:** The inferred schema will be displayed in an editable table. You can make any necessary changes.
    4.  **Generate Config:** Once you are satisfied with the schema, click "Generate Configuration File".
    """)

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.info("Option 1: Upload Table Schema")
        schema_file = st.file_uploader("Upload your table schema file (e.g., columns: 'column_name', 'datatype', 'description')",
                                       type=['csv'],
                                       key="schema_file",
                                       disabled=st.session_state.get("sample_data_file") is not None)


    with col2:
        st.info("Option 2: Upload Sample Data")
        sample_data_file = st.file_uploader("Upload a sample data file to infer schema from its columns",
                                            type=['csv'],
                                            key="sample_data_file",
                                            disabled=st.session_state.get("schema_file") is not None)


    st.divider()

    if st.button("Infer Schema", type="secondary", use_container_width=True):
        st.session_state.inferred_schema = None  # Reset on each click
        st.session_state.gcs_uri_data = None  # Reset on each click
        st.session_state.generated_config_df = None # Reset on each click
        if schema_file:
            with st.spinner("Reading schema from file..."):
                try:
                    df = pd.read_csv(schema_file)
                    required_cols = ['column_name', 'datatype', 'description']
                    if all(col in df.columns for col in required_cols):
                        st.session_state.inferred_schema = df[required_cols]
                        st.success("Schema loaded successfully!")
                    else:
                        st.error(f"Schema file must contain the columns: {', '.join(required_cols)}")
                except Exception as e:
                    st.error(f"Error reading schema file: {e}")
        elif sample_data_file:
            with st.spinner("Inferring schema from sample data..."):
                try:
                    file_content = sample_data_file.getvalue()
                    gcs_uri_data = upload_files_to_gcs(
                        st,
                        st.session_state.gcs_bucket,
                        file_content,
                        sample_data_file.type,
                        sample_data_file.name
                    )
                    st.session_state.gcs_uri_data = gcs_uri_data

                    prompt = "Infer the schema from the provided data file. The schema should include column_name, datatype, and a description for each column."

                    response_str = generate_config(
                        prompt=prompt,
                        gcs_uri=[gcs_uri_data],
                        base64_data=None
                    )

                    if response_str:
                        response_json = json.loads(response_str)
                        schema_list = response_json.get("schema", [])
                        if isinstance(schema_list, dict):
                            schema_list = list(schema_list.values())
                        inferred_df = pd.DataFrame(schema_list)

                        required_cols = ['column_name', 'datatype', 'description']
                        if all(col in inferred_df.columns for col in required_cols):
                            st.session_state.inferred_schema = inferred_df[required_cols]
                            st.success("Schema inferred successfully from sample data!")
                        else:
                            st.error(f"Schema inference did not return the required columns. Got: {inferred_df.columns.tolist()}")
                            st.json(response_json)
                    else:
                        st.error("Failed to get a response from the schema inference service.")
                except Exception as e:
                    st.error(f"Error inferring schema: {e}")
        else:
            st.warning("Please upload either a schema file or a sample data file.")

    # --- Step 2: Display, Edit, and Generate Final Config ---
    if st.session_state.inferred_schema is not None:
        st.subheader("Review and Edit Inferred Schema")
        st.info("You can edit the values directly in the table below. Add or remove rows as needed.")

        edited_df = st.data_editor(
            st.session_state.inferred_schema,
            num_rows="dynamic",
            use_container_width=True,
            key="schema_editor"
        )
        st.session_state.edited_schema = edited_df

        st.divider()

        if st.button("Generate Configuration File", type="primary", use_container_width=True):
            st.session_state.generated_config_df = None # Reset on click
            # We must have an uploaded file to proceed, prefering the data file.
            source_file = sample_data_file or schema_file
            if source_file and st.session_state.edited_schema is not None:
                with st.spinner("Generating final configuration based on your edited schema..."):
                    try:
                        # Convert the edited schema DataFrame to a JSON string
                        schema_json_string = st.session_state.edited_schema.to_json(orient='records', indent=4)

                        gcs_uri_list = []
                        prompt = f"""Generate a complete configuration file in JSON format using the provided schema.

Schema:
{schema_json_string}
"""
                        if sample_data_file and st.session_state.gcs_uri_data:
                            gcs_uri_list = [st.session_state.gcs_uri_data]
                            prompt = f"""Generate a complete configuration file in JSON format using the provided schema and the data from the referenced file.

Schema:
{schema_json_string}
"""

                        # The final API call using the edited schema
                        response_str = generate_config(
                            prompt=prompt,
                            gcs_uri=gcs_uri_list,
                            base64_data=None
                        )

                        # Parse and display the final JSON response
                        if response_str:
                            response_json = json.loads(response_str)

                            config_data = response_json.get("generated_config", {}).get("config", [])
                            if not config_data and isinstance(response_json.get("config"), list):
                                config_data = response_json.get("config", [])

                            if config_data:
                                df_config = pd.DataFrame(config_data)
                                st.session_state.generated_config_df = df_config
                                st.success("Final configuration generated successfully!")
                                # st.balloons()
                            else:
                                st.error("Could not parse the generated configuration. Displaying raw response:")
                                st.json(response_json)
                        else:
                            st.error("Received an empty response from the configuration generation service.")

                    except Exception as e:
                        st.error(f"Failed to generate final configuration: {e}")
            else:
                st.warning("Could not find source file or edited schema. Please restart the process.")

    if st.session_state.generated_config_df is not None:
        st.subheader("Generated Configuration")
        st.dataframe(st.session_state.generated_config_df)

        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode('utf-8')

        csv = convert_df_to_csv(st.session_state.generated_config_df)
        st.download_button(
            label="Download Configuration as CSV",
            data=csv,
            file_name="generated_config.csv",
            mime="text/csv",
            use_container_width=True
        )

if __name__ == "__main__":
    main()