import streamlit as st
from utils.init import init_session_state

init_session_state()
import xml.etree.ElementTree as ET
import logging
import re
from utils.bq_utils import insert_entity_to_bq
from utils.vertexai_utils import generate_embedding


def main():
    st.set_page_config(page_title="Load Data Model Entities", layout="wide")
    st.title("Load Data Model Entities")

    st.markdown(
        """
        Paste an XML block from an ER Studio data model export below.
        The tool will extract the `complexType` name, generate an embedding for the XML content,
        and load it into a BigQuery table for further analysis and similarity search.
        The target table is `gdm.entities` and will be created if it doesn't exist.
        """
    )

    xml_input = st.text_area("Paste XML Block Here", height=400, key="xml_input_entity")

    if st.button("Load Entity to BigQuery", type="primary", use_container_width=True):
        if not xml_input:
            st.warning("Please paste an XML block.")
            return

        with st.spinner("Processing entity..."):
            try:
                # Pre-process XML to remove namespace prefixes to avoid "unbound prefix" errors.
                # This is a common issue when pasting snippets from a larger XML document.
                processed_xml = re.sub(r'(</?)\w+:', r'\1', xml_input)

                # 1. Extract complexType name
                root = ET.fromstring(processed_xml)
                entity_name = None

                # The complexType element could be the root of the snippet or a descendant.
                if root.tag == 'complexType':
                    entity_name = root.get('name')
                else:
                    complex_type_node = root.find('.//complexType')
                    if complex_type_node is not None:
                        entity_name = complex_type_node.get('name')

                if not entity_name:
                    st.error("Could not find a `<complexType>` element with a 'name' attribute in the provided XML.")
                    return
                
                st.info(f"Extracted entity name: **{entity_name}**")

                # 2. Generate embedding using the original, unprocessed XML to retain all information.
                st.info("Generating embedding for the XML block...")
                embedding = generate_embedding(xml_input)
                if not embedding or not isinstance(embedding, list):
                    st.error("Failed to generate a valid embedding.")
                    return
                
                st.info(f"Embedding generated with {len(embedding)} dimensions.")

                # 3. Insert into BigQuery
                project_id = st.session_state.get("project_id", "r2d2-00")
                dataset_id = "gdm"
                table_name = "entities"

                success = insert_entity_to_bq(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_name=table_name,
                    entity_name=entity_name,
                    xml_block=xml_input, # Store the original XML in BigQuery
                    embedding=embedding,
                )
                if success:
                    pass

            except ET.ParseError as e:
                st.error(f"Invalid XML format. Please check the pasted content. Error: {e}")
            except Exception as e:
                st.error(f"An unexpected error occurred: {e}")
                logging.error(f"Error in load_entities: {e}", exc_info=True)


if __name__ == "__main__":
    main()