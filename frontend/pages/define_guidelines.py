import streamlit as st
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from frontend.utils.bq_utils import (
    create_guidelines_table_if_not_exists,
    get_all_guidelines,
    add_guideline,
    update_guideline,
    delete_guideline
)

st.set_page_config(page_title="Define Guidelines", page_icon="✅", layout="wide")

# Initialize session state for guidelines
if 'guidelines' not in st.session_state:
    st.session_state.guidelines = get_all_guidelines()

if 'editing_guideline' not in st.session_state:
    st.session_state.editing_guideline = None

# Create the table if it doesn't exist
create_guidelines_table_if_not_exists()

st.title("Manage Data Model Review Guidelines")

st.markdown("""
Here you can define the rules and guidelines that the Data Model Review Agent will use to assess the completeness and correctness of your data models.
""")

# --- Display and Edit Guidelines ---
st.subheader("Current Guidelines")

if st.session_state.guidelines.empty:
    st.info("No guidelines defined yet. Add one below to get started.")
else:
    for index, row in st.session_state.guidelines.iterrows():
        with st.container(border=True):
            guideline_id = row["guideline_id"]

            if st.session_state.editing_guideline == guideline_id:
                # Edit mode
                with st.form(f"edit_form_{guideline_id}"):
                    new_text = st.text_area("Edit guideline:", value=row["guideline_text"], key=f"text_{guideline_id}")
                    new_status = st.checkbox("Active", value=row["is_active"], key=f"status_{guideline_id}")
                    save_button = st.form_submit_button("Save")
                    
                    if save_button:
                        update_guideline(guideline_id, new_text, new_status)
                        st.session_state.editing_guideline = None
                        st.session_state.guidelines = get_all_guidelines()
                        st.rerun()
            else:
                # View mode
                col1, col2, col3, col4 = st.columns([0.7, 0.1, 0.1, 0.1])
                with col1:
                    st.markdown(f"{row['guideline_text']}")
                with col2:
                    is_active = st.toggle("Active", value=row["is_active"], key=f"toggle_{guideline_id}_{index}")
                    if is_active != row["is_active"]:
                        update_guideline(guideline_id, row["guideline_text"], is_active)
                        st.session_state.guidelines = get_all_guidelines()
                        st.rerun()
                with col3:
                    if st.button("✏️", key=f"edit_{guideline_id}_{index}"):
                        st.session_state.editing_guideline = guideline_id
                        st.rerun()
                with col4:
                    if st.button("🗑️", key=f"delete_{guideline_id}_{index}"):
                        delete_guideline(guideline_id)
                        st.session_state.guidelines = get_all_guidelines()
                        st.rerun()

st.divider()

# --- Add New Guideline Form ---
st.subheader("Add a New Guideline")
with st.form("new_guideline_form", clear_on_submit=True):
    new_guideline_text = st.text_area("Guideline Description:")
    submitted = st.form_submit_button("Add Guideline")
    if submitted and new_guideline_text:
        add_guideline(new_guideline_text)
        st.session_state.guidelines = get_all_guidelines() # Refresh guidelines
        st.rerun()
