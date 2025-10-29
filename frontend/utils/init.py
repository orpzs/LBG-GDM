import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

def init_session_state():
    if "fastapi_url" not in st.session_state:
        st.session_state.fastapi_url = os.environ.get("API_BASE_URL", "http://localhost:8000") # Default value
    if "user_id" not in st.session_state:
        try:
            user_email = st.context.headers.get("X-Goog-Authenticated-User-Email")
            if user_email:
                st.session_state.user_id = user_email
            else:
                raise KeyError("X-Goog-Authenticated-User-Email header not found")
        except (KeyError, AttributeError):
            st.session_state.user_id = f"my_user_{st.context.headers.get('Host', 'default_host') if hasattr(st.context, 'headers') else 'default_host'}"
    if "app_name" not in st.session_state:
        st.session_state.app_name = "tdd_generate_app" 
    if "project_id" not in st.session_state:
        st.session_state.project_id = "r2d2-00" 
    if "session_id" not in st.session_state:
        st.session_state.session_id = None  
    if "messages" not in st.session_state:
        st.session_state.messages = []  
    if "raw_events" not in st.session_state:
        st.session_state.raw_events = []  
    if "available_apps" not in st.session_state:
        st.session_state.available_apps = []
    if "available_sessions" not in st.session_state:
        st.session_state.available_sessions = []
    if "selected_session_to_load" not in st.session_state:
        st.session_state.selected_session_to_load = None
    if "modified_prompt" not in st.session_state: 
        st.session_state.modified_prompt = None
    if "user_chats" not in st.session_state:
        st.session_state.user_chats = {}
    if "gcs_bucket" not in st.session_state:
        st.session_state.gcs_bucket = "gs://hld-proc-bucket"
    if "project_id" not in st.session_state:
        st.session_state.project_id = "r2d2-00"
    if "cc_bq_dataset" not in st.session_state:
        st.session_state.cc_bq_dataset = "outputs"
    if "cc_bq_table" not in st.session_state:
        st.session_state.cc_bq_table = "hld_compliance_checks"
    if "uploader_key" not in st.session_state:
        st.session_state.uploader_key = 0
    if "parser_output" not in st.session_state:
        st.session_state.parser_output = None
    if "analysis_running" not in st.session_state:
        st.session_state.analysis_running = False
    if "q_id" not in st.session_state:
        st.session_state.q_id = None
    if "error" not in st.session_state:
        st.session_state.error = None
    if "processing_status" not in st.session_state:
        st.session_state.processing_status = None
    if "uploaded_file_name" not in st.session_state:
        st.session_state.uploaded_file_name = None
    if "uploaded_file" not in st.session_state:
        st.session_state.uploaded_file = None
    if "file_uploader_key" not in st.session_state:
        st.session_state.file_uploader_key = 0
    if "get_sources_for_sids" not in st.session_state:
        st.session_state.get_sources_for_sids = None
    
