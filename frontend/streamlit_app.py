import streamlit as st
st.set_page_config(layout="wide")
from functools import partial
import json
from typing import Any, Dict, List
import uuid
import logging
from logging import getLogger
from style.app_markdown import MARKDOWN_STR_1,  MARKDOWN_STR_2
# from utils.local_chat_history import LocalChatMessageHistory
import google.oauth2.id_token
import os
from dotenv import load_dotenv
from google.adk.sessions import Session
import requests
from utils.schema import ImageData, ChatRequest, ChatResponse, inlineData, fileUriData
load_dotenv()

# @st.cache_resource
# def get_token():
#     cloudrun_token = google.oauth2.id_token.fetch_id_token(google.auth.transport.requests.Request(), os.environ.get("API_BASE_URL", "http://localhost:8000"))
#     return cloudrun_token



# --- Streamlit Session State Initialization ---
if "fastapi_url" not in st.session_state:
    st.session_state.fastapi_url = os.environ.get("API_BASE_URL", "http://localhost:8000") # Default value
if "user_id" not in st.session_state:
    try:
        user_email = st.context.headers.get("X-Goog-Authenticated-User-Email") #X-Goog-Authenticated-User-Email': 'accounts.google.com:moksh.atukuri@vodafone.com'
        # user_email = 'accounts.google.com:moksh.atukuri@vodafone.com'
        if user_email:
            st.session_state.user_id = user_email
        else:
            raise KeyError("X-Goog-Authenticated-User-Email header not found")
    except KeyError:
        st.session_state.user_id = f"my_user_{st.context.headers.get('Host', 'default_host')}"
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
# if "google_auth_token" not in st.session_state:
#     st.session_state.google_auth_token = get_token()




# print(st.context.headers.to_dict())
# print(st.context.cookies.to_dict())

app_logger = getLogger()
app_logger.addHandler(logging.StreamHandler())
# app_logger.setLevel(logging.DEBUG)


st.markdown(MARKDOWN_STR_2, unsafe_allow_html=True)

st.logo(
    "frontend/images/moksh_gemini_logo.png",
    size="large",
    link="https://www.google.com",
    icon_image=None,
)




def setup_pages() -> None:
    # st.header("Network HLD Assitant")
    pg = st.navigation([st.Page("pages/data_model_review.py", title="Data Model Review", icon=":material/pageview:"),
                        st.Page("pages/load_entities.py", title="Load Data Model Entities", icon=":material/upload_file:"),
                        st.Page("pages/define_guidelines.py", title="Define Guidelines", icon=":material/rule:"), 
                        st.Page("pages/sql_analysis_page.py", title="SQL Analysis", icon=":material/analytics:"),
                        # st.Page("pages/tdd_generate.py", title="Generation Assistant", icon=":material/description:"),
                        # st.Page("pages/generate_config_files.py", title="Config Generator", icon=":material/settings_applications:"),

                        ])
    pg.run()




def main() -> None:
    
    setup_pages()
    # chat_page()


if __name__ == "__main__":
    main()
