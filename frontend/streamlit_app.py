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



from utils.init import init_session_state

init_session_state()




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
    # st.header("Assitant")
    pg = st.navigation([#st.Page("pages/data_model_review.py", title="Data Model Review", icon=":material/pageview:"),
                        #st.Page("pages/load_entities.py", title="Load Data Model Entities", icon=":material/upload_file:"),
                        #st.Page("pages/define_guidelines.py", title="Define Guidelines", icon=":material/rule:"), 
                        st.Page("pages/sql_analysis_page.py", title="SQL Analysis", icon=":material/analytics:"),
                        st.Page("pages/lineage_explorer.py", title="Lineage Explorer", icon=":material/account_tree:"),
                        st.Page("pages/sql_stats.py", title="SQL Stats", icon=":material/query_stats:"),
                        # st.Page("pages/tdd_generate.py", title="Generation Assistant", icon=":material/description:"),
                        # st.Page("pages/generate_config_files.py", title="Config Generator", icon=":material/settings_applications:"),

                        ])
    pg.run()




def main() -> None:
    
    setup_pages()
    # chat_page()


if __name__ == "__main__":
    main()
