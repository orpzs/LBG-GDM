import streamlit as st
from uuid import uuid4
from functools import partial
import json
from typing import Any, Dict, List
import uuid
import logging
from style.app_markdown import add_app_name_header
from utils.content_utils import get_full_content_text, tools_calls_in_content
from utils.message_editing import MessageEditing
from utils.stream_handler import Client, StreamHandler, get_chain_response
from utils.vertexai_utils import agents
from vertexai.preview.generative_models import Content, Part
from utils.chat_utils import save_chat

import requests
from pydantic import BaseModel
import typing, os
from typing import List, Literal, Optional, Dict, Any

from google.adk.sessions import Session
from google.adk.events import Event
from google.genai import types
from utils.api_utils import _create_session_api, _get_api_url, _get_session_history, _list_sessions_api

from utils.chat_utils import save_chat


EMPTY_CHAT_NAME = ""
NUM_CHAT_IN_RECENT = 3


def new_chat_creation():
        if not st.session_state.app_name:
            st.error("Please select an application first.")
            return
        session_id = str(uuid.uuid4())
        new_session = _create_session_api(st.session_state.app_name, st.session_state.user_id, session_id =session_id )
        if new_session:
            st.session_state.session_id = session_id
            st.session_state.uploader_key = 0
            st.session_state.messages = [] 
            st.session_state.raw_events = []
            if st.session_state.session_id not in st.session_state.user_chats:
                st.session_state.user_chats[st.session_state.session_id] = {"messages":[]}
            else:
                st.session_state.user_chats[st.session_state.session_id]['messages'] = []
            st.session_state.modified_prompt = None
            # st.rerun() # REFRESH
        else:
            st.error("Failed to create new session.")


class SideBar:
    """Manages the sidebar components of the Streamlit application."""

    def __init__(self, st: Any) -> None:
        self.st = st

    def multiselect_checkboxes(self, label: str, options: list[str]) -> list[str]:


        selected = []
        self.st.write(f"**{label}**")  # Make the label bold

        cols = self.st.columns(min(1, len(options)))  # Divide options into columns

        for i, option in enumerate(options):
            with cols[i % len(cols)]:
                if self.st.checkbox(f"**{option}**", key=f"{label}_{option}",label_visibility="visible"):  # Unique key for each checkbox
                    selected.append(option)

        return selected



    def init_side_bar(self) -> None:
        """Initialize and render the sidebar components."""
        with self.st.sidebar:
            st.button("New Chat", on_click=new_chat_creation, key="new_chat_button", icon=":material/add_circle:",use_container_width=True)
            self.st.divider()
            self.st.header("Chat History") 

            NUM_CHAT_IN_RECENT = 5
            app_name = st.session_state.app_name
            user_id = st.session_state.user_id # Make sure it exists.
            if not app_name or not user_id:
                st.warning("Select an app and configure user ID to load sessions.")  # Added app selection message
                return

            sessions: List[Session] = _list_sessions_api(app_name, user_id)  # Fetch sessions from API
            if sessions is None:
                print("Unable to Fetch Sessions Api")
                return

            if not sessions:
                st.info("No sessions found. Start a new chat!")
                return
            
            # with self.st.expander("Click to expand"):
            sorted_sessions = sorted(sessions, key=lambda x: x['lastUpdateTime'], reverse=True)
            
            # print(sorted_sessions)
            for chat in sorted_sessions[:NUM_CHAT_IN_RECENT]:
                # print("Getting top 5")
                
                if self.st.button(chat.get('id')):
                    st.session_state.messages = []
                    self.st.session_state.run_id = None
                    self.st.session_state["session_id"] = chat.get('id')
                    event = _get_session_history(app_name, user_id,chat.get('id'))
                    if event and isinstance(event, list):
                        for item in event:
                            if "content" in item and item["content"]:
                                content = item["content"]
                                role = item.get("role", "user")
                                text = None
                                if isinstance(content, dict) and "parts" in content:
                                    if content["role"] == "model":
                                        role = "assistant"
                                    else:
                                        role = "user"
                                    parts = content["parts"]
                                    text = ""
                                    for part in parts:
                                        if isinstance(part, dict)and "text" in part:
                                            text += part["text"]
                                    
                                    # st.session_state.messages.append({"role": role, "content": text})
                                elif isinstance(content, list):
                                    for element in content:
                                        if isinstance(element, dict)and "parts" in element:
                                            parts = element["parts"]
                                            text = ""
                                            for part in parts:
                                                if isinstance(part, dict) and 'text' in part:
                                                    text += part["text"]
                                    # st.session_state.messages.append({"role": role, "content": text})
                                elif isinstance(content, str):  # Handle case where content is just a string
                                    text= content
                                    # st.session_state.messages.append({"role": role, "content": content})
                                else:
                                    print(f"Unexpected content type: {type(content)}. Content was: {content}")  # debugging
                                
                                if text:
                                    st.session_state.messages.append({"role": role, "content": text})
                            else:
                                print("Event missing 'content' or content is empty.")
                    else:
                        print("Event is None or not a list.")


                            
